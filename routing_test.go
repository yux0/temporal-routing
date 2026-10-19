package routing

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Test activities and workflows. FetchAccount is registered under a custom
// name, Payments with a struct prefix, SendEmail under its Go name.

func FetchAccount(ctx context.Context) (string, error) {
	r, _ := FromContext(ctx)
	return r.Owner, nil
}

func SendEmail(ctx context.Context) error { return nil }

type Payments struct{}

func (p *Payments) Charge(ctx context.Context) error { return nil }
func (p *Payments) Refund(ctx context.Context) error { return nil }

type parentInput struct {
	ExplicitChargeQueue string
	CallChargeByGoName  bool
	SkipChildren        bool
}

func ParentWorkflow(ctx workflow.Context, in parentInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err := workflow.ExecuteActivity(ctx, FetchAccount).Get(ctx, nil); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(ctx, SendEmail).Get(ctx, nil); err != nil {
		return err
	}
	chargeCtx := ctx
	if in.ExplicitChargeQueue != "" {
		chargeCtx = workflow.WithTaskQueue(ctx, in.ExplicitChargeQueue)
	}
	var charge any = "pay.Charge"
	if in.CallChargeByGoName {
		var p *Payments
		charge = p.Charge
	}
	if err := workflow.ExecuteActivity(chargeCtx, charge).Get(ctx, nil); err != nil {
		return err
	}
	if in.SkipChildren {
		return nil
	}
	if err := workflow.ExecuteChildWorkflow(ctx, ChildWorkflow).Get(ctx, nil); err != nil {
		return err
	}
	return workflow.ExecuteChildWorkflow(ctx, OtherChild).Get(ctx, nil)
}

func ChildWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err := workflow.ExecuteActivity(ctx, FetchAccount).Get(ctx, nil); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, SendEmail).Get(ctx, nil)
}

func OtherChild(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	return workflow.ExecuteActivity(ctx, SendEmail).Get(ctx, nil)
}

func testCatalog() *Catalog {
	c := NewCatalog()
	c.AddActivity(FetchAccount, activity.RegisterOptions{Name: "acct.fetch"}, "accounts")
	c.AddActivity(SendEmail, activity.RegisterOptions{}, "notify")
	c.AddActivity(&Payments{}, activity.RegisterOptions{Name: "pay."}, "payments")
	c.AddWorkflow(ParentWorkflow, workflow.RegisterOptions{}, "orders")
	c.AddWorkflow(ChildWorkflow, workflow.RegisterOptions{}, "children")
	c.AddWorkflow(OtherChild, workflow.RegisterOptions{}, "children")
	return c
}

var devPolicy = Policy{Enabled: true, OverrideScheduleToStartTimeout: 30 * time.Second}

// call is what the SDK sees after the routing interceptor ran.
type call struct {
	Workflow  string // workflow type making the call
	Kind      string // "start", "activity" or "child"
	Type      string // activity or child workflow type
	TaskQueue string
	Route     *Route
	S2S       time.Duration
}

type recorder struct {
	interceptor.WorkerInterceptorBase
	mu    sync.Mutex
	calls []call
}

func (r *recorder) add(c call) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, c)
}

func (r *recorder) find(wf, kind, typ string) (call, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.calls {
		if c.Workflow == wf && c.Kind == kind && c.Type == typ {
			return c, true
		}
	}
	return call{}, false
}

func routePtr(ctx workflow.Context) *Route {
	if r, ok := FromWorkflowContext(ctx); ok {
		return &r
	}
	return nil
}

func (r *recorder) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	in := &recorderInbound{r: r}
	in.Next = next
	return in
}

type recorderInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	r *recorder
}

func (in *recorderInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	o := &recorderOutbound{r: in.r}
	o.Next = outbound
	return in.Next.Init(o)
}

func (in *recorderInbound) ExecuteWorkflow(ctx workflow.Context, input *interceptor.ExecuteWorkflowInput) (any, error) {
	info := workflow.GetInfo(ctx)
	in.r.add(call{Workflow: info.WorkflowType.Name, Kind: "start", Type: info.WorkflowType.Name, TaskQueue: info.TaskQueueName, Route: routePtr(ctx)})
	return in.Next.ExecuteWorkflow(ctx, input)
}

type recorderOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	r *recorder
}

func (o *recorderOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	opts := workflow.GetActivityOptions(ctx)
	o.r.add(call{Workflow: workflow.GetInfo(ctx).WorkflowType.Name, Kind: "activity", Type: activityType, TaskQueue: opts.TaskQueue, S2S: opts.ScheduleToStartTimeout})
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (o *recorderOutbound) ExecuteChildWorkflow(ctx workflow.Context, childWorkflowType string, args ...any) workflow.ChildWorkflowFuture {
	o.r.add(call{Workflow: workflow.GetInfo(ctx).WorkflowType.Name, Kind: "child", Type: childWorkflowType, TaskQueue: workflow.GetChildWorkflowOptions(ctx).TaskQueue, Route: routePtr(ctx)})
	return o.Next.ExecuteChildWorkflow(ctx, childWorkflowType, args...)
}

type logEntry struct {
	level, msg string
	keyvals    []any
}

type captureLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

func (l *captureLogger) add(level, msg string, kv []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, logEntry{level, msg, kv})
}
func (l *captureLogger) Debug(msg string, kv ...any) { l.add("debug", msg, kv) }
func (l *captureLogger) Info(msg string, kv ...any)  { l.add("info", msg, kv) }
func (l *captureLogger) Warn(msg string, kv ...any)  { l.add("warn", msg, kv) }
func (l *captureLogger) Error(msg string, kv ...any) { l.add("error", msg, kv) }

func (l *captureLogger) has(level, substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.entries {
		if e.level == level && strings.Contains(e.msg, substr) {
			return true
		}
	}
	return false
}

type harness struct {
	env    *testsuite.TestWorkflowEnvironment
	rec    *recorder
	logger *captureLogger
}

// newHarness runs workflows on taskQueue with route in the start header, as a
// worker with workerPolicy would.
func newHarness(t *testing.T, workerPolicy Policy, taskQueue string, route *Route) *harness {
	var s testsuite.WorkflowTestSuite
	logger := &captureLogger{}
	s.SetLogger(logger)
	env := s.NewTestWorkflowEnvironment()
	rec := &recorder{}
	// Each inbound's Init wraps the outbound it receives, so the first inbound
	// interceptor ends up with the innermost outbound: the recorder sees calls
	// after routing changed them.
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{
		rec,
		NewInterceptor(Options{Policy: workerPolicy, Catalog: testCatalog()}),
	}})
	env.SetContextPropagators([]workflow.ContextPropagator{NewContextPropagator(workerPolicy)})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: taskQueue})
	if route != nil {
		p, err := converter.GetDefaultDataConverter().ToPayload(*route)
		require.NoError(t, err)
		env.SetHeader(&commonpb.Header{Fields: map[string]*commonpb.Payload{HeaderKey: p}})
	}
	env.RegisterWorkflow(ParentWorkflow)
	env.RegisterWorkflow(ChildWorkflow)
	env.RegisterWorkflow(OtherChild)
	env.RegisterActivityWithOptions(FetchAccount, activity.RegisterOptions{Name: "acct.fetch"})
	env.RegisterActivity(SendEmail)
	env.RegisterActivityWithOptions(&Payments{}, activity.RegisterOptions{Name: "pay."})
	return &harness{env: env, rec: rec, logger: logger}
}

func (h *harness) run(t *testing.T, in parentInput) {
	h.env.ExecuteWorkflow(ParentWorkflow, in)
	require.True(t, h.env.IsWorkflowCompleted())
	require.NoError(t, h.env.GetWorkflowError())
}

func (h *harness) queue(t *testing.T, wf, kind, typ string) string {
	c, ok := h.rec.find(wf, kind, typ)
	require.True(t, ok, "no %s %s call from %s", kind, typ, wf)
	return c.TaskQueue
}

func TestNoRouteLeavesProductionRunUnchanged(t *testing.T) {
	h := newHarness(t, devPolicy, "orders", nil)
	h.run(t, parentInput{})
	for _, typ := range []string{"acct.fetch", "SendEmail", "pay.Charge"} {
		require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "activity", typ), typ)
	}
	require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "child", "ChildWorkflow"))
}

func TestActivityOverrideAndHomeQueues(t *testing.T) {
	h := newHarness(t, devPolicy, "orders", &Route{Owner: "yu", HomeTaskQueue: "orders", Activities: map[string]string{"SendEmail": "dev-yu"}})
	h.run(t, parentInput{})
	require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "activity", "acct.fetch"))
	require.Equal(t, "dev-yu", h.queue(t, "ParentWorkflow", "activity", "SendEmail"))
	require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "activity", "pay.Charge"))

	c, _ := h.rec.find("ParentWorkflow", "activity", "SendEmail")
	require.Equal(t, 30*time.Second, c.S2S, "override activities get the policy's schedule-to-start timeout")
	c, _ = h.rec.find("ParentWorkflow", "activity", "acct.fetch")
	require.Zero(t, c.S2S)
	require.True(t, h.logger.has("info", "activity sent to override task queue"))
}

// The case the routing layer exists for: workflow code runs locally, every
// activity goes where production would have sent it.
func TestWorkflowOnDevQueueSendsActivitiesToHomeQueue(t *testing.T) {
	h := newHarness(t, devPolicy, "dev-yu", &Route{Owner: "yu", WorkflowTaskQueue: "dev-yu", HomeTaskQueue: "orders"})
	h.run(t, parentInput{SkipChildren: true})
	for _, typ := range []string{"acct.fetch", "SendEmail", "pay.Charge"} {
		require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "activity", typ), typ)
	}
}

func TestCatalogHomeWhenRouteHasNoHome(t *testing.T) {
	h := newHarness(t, devPolicy, "dev-yu", &Route{WorkflowTaskQueue: "dev-yu"})
	h.run(t, parentInput{SkipChildren: true})
	require.Equal(t, "accounts", h.queue(t, "ParentWorkflow", "activity", "acct.fetch"))
	require.Equal(t, "notify", h.queue(t, "ParentWorkflow", "activity", "SendEmail"))
	require.Equal(t, "payments", h.queue(t, "ParentWorkflow", "activity", "pay.Charge"))
}

func TestCatalogHomeOnDevQueueWithoutRoute(t *testing.T) {
	h := newHarness(t, devPolicy, "dev-yu", nil)
	h.run(t, parentInput{SkipChildren: true})
	require.Equal(t, "notify", h.queue(t, "ParentWorkflow", "activity", "SendEmail"))
}

func TestUntrustedHomeQueueIsIgnored(t *testing.T) {
	h := newHarness(t, devPolicy, "dev-yu", &Route{WorkflowTaskQueue: "dev-yu", HomeTaskQueue: "exfil"})
	h.run(t, parentInput{SkipChildren: true})
	require.Equal(t, "notify", h.queue(t, "ParentWorkflow", "activity", "SendEmail"), "falls back to the catalog")
	require.True(t, h.logger.has("warn", "ignoring route home task queue"))

	p := devPolicy
	p.AllowedHomeQueues = []string{"exfil"}
	h = newHarness(t, p, "dev-yu", &Route{WorkflowTaskQueue: "dev-yu", HomeTaskQueue: "exfil"})
	h.run(t, parentInput{SkipChildren: true})
	require.Equal(t, "exfil", h.queue(t, "ParentWorkflow", "activity", "SendEmail"), "allowed by policy")
}

func TestExplicitActivityQueueIsKept(t *testing.T) {
	h := newHarness(t, devPolicy, "dev-yu", &Route{WorkflowTaskQueue: "dev-yu", HomeTaskQueue: "orders"})
	h.run(t, parentInput{ExplicitChargeQueue: "payments-v2", SkipChildren: true})
	require.Equal(t, "payments-v2", h.queue(t, "ParentWorkflow", "activity", "pay.Charge"))
}

func TestOverrideBeatsExplicitActivityQueue(t *testing.T) {
	h := newHarness(t, devPolicy, "orders", &Route{HomeTaskQueue: "orders", Activities: map[string]string{"pay.Charge": "dev-yu"}})
	h.run(t, parentInput{ExplicitChargeQueue: "payments-v2", SkipChildren: true})
	require.Equal(t, "dev-yu", h.queue(t, "ParentWorkflow", "activity", "pay.Charge"))
}

func TestWorkerPolicyDisabledIgnoresRoute(t *testing.T) {
	h := newHarness(t, Policy{}, "orders", &Route{HomeTaskQueue: "orders", Activities: map[string]string{"SendEmail": "dev-yu"}})
	h.run(t, parentInput{SkipChildren: true})
	require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "activity", "SendEmail"))
	require.True(t, h.logger.has("warn", "ignored parts of the run's route"))
}

func TestWorkerRejectsDisallowedQueue(t *testing.T) {
	h := newHarness(t, devPolicy, "orders", &Route{HomeTaskQueue: "orders", Activities: map[string]string{
		"SendEmail":  "orders-shadow",
		"acct.fetch": "dev-yu",
	}})
	h.run(t, parentInput{SkipChildren: true})
	require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "activity", "SendEmail"), "rejected override falls back to the home queue")
	require.Equal(t, "dev-yu", h.queue(t, "ParentWorkflow", "activity", "acct.fetch"))
	require.True(t, h.logger.has("warn", "ignored parts of the run's route"))
}

func TestWorkerRejectsUnlistedOwner(t *testing.T) {
	p := devPolicy
	p.AllowedOwners = []string{"alice"}
	h := newHarness(t, p, "orders", &Route{Owner: "mallory", HomeTaskQueue: "orders", Activities: map[string]string{"SendEmail": "dev-yu"}})
	h.run(t, parentInput{SkipChildren: true})
	require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "activity", "SendEmail"))
}

func nestedRoute() *Route {
	return &Route{Owner: "yu", HomeTaskQueue: "orders", ChildWorkflows: map[string]Route{
		"ChildWorkflow": {WorkflowTaskQueue: "dev-yu", Activities: map[string]string{"acct.fetch": "dev-yu"}},
	}}
}

func assertNestedRouting(t *testing.T, h *harness) {
	t.Helper()
	// Parent: no activity overrides of its own.
	require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "activity", "SendEmail"))

	// Configured child: own queue, own route with the owner and the queue it
	// would have run on.
	c, ok := h.rec.find("ParentWorkflow", "child", "ChildWorkflow")
	require.True(t, ok)
	require.Equal(t, "dev-yu", c.TaskQueue)
	require.NotNil(t, c.Route)
	require.Equal(t, Route{Owner: "yu", WorkflowTaskQueue: "dev-yu", HomeTaskQueue: "orders", Activities: map[string]string{"acct.fetch": "dev-yu"}}, *c.Route)

	start, ok := h.rec.find("ChildWorkflow", "start", "ChildWorkflow")
	require.True(t, ok)
	require.Equal(t, "dev-yu", start.TaskQueue)
	require.NotNil(t, start.Route, "child run must see its route from its start header")
	require.Equal(t, "dev-yu", start.Route.Activities["acct.fetch"])
	require.Equal(t, "dev-yu", h.queue(t, "ChildWorkflow", "activity", "acct.fetch"))
	require.Equal(t, "orders", h.queue(t, "ChildWorkflow", "activity", "SendEmail"))

	// Unconfigured sibling: no route, where production would have put it.
	c, ok = h.rec.find("ParentWorkflow", "child", "OtherChild")
	require.True(t, ok)
	require.Equal(t, "orders", c.TaskQueue)
	require.Nil(t, c.Route)
	start, ok = h.rec.find("OtherChild", "start", "OtherChild")
	require.True(t, ok)
	require.Nil(t, start.Route)
}

func TestNestedChildRoutes(t *testing.T) {
	h := newHarness(t, devPolicy, "orders", nestedRoute())
	h.run(t, parentInput{})
	assertNestedRouting(t, h)
}

// Workers inherit client interceptors, so the routing layer often runs twice.
func TestStackedInterceptorsRouteOnce(t *testing.T) {
	h := newHarness(t, devPolicy, "orders", nestedRoute())
	h.env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{
		h.rec,
		NewInterceptor(Options{Policy: devPolicy, Catalog: testCatalog()}),
		NewInterceptor(Options{Policy: devPolicy, Catalog: testCatalog()}),
	}})
	h.run(t, parentInput{})
	assertNestedRouting(t, h)
}

func TestChildOfDevRunGoesHome(t *testing.T) {
	h := newHarness(t, devPolicy, "dev-yu", &Route{WorkflowTaskQueue: "dev-yu", HomeTaskQueue: "orders"})
	h.run(t, parentInput{})
	require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "child", "ChildWorkflow"))
	require.Equal(t, "orders", h.queue(t, "ParentWorkflow", "child", "OtherChild"))

	h = newHarness(t, devPolicy, "dev-yu", &Route{WorkflowTaskQueue: "dev-yu"})
	h.run(t, parentInput{})
	require.Equal(t, "children", h.queue(t, "ParentWorkflow", "child", "ChildWorkflow"), "no route home: catalog")
}

func TestStructPrefixCalledByGoNameIsReported(t *testing.T) {
	h := newHarness(t, devPolicy, "orders", &Route{WorkflowTaskQueue: "dev-yu"})
	h.env.ExecuteWorkflow(ParentWorkflow, parentInput{CallChargeByGoName: true, SkipChildren: true})
	require.True(t, h.env.IsWorkflowCompleted())
	// The SDK schedules "Charge", which no worker registered.
	require.Error(t, h.env.GetWorkflowError())
	c, ok := h.rec.find("ParentWorkflow", "activity", "Charge")
	require.True(t, ok, "the type is passed through unchanged")
	require.Equal(t, "orders", c.TaskQueue)
	require.True(t, h.logger.has("error", "registered under a custom name"))
}

func TestActivityNames(t *testing.T) {
	cases := []struct {
		name string
		af   any
		opts activity.RegisterOptions
		want []activityName
	}{
		{"function", SendEmail, activity.RegisterOptions{}, []activityName{{"SendEmail", "SendEmail"}}},
		{"function with name", FetchAccount, activity.RegisterOptions{Name: "acct.fetch"}, []activityName{{"FetchAccount", "acct.fetch"}}},
		{"struct", &Payments{}, activity.RegisterOptions{}, []activityName{{"Charge", "Charge"}, {"Refund", "Refund"}}},
		{"struct with prefix", &Payments{}, activity.RegisterOptions{Name: "pay."}, []activityName{{"Charge", "pay.Charge"}, {"Refund", "pay.Refund"}}},
		{"method value", (&Payments{}).Charge, activity.RegisterOptions{}, []activityName{{"Charge", "Charge"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, activityNames(tc.af, tc.opts))
		})
	}
}

// TestCatalogMatchesSDKResolution checks the catalog's names against the type
// names the SDK actually schedules, for every registration and call style.
func TestCatalogMatchesSDKResolution(t *testing.T) {
	var p *Payments
	cases := []struct {
		name     string
		register func(env *testsuite.TestWorkflowEnvironment, c *Catalog)
		call     any
		want     string
	}{
		{"function by function", func(env *testsuite.TestWorkflowEnvironment, c *Catalog) {
			c.RegisterActivity(env, SendEmail, activity.RegisterOptions{}, "q")
		}, SendEmail, "SendEmail"},
		{"named function by function", func(env *testsuite.TestWorkflowEnvironment, c *Catalog) {
			c.RegisterActivity(env, FetchAccount, activity.RegisterOptions{Name: "acct.fetch"}, "q")
		}, FetchAccount, "acct.fetch"},
		{"named function by string", func(env *testsuite.TestWorkflowEnvironment, c *Catalog) {
			c.RegisterActivity(env, FetchAccount, activity.RegisterOptions{Name: "acct.fetch"}, "q")
		}, "acct.fetch", "acct.fetch"},
		{"struct by method", func(env *testsuite.TestWorkflowEnvironment, c *Catalog) {
			c.RegisterActivity(env, &Payments{}, activity.RegisterOptions{}, "q")
		}, p.Charge, "Charge"},
		{"prefixed struct by string", func(env *testsuite.TestWorkflowEnvironment, c *Catalog) {
			c.RegisterActivity(env, &Payments{}, activity.RegisterOptions{Name: "pay."}, "q")
		}, "pay.Charge", "pay.Charge"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			c := NewCatalog()
			tc.register(env, c)
			var scheduled string
			env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
				scheduled = info.ActivityType.Name
			})
			env.ExecuteWorkflow(func(ctx workflow.Context) error {
				ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
				return workflow.ExecuteActivity(ctx, tc.call).Get(ctx, nil)
			})
			require.NoError(t, env.GetWorkflowError())
			require.Equal(t, tc.want, scheduled)
			_, ok := c.ActivityHomeQueue(scheduled)
			require.True(t, ok, "catalog must know the scheduled name %q", scheduled)
		})
	}
}

func TestClientApplyStart(t *testing.T) {
	i := NewInterceptor(Options{Policy: Policy{Enabled: true}, Catalog: testCatalog()})

	opts := &client.StartWorkflowOptions{TaskQueue: "orders"}
	_, err := i.applyStart(context.Background(), opts)
	require.NoError(t, err)
	require.Equal(t, "orders", opts.TaskQueue, "no route, no change")

	ctx := WithRoute(context.Background(), Route{WorkflowTaskQueue: "dev-yu", Activities: map[string]string{"SendEmail": "dev-yu"}})
	ctx, err = i.applyStart(ctx, opts)
	require.NoError(t, err)
	require.Equal(t, "dev-yu", opts.TaskQueue)
	sent, _ := FromContext(ctx)
	require.Equal(t, "orders", sent.HomeTaskQueue, "the propagator sends the queue the run would have used")

	ctx = WithRoute(context.Background(), Route{Activities: map[string]string{"FetchAccount": "dev-yu"}})
	_, err = i.applyStart(ctx, &client.StartWorkflowOptions{TaskQueue: "orders"})
	require.ErrorContains(t, err, `activity "FetchAccount" is not in the catalog (it is registered as acct.fetch`)

	ctx = WithRoute(context.Background(), Route{Activities: map[string]string{"Charge": "dev-yu"}})
	_, err = i.applyStart(ctx, &client.StartWorkflowOptions{TaskQueue: "orders"})
	require.ErrorContains(t, err, "registered as pay.Charge")

	ctx = WithRoute(context.Background(), Route{ChildWorkflows: map[string]Route{"ChildWorkflow": {WorkflowTaskQueue: "orders"}}})
	_, err = i.applyStart(ctx, &client.StartWorkflowOptions{TaskQueue: "orders"})
	require.ErrorContains(t, err, `child ChildWorkflow: workflow task queue "orders" does not match`)

	disabled := NewInterceptor(Options{})
	ctx = WithRoute(context.Background(), Route{WorkflowTaskQueue: "dev-yu"})
	_, err = disabled.applyStart(ctx, &client.StartWorkflowOptions{})
	require.ErrorContains(t, err, "disabled by policy")
}

type mapHeader map[string]*commonpb.Payload

func (h mapHeader) Set(k string, p *commonpb.Payload)      { h[k] = p }
func (h mapHeader) Get(k string) (*commonpb.Payload, bool) { p, ok := h[k]; return p, ok }
func (h mapHeader) ForEachKey(fn func(string, *commonpb.Payload) error) error {
	for k, p := range h {
		if err := fn(k, p); err != nil {
			return err
		}
	}
	return nil
}

func TestPropagatorRoundTrip(t *testing.T) {
	p := NewContextPropagator(devPolicy)
	want := Route{Owner: "yu", WorkflowTaskQueue: "dev-yu", ChildWorkflows: map[string]Route{"C": {Activities: map[string]string{"A": "dev-yu"}}}}
	h := mapHeader{}
	require.NoError(t, p.Inject(WithRoute(context.Background(), want), h))
	ctx, err := p.Extract(context.Background(), h)
	require.NoError(t, err)
	got, ok := FromContext(ctx)
	require.True(t, ok)
	want.ChildWorkflows["C"] = Route{Owner: "yu", Activities: map[string]string{"A": "dev-yu"}}
	require.Equal(t, want, got)

	empty := mapHeader{}
	require.NoError(t, p.Inject(context.Background(), empty))
	require.Empty(t, empty)

	bad := mapHeader{HeaderKey: &commonpb.Payload{Data: []byte("not json")}}
	ctx, err = p.Extract(context.Background(), bad)
	require.NoError(t, err, "a bad header must not fail the activity")
	_, ok = FromContext(ctx)
	require.False(t, ok)
}

func TestPolicyFromEnv(t *testing.T) {
	t.Setenv("ROUTING_OVERRIDES_ENABLED", "true")
	t.Setenv("ROUTING_ALLOWED_OWNERS", "alice, bob")
	t.Setenv("ROUTING_SCHEDULE_TO_START_TIMEOUT", "2m")
	t.Setenv("ROUTING_HOME_QUEUES", "orders,payments")
	p, err := PolicyFromEnv()
	require.NoError(t, err)
	require.True(t, p.Enabled)
	require.Equal(t, []string{"alice", "bob"}, p.AllowedOwners)
	require.Equal(t, 2*time.Minute, p.OverrideScheduleToStartTimeout)
	require.Equal(t, []string{"orders", "payments"}, p.AllowedHomeQueues)

	t.Setenv("ROUTING_ALLOWED_QUEUE", "(")
	_, err = PolicyFromEnv()
	require.ErrorContains(t, err, "ROUTING_ALLOWED_QUEUE")
}

func ExampleWithRoute() {
	// Parent runs on production; child Settle runs on the local worker and
	// sends its ChargeCard activity to the local worker too.
	ctx := WithRoute(context.Background(), Route{
		Owner: "yu",
		ChildWorkflows: map[string]Route{
			"Settle": {WorkflowTaskQueue: "dev-yu", Activities: map[string]string{"ChargeCard": "dev-yu"}},
		},
	})
	r, _ := FromContext(ctx)
	fmt.Println(r.ChildWorkflows["Settle"].WorkflowTaskQueue)
	// Output: dev-yu
}
