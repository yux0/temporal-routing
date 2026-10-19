package routing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func UpdatableWorkflow(ctx workflow.Context) error {
	done := false
	if err := workflow.SetUpdateHandler(ctx, "send", func(uctx workflow.Context) error {
		uctx = workflow.WithActivityOptions(uctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		return workflow.ExecuteActivity(uctx, SendEmail).Get(uctx, nil)
	}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, "finish", func(workflow.Context) error {
		done = true
		return nil
	}); err != nil {
		return err
	}
	return workflow.Await(ctx, func() bool { return done })
}

// ReplayableWorkflow calls activities by registered name only, because
// WorkflowReplayer cannot register activities and so cannot resolve aliases.
func ReplayableWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err := workflow.ExecuteActivity(ctx, "SendEmail").Get(ctx, nil); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, "pay.Charge").Get(ctx, nil)
}

type cluster struct {
	t      *testing.T
	client client.Client
}

func startCluster(t *testing.T) *cluster {
	if testing.Short() {
		t.Skip("starts a Temporal dev server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{LogLevel: "error"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Stop() })

	routing := NewInterceptor(Options{Policy: devPolicy, Catalog: testCatalog()})
	c, err := client.Dial(client.Options{
		HostPort:           srv.FrontendHostPort(),
		ContextPropagators: []workflow.ContextPropagator{NewContextPropagator(devPolicy)},
		Interceptors:       []interceptor.ClientInterceptor{routing},
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)

	cl := &cluster{t: t, client: c}
	registerWorkflows := func(w worker.Worker) {
		for _, wf := range []any{ParentWorkflow, ChildWorkflow, OtherChild, UpdatableWorkflow, ReplayableWorkflow} {
			w.RegisterWorkflow(wf)
		}
	}
	registerActivities := func(w worker.Worker) {
		w.RegisterActivityWithOptions(FetchAccount, activity.RegisterOptions{Name: "acct.fetch"})
		w.RegisterActivity(SendEmail)
		w.RegisterActivityWithOptions(&Payments{}, activity.RegisterOptions{Name: "pay."})
	}
	// Production: one worker per workflow queue hosting workflows and
	// activities. The developer's laptop polls dev-yu.
	cl.worker("orders", "prod-orders", registerWorkflows, registerActivities)
	cl.worker("children", "prod-children", registerWorkflows, registerActivities)
	cl.worker("dev-yu", "dev-laptop", registerWorkflows, registerActivities)
	return cl
}

func (cl *cluster) worker(queue, identity string, register ...func(worker.Worker)) {
	// The routing interceptor comes from the client's interceptors.
	w := worker.New(cl.client, queue, worker.Options{Identity: identity})
	for _, r := range register {
		r(w)
	}
	require.NoError(cl.t, w.Start())
	cl.t.Cleanup(w.Stop)
}

// dumpOpen logs where every open workflow's tasks went, to show which queue
// a stuck run is waiting on.
func (cl *cluster) dumpOpen() {
	resp, err := cl.client.ListWorkflow(context.Background(), &workflowservice.ListWorkflowExecutionsRequest{Query: "ExecutionStatus = 'Running'"})
	if err != nil {
		cl.t.Logf("list open workflows: %v", err)
		return
	}
	for _, e := range resp.Executions {
		h := cl.history(e.Execution.WorkflowId, e.Execution.RunId)
		cl.t.Logf("open %s (%s) on %s: %+v", e.Execution.WorkflowId, e.Type.Name, e.TaskQueue, summarize(h))
		last := h.Events[len(h.Events)-1]
		cl.t.Logf("  last event %d %s", last.EventId, last.EventType)
	}
}

func timeout(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func (cl *cluster) history(id, runID string) *historypb.History {
	iter := cl.client.GetWorkflowHistory(context.Background(), id, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	h := &historypb.History{}
	for iter.HasNext() {
		e, err := iter.Next()
		require.NoError(cl.t, err)
		h.Events = append(h.Events, e)
	}
	return h
}

// dispatch summarizes where a run's tasks went and which worker took them.
type dispatch struct {
	workflowTaskIdentity string               // identity that completed the first workflow task
	activityQueue        map[string]string    // activity type → scheduled task queue
	activityIdentity     map[string]string    // activity type → identity that started it
	childQueue           map[string]string    // child type → task queue
	childExecution       map[string][2]string // child type → workflow ID, run ID
}

func summarize(h *historypb.History) dispatch {
	d := dispatch{activityQueue: map[string]string{}, activityIdentity: map[string]string{}, childQueue: map[string]string{}, childExecution: map[string][2]string{}}
	scheduled := map[int64]string{}
	for _, e := range h.Events {
		switch a := e.Attributes.(type) {
		case *historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes:
			if d.workflowTaskIdentity == "" {
				d.workflowTaskIdentity = a.WorkflowTaskCompletedEventAttributes.Identity
			}
		case *historypb.HistoryEvent_ActivityTaskScheduledEventAttributes:
			attrs := a.ActivityTaskScheduledEventAttributes
			scheduled[e.EventId] = attrs.ActivityType.Name
			d.activityQueue[attrs.ActivityType.Name] = attrs.TaskQueue.Name
		case *historypb.HistoryEvent_ActivityTaskStartedEventAttributes:
			attrs := a.ActivityTaskStartedEventAttributes
			d.activityIdentity[scheduled[attrs.ScheduledEventId]] = attrs.Identity
		case *historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes:
			attrs := a.StartChildWorkflowExecutionInitiatedEventAttributes
			d.childQueue[attrs.WorkflowType.Name] = attrs.TaskQueue.Name
		case *historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes:
			attrs := a.ChildWorkflowExecutionStartedEventAttributes
			d.childExecution[attrs.WorkflowType.Name] = [2]string{attrs.WorkflowExecution.WorkflowId, attrs.WorkflowExecution.RunId}
		}
	}
	return d
}

func TestIntegrationRoutedRun(t *testing.T) {
	cl := startCluster(t)
	ctx := WithRoute(context.Background(), Route{
		Owner:             "yu",
		WorkflowTaskQueue: "dev-yu",
		Activities:        map[string]string{"SendEmail": "dev-yu"},
		ChildWorkflows: map[string]Route{
			"ChildWorkflow": {WorkflowTaskQueue: "dev-yu", Activities: map[string]string{"acct.fetch": "dev-yu"}},
		},
	})
	id := fmt.Sprintf("dev-yu-%d", time.Now().UnixNano())
	run, err := cl.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: "orders"}, ParentWorkflow, parentInput{})
	require.NoError(t, err)
	if err := run.Get(timeout(t), nil); err != nil {
		cl.dumpOpen()
		require.NoError(t, err)
	}

	parent := summarize(cl.history(id, run.GetRunID()))
	require.Equal(t, "dev-laptop", parent.workflowTaskIdentity, "parent workflow tasks run on the local worker")
	require.Equal(t, map[string]string{"acct.fetch": "orders", "SendEmail": "dev-yu", "pay.Charge": "orders"}, parent.activityQueue)
	require.Equal(t, map[string]string{"acct.fetch": "prod-orders", "SendEmail": "dev-laptop", "pay.Charge": "prod-orders"}, parent.activityIdentity)
	require.Equal(t, map[string]string{"ChildWorkflow": "dev-yu", "OtherChild": "orders"}, parent.childQueue)

	ce := parent.childExecution["ChildWorkflow"]
	child := summarize(cl.history(ce[0], ce[1]))
	require.Equal(t, "dev-laptop", child.workflowTaskIdentity)
	require.Equal(t, map[string]string{"acct.fetch": "dev-yu", "SendEmail": "orders"}, child.activityQueue)
	require.Equal(t, map[string]string{"acct.fetch": "dev-laptop", "SendEmail": "prod-orders"}, child.activityIdentity)

	oe := parent.childExecution["OtherChild"]
	other := summarize(cl.history(oe[0], oe[1]))
	require.Equal(t, "prod-orders", other.workflowTaskIdentity, "unconfigured child runs where production would run it")
	require.Equal(t, map[string]string{"SendEmail": "orders"}, other.activityQueue)
}

// An update (or signal) sent with a routed context carries the route in its
// header. It must not reroute a run that is already executing.
func TestIntegrationUpdateHeaderCannotReroute(t *testing.T) {
	cl := startCluster(t)
	startCtx := WithRoute(context.Background(), Route{Owner: "yu", Activities: map[string]string{"acct.fetch": "dev-yu"}})
	id := fmt.Sprintf("dev-yu-upd-%d", time.Now().UnixNano())
	run, err := cl.client.ExecuteWorkflow(startCtx, client.StartWorkflowOptions{ID: id, TaskQueue: "orders"}, UpdatableWorkflow)
	require.NoError(t, err)

	updCtx := WithRoute(context.Background(), Route{Owner: "mallory", Activities: map[string]string{"SendEmail": "dev-mallory"}})
	h, err := cl.client.UpdateWorkflow(updCtx, client.UpdateWorkflowOptions{WorkflowID: id, UpdateName: "send", WaitForStage: client.WorkflowUpdateStageCompleted})
	require.NoError(t, err)
	require.NoError(t, h.Get(timeout(t), nil))
	h, err = cl.client.UpdateWorkflow(context.Background(), client.UpdateWorkflowOptions{WorkflowID: id, UpdateName: "finish", WaitForStage: client.WorkflowUpdateStageCompleted})
	require.NoError(t, err)
	require.NoError(t, h.Get(timeout(t), nil))
	require.NoError(t, run.Get(timeout(t), nil))

	d := summarize(cl.history(id, run.GetRunID()))
	require.Equal(t, "orders", d.activityQueue["SendEmail"], "the start header's route applies, not the update header's")
}

// Routing only changes task queues, which replay does not compare, so a routed
// history replays with different routing rules or none at all.
func TestIntegrationRoutedHistoryReplays(t *testing.T) {
	cl := startCluster(t)
	ctx := WithRoute(context.Background(), Route{Owner: "yu", Activities: map[string]string{"SendEmail": "dev-yu"}})
	id := fmt.Sprintf("dev-yu-replay-%d", time.Now().UnixNano())
	run, err := cl.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: "orders"}, ReplayableWorkflow)
	require.NoError(t, err)
	require.NoError(t, run.Get(timeout(t), nil))
	hist := cl.history(id, run.GetRunID())
	require.Equal(t, "dev-yu", summarize(hist).activityQueue["SendEmail"])

	changed := NewCatalog()
	changed.AddActivity(SendEmail, activity.RegisterOptions{}, "notify-v2")
	changed.AddActivity(&Payments{}, activity.RegisterOptions{Name: "pay."}, "payments-v2")
	for name, opts := range map[string]worker.WorkflowReplayerOptions{
		"same rules": {
			ContextPropagators: []workflow.ContextPropagator{NewContextPropagator(devPolicy)},
			Interceptors:       []interceptor.WorkerInterceptor{NewInterceptor(Options{Policy: devPolicy, Catalog: testCatalog()})},
		},
		"changed catalog": {
			ContextPropagators: []workflow.ContextPropagator{NewContextPropagator(devPolicy)},
			Interceptors:       []interceptor.WorkerInterceptor{NewInterceptor(Options{Policy: devPolicy, Catalog: changed})},
		},
		"routing disabled": {
			ContextPropagators: []workflow.ContextPropagator{NewContextPropagator(Policy{})},
			Interceptors:       []interceptor.WorkerInterceptor{NewInterceptor(Options{})},
		},
		"no routing layer": {},
	} {
		t.Run(name, func(t *testing.T) {
			r, err := worker.NewWorkflowReplayerWithOptions(opts)
			require.NoError(t, err)
			r.RegisterWorkflow(ReplayableWorkflow)
			require.NoError(t, r.ReplayWorkflowHistory(nil, hist))
		})
	}
}

// Documents an SDK limitation that affects replay testing of routed (and any)
// histories: WorkflowReplayer has no RegisterActivity, so a workflow that
// calls an activity by function while it was registered under a custom name
// schedules the Go name on replay and does not match the recorded history.
func TestIntegrationReplayerCannotResolveActivityAlias(t *testing.T) {
	cl := startCluster(t)
	id := fmt.Sprintf("alias-%d", time.Now().UnixNano())
	run, err := cl.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: id, TaskQueue: "orders"}, ChildWorkflow)
	require.NoError(t, err)
	require.NoError(t, run.Get(timeout(t), nil))
	hist := cl.history(id, run.GetRunID())
	require.Contains(t, summarize(hist).activityQueue, "acct.fetch")

	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflow(ChildWorkflow)
	err = r.ReplayWorkflowHistory(nil, hist)
	require.ErrorContains(t, err, "nondeterministic")
}
