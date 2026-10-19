package routing

import (
	"context"
	"errors"
	"slices"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// Options configures an [Interceptor].
type Options struct {
	// Policy decides which routes are sent (client) and honored (worker).
	Policy Policy
	// Catalog supplies home task queues and valid type names. Optional, but
	// without it activities of a run routed to a dev queue also go to the dev
	// queue unless the workflow code sets their task queue explicitly.
	Catalog *Catalog
}

// Interceptor applies routes. Register it in client.Options.Interceptors:
// workers created from that client pick it up automatically, because it also
// implements interceptor.WorkerInterceptor. Registering it again in
// worker.Options.Interceptors is harmless.
type Interceptor struct {
	interceptor.InterceptorBase
	opts Options
}

// NewInterceptor returns an Interceptor for opts.
func NewInterceptor(opts Options) *Interceptor {
	return &Interceptor{opts: opts}
}

func (i *Interceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	c := &clientOutbound{root: i}
	c.Next = next
	return c
}

func (i *Interceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	w := &workflowInbound{root: i}
	w.Next = next
	return w
}

type clientOutbound struct {
	interceptor.ClientOutboundInterceptorBase
	root *Interceptor
}

func (c *clientOutbound) ExecuteWorkflow(ctx context.Context, in *interceptor.ClientExecuteWorkflowInput) (client.WorkflowRun, error) {
	ctx, err := c.root.applyStart(ctx, in.Options)
	if err != nil {
		return nil, err
	}
	return c.Next.ExecuteWorkflow(ctx, in)
}

func (c *clientOutbound) SignalWithStartWorkflow(ctx context.Context, in *interceptor.ClientSignalWithStartWorkflowInput) (client.WorkflowRun, error) {
	ctx, err := c.root.applyStart(ctx, in.Options)
	if err != nil {
		return nil, err
	}
	return c.Next.SignalWithStartWorkflow(ctx, in)
}

// applyStart rejects a route the policy or catalog does not allow, so the
// caller learns about it instead of the run silently ignoring it. It records
// the run's home queue in the route and sets the initial workflow task queue.
// The returned context carries the completed route for the propagator.
func (i *Interceptor) applyStart(ctx context.Context, opts *client.StartWorkflowOptions) (context.Context, error) {
	r, ok := FromContext(ctx)
	if !ok || r.isEmpty() {
		return ctx, nil
	}
	if err := errors.Join(i.opts.Policy.Validate(r), i.opts.Catalog.Validate(r)); err != nil {
		return ctx, err
	}
	if r.HomeTaskQueue == "" {
		r.HomeTaskQueue = opts.TaskQueue
	}
	if r.WorkflowTaskQueue != "" {
		opts.TaskQueue = r.WorkflowTaskQueue
	}
	return WithRoute(ctx, r), nil
}

type workflowInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	root *Interceptor
}

func (w *workflowInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	o := &workflowOutbound{root: w.root}
	o.Next = outbound
	return w.Next.Init(o)
}

// The SDK adds client interceptors that are also worker interceptors to every
// worker made from the client, so the routing layer is often stacked twice.
// Applying a route twice is not idempotent for child workflows (the second
// pass would look the child up in the child's own route), so the outer layer
// marks each call it handled and inner layers pass it through.
type (
	routedCallKey  struct{}
	loggedStartKey struct{}
)

func (w *workflowInbound) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	if ctx.Value(loggedStartKey{}) != nil {
		return w.Next.ExecuteWorkflow(ctx, in)
	}
	ctx = workflow.WithValue(ctx, loggedStartKey{}, true)
	if s, _ := ctx.Value(workflowContextKey{}).(*workflowState); s != nil {
		logger := workflow.GetLogger(ctx)
		if len(s.rejected) > 0 {
			logger.Warn("routing: ignored parts of the run's route", "Rejected", s.rejected)
		}
		if s.route != nil {
			logger.Info("routing: run started with a route", "Owner", s.route.Owner, "Route", *s.route)
		}
	}
	return w.Next.ExecuteWorkflow(ctx, in)
}

type workflowOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *Interceptor
}

func (o *workflowOutbound) ExecuteActivity(ctx workflow.Context, activityType string, args ...any) workflow.Future {
	if ctx.Value(routedCallKey{}) == nil {
		ctx = workflow.WithValue(o.root.routeActivity(ctx, activityType), routedCallKey{}, true)
	}
	return o.Next.ExecuteActivity(ctx, activityType, args...)
}

func (o *workflowOutbound) ExecuteChildWorkflow(ctx workflow.Context, childWorkflowType string, args ...any) workflow.ChildWorkflowFuture {
	if ctx.Value(routedCallKey{}) == nil {
		ctx = workflow.WithValue(o.root.routeChild(ctx, childWorkflowType), routedCallKey{}, true)
	}
	return o.Next.ExecuteChildWorkflow(ctx, childWorkflowType, args...)
}

// Activity task queue precedence: the route's override for the activity type,
// then a queue the workflow code set explicitly, then the run's home queue
// (where the activity would have gone without the route), then the catalog
// home queue. Only routed runs and runs on an override queue are changed, so
// unrouted production runs behave exactly as without this interceptor.
func (i *Interceptor) routeActivity(ctx workflow.Context, activityType string) workflow.Context {
	logger := workflow.GetLogger(ctx)
	i.checkActivityName(logger, activityType)

	route, routed := FromWorkflowContext(ctx)
	if q, ok := route.Activities[activityType]; routed && ok {
		opts := workflow.GetActivityOptions(ctx)
		opts.TaskQueue = q
		if t := i.opts.Policy.OverrideScheduleToStartTimeout; t > 0 && opts.ScheduleToStartTimeout == 0 {
			opts.ScheduleToStartTimeout = t
		}
		logger.Info("routing: activity sent to override task queue",
			"ActivityType", activityType, "TaskQueue", q, "Owner", route.Owner)
		return workflow.WithActivityOptions(ctx, opts)
	}

	current := workflow.GetInfo(ctx).TaskQueueName
	if !routed && !i.isOverrideQueue(current) {
		return ctx
	}
	// The SDK seeds the activity task queue with the run's own queue, so equal
	// means the workflow code did not choose one.
	if q := workflow.GetActivityOptions(ctx).TaskQueue; q != "" && q != current {
		return ctx
	}
	home := i.trustedHome(logger, route.HomeTaskQueue)
	if home == "" {
		home, _ = i.opts.Catalog.ActivityHomeQueue(activityType)
	}
	if home == "" {
		logger.Warn("routing: activity has no home task queue; dispatching to the run's task queue",
			"ActivityType", activityType, "TaskQueue", current)
		return ctx
	}
	return workflow.WithTaskQueue(ctx, home)
}

// A child's home queue is where it would have started without the route: a
// queue the workflow code set explicitly, else the parent's home queue (child
// workflows default to the parent's queue), else the catalog home queue.
// A listed child starts with its own route, on its override queue if it has
// one and on its home queue otherwise. Any other child of a routed run starts
// on its home queue without a route, so it runs exactly as in production.
func (i *Interceptor) routeChild(ctx workflow.Context, childWorkflowType string) workflow.Context {
	route, routed := FromWorkflowContext(ctx)
	current := workflow.GetInfo(ctx).TaskQueueName
	if !routed && !i.isOverrideQueue(current) {
		return ctx
	}
	logger := workflow.GetLogger(ctx)

	home := workflow.GetChildWorkflowOptions(ctx).TaskQueue
	if home == current {
		home = ""
	}
	if home == "" {
		home = i.trustedHome(logger, route.HomeTaskQueue)
	}
	if home == "" {
		home, _ = i.opts.Catalog.WorkflowHomeQueue(childWorkflowType)
	}

	if child, ok := route.ChildWorkflows[childWorkflowType]; ok && !child.isEmpty() {
		child.HomeTaskQueue = home
		ctx = withWorkflowRoute(ctx, &child)
		if child.WorkflowTaskQueue != "" {
			logger.Info("routing: child workflow sent to override task queue",
				"WorkflowType", childWorkflowType, "TaskQueue", child.WorkflowTaskQueue, "Owner", child.Owner)
			return workflow.WithWorkflowTaskQueue(ctx, child.WorkflowTaskQueue)
		}
	} else {
		ctx = withWorkflowRoute(ctx, nil)
	}

	if home == "" {
		logger.Warn("routing: child workflow has no home task queue; starting it on the run's task queue",
			"WorkflowType", childWorkflowType, "TaskQueue", current)
		return ctx
	}
	return workflow.WithWorkflowTaskQueue(ctx, home)
}

func (i *Interceptor) isOverrideQueue(q string) bool {
	return i.opts.Policy.allowedQueue().MatchString(q)
}

// trustedHome returns q if work may be sent there as a home queue. The value
// comes from a header, so an unknown queue could otherwise be used to pull
// production activities to an arbitrary poller.
func (i *Interceptor) trustedHome(logger log.Logger, q string) string {
	if q == "" {
		return ""
	}
	if i.opts.Catalog.KnowsQueue(q) || slices.Contains(i.opts.Policy.AllowedHomeQueues, q) {
		return q
	}
	logger.Warn("routing: ignoring route home task queue that is not in the catalog or Policy.AllowedHomeQueues",
		"HomeTaskQueue", q)
	return ""
}

// checkActivityName logs when a workflow schedules an activity by Go function
// name although it was registered under a custom name. That happens when the
// worker running the workflow did not register the alias, or when a struct was
// registered with a name prefix (the SDK records no alias for those). The type
// is never rewritten: that would change in-flight runs' commands on replay.
func (i *Interceptor) checkActivityName(logger log.Logger, activityType string) {
	if i.opts.Catalog == nil {
		return
	}
	if _, ok := i.opts.Catalog.ActivityHomeQueue(activityType); ok {
		return
	}
	if names := i.opts.Catalog.RegisteredNamesFor(activityType); len(names) > 0 {
		logger.Error("routing: activity scheduled by its Go name but registered under a custom name; call it by the registered name or register the alias on this worker",
			"ActivityType", activityType, "RegisteredAs", names)
	}
}
