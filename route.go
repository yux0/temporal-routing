// Package routing lets a caller choose, per workflow run, which task queue the
// run's workflow tasks and each of its activities (by registered type name) are
// dispatched to. It is meant for pointing parts of a run at a local dev worker
// while the rest keeps running on production workers.
//
// A [Route] is attached to a Go context with [WithRoute], written into the
// workflow start header by the context propagator from [NewContextPropagator],
// and applied by the [Interceptor] on both the client and the worker.
package routing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"
)

// Route describes task-queue overrides for one workflow run.
type Route struct {
	// WorkflowTaskQueue overrides the task queue the run's workflow tasks go to.
	// Empty means the queue given in the start (or child workflow) options.
	WorkflowTaskQueue string `json:"workflowTaskQueue,omitempty"`
	// Activities maps a registered activity type name to the task queue that
	// activity is dispatched to from this run.
	Activities map[string]string `json:"activities,omitempty"`
	// ChildWorkflows maps a child workflow type name to the Route that child
	// run starts with. Children not listed here start without a route.
	ChildWorkflows map[string]Route `json:"childWorkflows,omitempty"`
	// Owner identifies who created the route, for audit logs and allowlists.
	Owner string `json:"owner,omitempty"`
	// HomeTaskQueue is the task queue the run would run on without the route.
	// Activities and child workflows without an override go where they would
	// have gone from there, so the rest of the run behaves like production.
	// The client interceptor fills it in; the interceptor fills it in for
	// child routes. Workers only use it if it is a trusted queue (see
	// Policy.AllowedHomeQueues).
	HomeTaskQueue string `json:"homeTaskQueue,omitempty"`
}

// isEmpty ignores Owner and HomeTaskQueue: a route without overrides routes
// nothing.
func (r Route) isEmpty() bool {
	return r.WorkflowTaskQueue == "" && len(r.Activities) == 0 && len(r.ChildWorkflows) == 0
}

type contextKey struct{}

// WithRoute returns a copy of ctx carrying r. Pass the result to
// client.ExecuteWorkflow or client.SignalWithStartWorkflow.
func WithRoute(ctx context.Context, r Route) context.Context {
	return context.WithValue(ctx, contextKey{}, r)
}

// FromContext returns the route carried by ctx. Inside an activity it returns
// the route of the workflow run that scheduled the activity.
func FromContext(ctx context.Context) (Route, bool) {
	r, ok := ctx.Value(contextKey{}).(Route)
	return r, ok
}

// workflowState is stored in workflow contexts. A nil route with a non-nil
// state means the start header was processed and carried no (valid) route.
type workflowState struct {
	route    *Route
	rejected []string
}

type workflowContextKey struct{}

// FromWorkflowContext returns the route the current workflow run started with,
// after the worker's [Policy] removed anything it does not allow.
func FromWorkflowContext(ctx workflow.Context) (Route, bool) {
	s, _ := ctx.Value(workflowContextKey{}).(*workflowState)
	if s == nil || s.route == nil {
		return Route{}, false
	}
	return *s.route, true
}

func withWorkflowRoute(ctx workflow.Context, r *Route) workflow.Context {
	return workflow.WithValue(ctx, workflowContextKey{}, &workflowState{route: r})
}

// DefaultAllowedQueue is the pattern override task queues must match when
// [Policy.AllowedQueue] is nil.
var DefaultAllowedQueue = regexp.MustCompile(`^dev-[a-z0-9][a-z0-9-]*$`)

// Policy decides which routes a client may send and a worker will honor. The
// zero value honors nothing, so production workers are safe by default.
type Policy struct {
	// Enabled must be true for any route to be honored.
	Enabled bool
	// AllowedQueue is the pattern every override task queue must match.
	// Nil means DefaultAllowedQueue.
	AllowedQueue *regexp.Regexp
	// AllowedOwners restricts which Route.Owner values are honored. Empty
	// allows any owner, including none.
	AllowedOwners []string
	// OverrideScheduleToStartTimeout, when positive, is applied to activities
	// dispatched to an override queue that have no ScheduleToStartTimeout, so a
	// run fails fast when the dev worker is not running.
	OverrideScheduleToStartTimeout time.Duration
	// AllowedHomeQueues lists task queues a route's HomeTaskQueue may name,
	// in addition to every home queue recorded in the Catalog. HomeTaskQueue
	// arrives in a header, so it is only trusted if it is a known queue.
	AllowedHomeQueues []string
}

// PolicyFromEnv builds a Policy from environment variables:
//
//	ROUTING_OVERRIDES_ENABLED=true
//	ROUTING_ALLOWED_QUEUE=^dev-[a-z0-9-]+$       (optional)
//	ROUTING_ALLOWED_OWNERS=alice,bob             (optional)
//	ROUTING_SCHEDULE_TO_START_TIMEOUT=2m         (optional)
//	ROUTING_HOME_QUEUES=orders,payments          (optional)
func PolicyFromEnv() (Policy, error) {
	var p Policy
	switch strings.ToLower(os.Getenv("ROUTING_OVERRIDES_ENABLED")) {
	case "1", "true", "yes":
		p.Enabled = true
	}
	if v := os.Getenv("ROUTING_ALLOWED_QUEUE"); v != "" {
		re, err := regexp.Compile(v)
		if err != nil {
			return Policy{}, fmt.Errorf("ROUTING_ALLOWED_QUEUE %q: %w", v, err)
		}
		p.AllowedQueue = re
	}
	p.AllowedOwners = splitList(os.Getenv("ROUTING_ALLOWED_OWNERS"))
	p.AllowedHomeQueues = splitList(os.Getenv("ROUTING_HOME_QUEUES"))
	if v := os.Getenv("ROUTING_SCHEDULE_TO_START_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Policy{}, fmt.Errorf("ROUTING_SCHEDULE_TO_START_TIMEOUT %q: %w", v, err)
		}
		p.OverrideScheduleToStartTimeout = d
	}
	return p, nil
}

func splitList(v string) []string {
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func (p Policy) allowedQueue() *regexp.Regexp {
	if p.AllowedQueue != nil {
		return p.AllowedQueue
	}
	return DefaultAllowedQueue
}

// Validate reports every part of r the policy does not allow.
func (p Policy) Validate(r Route) error {
	_, rejected := p.sanitize(r)
	if len(rejected) == 0 {
		return nil
	}
	return errors.New("routing: route rejected: " + strings.Join(rejected, "; "))
}

// sanitize returns r with every entry the policy does not allow removed, and a
// description of each removed entry.
func (p Policy) sanitize(r Route) (Route, []string) {
	if r.isEmpty() {
		return r, nil
	}
	if !p.Enabled {
		return Route{}, []string{"route overrides are disabled by policy"}
	}
	if len(p.AllowedOwners) > 0 && !slices.Contains(p.AllowedOwners, r.Owner) {
		return Route{}, []string{fmt.Sprintf("owner %q is not in the allowed owners list", r.Owner)}
	}
	var rejected []string
	p.sanitizeInto(&r, "", &rejected)
	return r, rejected
}

func (p Policy) sanitizeInto(r *Route, path string, rejected *[]string) {
	re := p.allowedQueue()
	if r.WorkflowTaskQueue != "" && !re.MatchString(r.WorkflowTaskQueue) {
		*rejected = append(*rejected, fmt.Sprintf("%sworkflow task queue %q does not match %s", path, r.WorkflowTaskQueue, re))
		r.WorkflowTaskQueue = ""
	}
	if len(r.Activities) > 0 {
		acts := make(map[string]string, len(r.Activities))
		for _, name := range workflow.DeterministicKeys(r.Activities) {
			q := r.Activities[name]
			if !re.MatchString(q) {
				*rejected = append(*rejected, fmt.Sprintf("%sactivity %q task queue %q does not match %s", path, name, q, re))
				continue
			}
			acts[name] = q
		}
		r.Activities = acts
	}
	if len(r.ChildWorkflows) > 0 {
		children := make(map[string]Route, len(r.ChildWorkflows))
		for _, name := range workflow.DeterministicKeys(r.ChildWorkflows) {
			child := r.ChildWorkflows[name]
			child.Owner = r.Owner
			p.sanitizeInto(&child, path+"child "+name+": ", rejected)
			children[name] = child
		}
		r.ChildWorkflows = children
	}
}
