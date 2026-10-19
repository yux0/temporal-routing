package routing

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

// HeaderKey is the Temporal header that carries the route.
const HeaderKey = "x-routing"

type propagator struct {
	policy Policy
}

// NewContextPropagator returns the propagator that carries a [Route] between
// the client, workflows, child workflows and activities. Register it in
// client.Options.ContextPropagators on every client, including the ones
// workers are created from. The worker honors only what policy allows.
func NewContextPropagator(policy Policy) workflow.ContextPropagator {
	return &propagator{policy: policy}
}

func (p *propagator) Inject(ctx context.Context, w workflow.HeaderWriter) error {
	r, ok := FromContext(ctx)
	if !ok || r.isEmpty() {
		return nil
	}
	return writeRoute(w, r)
}

func (p *propagator) Extract(ctx context.Context, h workflow.HeaderReader) (context.Context, error) {
	r, ok, err := readRoute(h)
	if err != nil || !ok {
		// The route is informational for activities; a bad header must not
		// fail the activity.
		return ctx, nil
	}
	r, _ = p.policy.sanitize(r)
	return WithRoute(ctx, r), nil
}

func (p *propagator) InjectFromWorkflow(ctx workflow.Context, w workflow.HeaderWriter) error {
	r, ok := FromWorkflowContext(ctx)
	if !ok || r.isEmpty() {
		return nil
	}
	return writeRoute(w, r)
}

// ExtractToWorkflow trusts only the first header it sees for a run, the start
// header. The SDK also extracts signal, query and update headers into contexts
// derived from the root context; ignoring those keeps a signal sender from
// rerouting a run that is already executing.
func (p *propagator) ExtractToWorkflow(ctx workflow.Context, h workflow.HeaderReader) (workflow.Context, error) {
	if ctx.Value(workflowContextKey{}) != nil {
		return ctx, nil
	}
	state := &workflowState{}
	r, ok, err := readRoute(h)
	if err != nil {
		// Returning the error would fail every workflow task of the run.
		state.rejected = []string{err.Error()}
	} else if ok {
		sanitized, rejected := p.policy.sanitize(r)
		state.rejected = rejected
		if !sanitized.isEmpty() {
			state.route = &sanitized
		}
	}
	return workflow.WithValue(ctx, workflowContextKey{}, state), nil
}

func writeRoute(w workflow.HeaderWriter, r Route) error {
	payload, err := converter.GetDefaultDataConverter().ToPayload(r)
	if err != nil {
		return fmt.Errorf("routing: encode %s header: %w", HeaderKey, err)
	}
	w.Set(HeaderKey, payload)
	return nil
}

func readRoute(h workflow.HeaderReader) (Route, bool, error) {
	payload, ok := h.Get(HeaderKey)
	if !ok {
		return Route{}, false, nil
	}
	var r Route
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &r); err != nil {
		return Route{}, false, fmt.Errorf("routing: decode %s header: %w", HeaderKey, err)
	}
	return r, true, nil
}
