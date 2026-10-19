package example

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/yux0/temporal-routing"
)

// TaskQueue is the production task queue of every workflow and activity in
// this sample.
const TaskQueue = "task-routing"

// OrderWorkflow is the version deployed to production workers. It returns one
// line per step saying which task queue ran it, so a routed run shows where
// each part executed.
func OrderWorkflow(ctx workflow.Context, orderID string) ([]string, error) {
	return runOrder(ctx, "OrderWorkflow", orderID, false)
}

// OrderWorkflowNext stands for the change on your branch: the same workflow
// with a fraud check before charging. Only the dev worker registers it, under
// the type name "OrderWorkflow", so a run routed to the dev queue executes
// this version while every other run executes OrderWorkflow.
func OrderWorkflowNext(ctx workflow.Context, orderID string) ([]string, error) {
	return runOrder(ctx, "OrderWorkflow (next)", orderID, true)
}

func runOrder(ctx workflow.Context, version, orderID string, checkFraud bool) ([]string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	steps := []string{fmt.Sprintf("%s ran on %s", version, workflow.GetInfo(ctx).TaskQueueName)}
	run := func(a any) error {
		var step string
		if err := workflow.ExecuteActivity(ctx, a, orderID).Get(ctx, &step); err != nil {
			return err
		}
		steps = append(steps, step)
		return nil
	}

	// Registered as "account.Fetch"; calling it by function resolves to that
	// name because this worker registers the activity.
	if err := run(FetchAccount); err != nil {
		return nil, err
	}
	if checkFraud {
		if err := run(CheckFraud); err != nil {
			return nil, err
		}
	}
	// Struct methods registered with a name prefix get no alias in the SDK, so
	// they must be called by their registered name.
	if err := run("payments.Charge"); err != nil {
		return nil, err
	}
	var shipment []string
	if err := workflow.ExecuteChildWorkflow(ctx, ShipmentWorkflow, orderID).Get(ctx, &shipment); err != nil {
		return nil, err
	}
	steps = append(steps, shipment...)
	if err := run(SendReceipt); err != nil {
		return nil, err
	}
	return steps, nil
}

// ShipmentWorkflow is a child workflow of OrderWorkflow.
func ShipmentWorkflow(ctx workflow.Context, orderID string) ([]string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	steps := []string{fmt.Sprintf("ShipmentWorkflow ran on %s", workflow.GetInfo(ctx).TaskQueueName)}
	var step string
	if err := workflow.ExecuteActivity(ctx, ReserveInventory, orderID).Get(ctx, &step); err != nil {
		return nil, err
	}
	return append(steps, step), nil
}

func ranOn(ctx context.Context) string {
	info := activity.GetInfo(ctx)
	return fmt.Sprintf("%s ran on %s", info.ActivityType.Name, info.TaskQueue)
}

func FetchAccount(ctx context.Context, orderID string) (string, error) { return ranOn(ctx), nil }

func SendReceipt(ctx context.Context, orderID string) (string, error) { return ranOn(ctx), nil }

func ReserveInventory(ctx context.Context, orderID string) (string, error) { return ranOn(ctx), nil }

// CheckFraud is new in OrderWorkflowNext and is not deployed to production.
func CheckFraud(ctx context.Context, orderID string) (string, error) { return ranOn(ctx), nil }

type Payments struct{}

func (p *Payments) Charge(ctx context.Context, orderID string) (string, error) {
	return ranOn(ctx), nil
}

type registeredWorkflow struct {
	fn   any
	opts workflow.RegisterOptions
}

type registeredActivity struct {
	fn   any
	opts activity.RegisterOptions
}

// The lists of what production and dev workers register. The Register and
// catalog functions all read them, so catalogs cannot drift from the
// registrations.
var (
	workflows = []registeredWorkflow{
		{OrderWorkflow, workflow.RegisterOptions{}},
		{ShipmentWorkflow, workflow.RegisterOptions{}},
	}
	activities = []registeredActivity{
		{FetchAccount, activity.RegisterOptions{Name: "account.Fetch"}},
		{&Payments{}, activity.RegisterOptions{Name: "payments."}},
		{SendReceipt, activity.RegisterOptions{}},
		{ReserveInventory, activity.RegisterOptions{}},
	}

	// What the dev worker runs instead of, or in addition to, production.
	devWorkflows = []registeredWorkflow{
		{OrderWorkflowNext, workflow.RegisterOptions{Name: "OrderWorkflow"}},
		{ShipmentWorkflow, workflow.RegisterOptions{}},
	}
	devOnlyActivities = []registeredActivity{
		{CheckFraud, activity.RegisterOptions{}},
	}
)

// Register registers the production workflows and activities on w.
func Register(w worker.Registry) {
	for _, wf := range workflows {
		w.RegisterWorkflowWithOptions(wf.fn, wf.opts)
	}
	for _, a := range activities {
		w.RegisterActivityWithOptions(a.fn, a.opts)
	}
}

// RegisterDev registers the dev worker's code on w: OrderWorkflowNext as
// "OrderWorkflow", and every activity including the dev-only ones.
func RegisterDev(w worker.Registry) {
	for _, wf := range devWorkflows {
		w.RegisterWorkflowWithOptions(wf.fn, wf.opts)
	}
	for _, a := range append(activities, devOnlyActivities...) {
		w.RegisterActivityWithOptions(a.fn, a.opts)
	}
}

// NewCatalog records every production type name with its production task
// queue. Workers use it to trust the home queue and check activity names;
// the starter uses it to reject route keys that are not registered names.
func NewCatalog() *routing.Catalog {
	c := routing.NewCatalog()
	for _, wf := range workflows {
		c.AddWorkflow(wf.fn, wf.opts, TaskQueue)
	}
	for _, a := range activities {
		c.AddActivity(a.fn, a.opts, TaskQueue)
	}
	return c
}

// NewDevCatalog is NewCatalog plus the dev-only activities, whose only home
// is devQueue.
func NewDevCatalog(devQueue string) *routing.Catalog {
	c := NewCatalog()
	for _, a := range devOnlyActivities {
		c.AddActivity(a.fn, a.opts, devQueue)
	}
	return c
}

// RouteDevOnlyActivities sends every dev-only activity to devQueue unless r
// already routes it. Production workers do not have these activities, so a
// run of OrderWorkflowNext that scheduled one on the production queue would
// retry it forever.
func RouteDevOnlyActivities(r *routing.Route, devQueue string) []string {
	var added []string
	for _, a := range devOnlyActivities {
		// AddActivity returns the registered type names, computed as the SDK does.
		for _, name := range routing.NewCatalog().AddActivity(a.fn, a.opts, devQueue) {
			if _, ok := r.Activities[name]; ok {
				continue
			}
			if r.Activities == nil {
				r.Activities = map[string]string{}
			}
			r.Activities[name] = devQueue
			added = append(added, name)
		}
	}
	return added
}
