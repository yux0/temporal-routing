package example

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/yux0/temporal-routing"
)

// runRouted executes OrderWorkflow as a worker with route in the start header
// would, starting on taskQueue, with the code that register registers.
func runRouted(t *testing.T, register func(worker.Registry), taskQueue string, route *routing.Route) []string {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	policy := routing.Policy{Enabled: true}
	env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{
		routing.NewInterceptor(routing.Options{Policy: policy, Catalog: NewCatalog()}),
	}})
	env.SetContextPropagators([]workflow.ContextPropagator{routing.NewContextPropagator(policy)})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: taskQueue})
	if route != nil {
		p, err := converter.GetDefaultDataConverter().ToPayload(*route)
		require.NoError(t, err)
		env.SetHeader(&commonpb.Header{Fields: map[string]*commonpb.Payload{routing.HeaderKey: p}})
	}
	register(env)

	env.ExecuteWorkflow("OrderWorkflow", "order-42")
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var steps []string
	require.NoError(t, env.GetWorkflowResult(&steps))
	return steps
}

func TestOrderWorkflowWithoutRoute(t *testing.T) {
	require.Equal(t, []string{
		"OrderWorkflow ran on task-routing",
		"account.Fetch ran on task-routing",
		"payments.Charge ran on task-routing",
		"ShipmentWorkflow ran on task-routing",
		"ReserveInventory ran on task-routing",
		"SendReceipt ran on task-routing",
	}, runRouted(t, Register, TaskQueue, nil))
}

// The workflow code runs on the developer's queue, one activity and the
// child's activity too; everything else stays in production.
func TestOrderWorkflowRouted(t *testing.T) {
	route := &routing.Route{
		Owner:             "dev",
		WorkflowTaskQueue: "dev-test",
		HomeTaskQueue:     TaskQueue, // the starter's client fills this in
		Activities:        map[string]string{"SendReceipt": "dev-test"},
		ChildWorkflows: map[string]routing.Route{
			"ShipmentWorkflow": {Activities: map[string]string{"ReserveInventory": "dev-test"}},
		},
	}
	require.Equal(t, []string{
		"OrderWorkflow ran on dev-test",
		"account.Fetch ran on task-routing",
		"payments.Charge ran on task-routing",
		"ShipmentWorkflow ran on task-routing",
		"ReserveInventory ran on dev-test",
		"SendReceipt ran on dev-test",
	}, runRouted(t, Register, "dev-test", route))
}

func TestCatalogKnowsRegisteredNames(t *testing.T) {
	c := NewCatalog()
	for _, name := range []string{"account.Fetch", "payments.Charge", "SendReceipt", "ReserveInventory"} {
		q, ok := c.ActivityHomeQueue(name)
		require.True(t, ok, name)
		require.Equal(t, TaskQueue, q)
	}
	require.Equal(t, []string{"account.Fetch"}, c.RegisteredNamesFor("FetchAccount"))
	require.Equal(t, []string{"payments.Charge"}, c.RegisteredNamesFor("Charge"))
}

// The dev worker runs OrderWorkflowNext under the type name OrderWorkflow; its
// new CheckFraud activity runs on the dev worker and everything else in
// production.
func TestOrderWorkflowNextOnDevWorker(t *testing.T) {
	route := &routing.Route{WorkflowTaskQueue: "dev-test", HomeTaskQueue: TaskQueue}
	require.Equal(t, []string{"CheckFraud"}, RouteDevOnlyActivities(route, "dev-test"))
	require.Equal(t, []string{
		"OrderWorkflow (next) ran on dev-test",
		"account.Fetch ran on task-routing",
		"CheckFraud ran on dev-test",
		"payments.Charge ran on task-routing",
		"ShipmentWorkflow ran on task-routing",
		"ReserveInventory ran on task-routing",
		"SendReceipt ran on task-routing",
	}, runRouted(t, RegisterDev, "dev-test", route))
}

func TestRouteDevOnlyActivitiesKeepsExplicitRoute(t *testing.T) {
	route := &routing.Route{Activities: map[string]string{"CheckFraud": "dev-other"}}
	require.Empty(t, RouteDevOnlyActivities(route, "dev-test"))
	require.Equal(t, "dev-other", route.Activities["CheckFraud"])
}

func TestDevCatalog(t *testing.T) {
	c := NewDevCatalog("dev-test")
	q, ok := c.ActivityHomeQueue("CheckFraud")
	require.True(t, ok)
	require.Equal(t, "dev-test", q)
	_, ok = NewCatalog().ActivityHomeQueue("CheckFraud")
	require.False(t, ok, "production does not know CheckFraud")
}
