### Example: run parts of a workflow on your machine

This sample lets you choose, for a single Workflow Execution, which Task Queue its Workflow Tasks go to and which Task Queue each Activity goes to, by registered Activity type. The same choices apply to each Child Workflow, through a nested route. Anything you don't override runs where it would have run anyway.

Typical use: debug or try out new Workflow code on your laptop while the Activities it calls keep running on production Workers, which can reach production services. Or the other way round: run one new Activity locally inside an otherwise normal execution.

```go
ctx := routing.WithRoute(ctx, routing.Route{
	Owner:             "yu",
	WorkflowTaskQueue: "dev-yu",                                      // Workflow code runs on my laptop
	Activities:        map[string]string{"SendReceipt": "dev-yu"},   // and so does this Activity
	ChildWorkflows: map[string]routing.Route{                        // child: its own route
		"ShipmentWorkflow": {Activities: map[string]string{"ReserveInventory": "dev-yu"}},
	},
})
we, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: "task-routing"}, "OrderWorkflow", "order-42")
```

### Two versions of the Workflow

The example plays out a typical change: you are adding a fraud check to `OrderWorkflow` and want to try it against production before merging.

- **Production Workers** register `OrderWorkflow`, the deployed version.
- **Your dev Worker** registers `OrderWorkflowNext` under the same type name, `OrderWorkflow`. It is the same Workflow plus a new `CheckFraud` Activity before charging. `CheckFraud` exists only on your dev Worker.

So an execution routed to your dev queue runs your version, and every other execution runs the production one. Both versions are in [order.go](order.go); `Register` and `RegisterDev` choose which one a Worker gets.

Production Workers don't have `CheckFraud`. If your version scheduled it on the production queue, it would be retried forever. So whenever the Workflow runs on your dev queue, the starter routes your dev-only Activities to your machine automatically (`RouteDevOnlyActivities`).

### Steps to run this sample

1) Run a Temporal service, e.g. `temporal server start-dev`. Run the commands below from the repository root.
2) Run the "production" Worker. It honors routes only when overrides are enabled:
```
ROUTING_OVERRIDES_ENABLED=true go run ./example/worker
```
3) In another terminal, run your dev Worker. It polls `dev-$USER`:
```
go run ./example/worker -dev
```
4) Start executions. Each one prints which Task Queue ran each step:
```
go run ./example/starter                           # production version, all in production
go run ./example/starter -local                    # your version on your machine
go run ./example/starter -act SendReceipt=local    # production version, one Activity on your machine
go run ./example/starter -local -act SendReceipt=local -child ShipmentWorkflow=local
go run ./example/starter -route example/route.json
```
`local` means `dev-$USER`. Without a route, the production version runs entirely in production:
```
OrderWorkflow ran on task-routing
account.Fetch ran on task-routing
payments.Charge ran on task-routing
ShipmentWorkflow ran on task-routing
ReserveInventory ran on task-routing
SendReceipt ran on task-routing
```
With `-local`, your version runs on your machine. Its new `CheckFraud` Activity runs there too, and every existing Activity still runs in production:
```
Routing dev-only activities to dev-yu [CheckFraud]
OrderWorkflow (next) ran on dev-yu
account.Fetch ran on task-routing
CheckFraud ran on dev-yu
payments.Charge ran on task-routing
ShipmentWorkflow ran on task-routing
ReserveInventory ran on task-routing
SendReceipt ran on task-routing
```
A route file can express things the flags can't, such as an override for a Child Workflow's Activity. Use `"local"` for your dev queue anywhere in the file. [`route.json`](route.json) runs your version and `SendReceipt` locally, and keeps `ShipmentWorkflow` in production while sending its `ReserveInventory` Activity to your machine:
```json
{
  "workflowTaskQueue": "local",
  "activities": { "SendReceipt": "local" },
  "childWorkflows": {
    "ShipmentWorkflow": { "activities": { "ReserveInventory": "local" } }
  }
}
```
```
Routing dev-only activities to dev-yu [CheckFraud]
OrderWorkflow (next) ran on dev-yu
account.Fetch ran on task-routing
CheckFraud ran on dev-yu
payments.Charge ran on task-routing
ShipmentWorkflow ran on task-routing
ReserveInventory ran on dev-yu
SendReceipt ran on dev-yu
```
Both programs read connection settings from the standard environment configuration (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, TLS settings, profiles). To debug the dev Worker with Delve, set `TEMPORAL_DEBUG=1`. This turns off the SDK's deadlock detector, so breakpoints don't fail Workflow Tasks.

### How it works

The [`routing`](..) package has three parts:

- **Context propagator.** Writes the route into the execution's start header. The route is recorded in history, so replay is deterministic.
- **Client interceptor.** Checks the route, records the Task Queue the execution would have used (`HomeTaskQueue`), and starts the execution on `WorkflowTaskQueue`.
- **Workflow outbound interceptor.** Sets the Task Queue of each Activity and Child Workflow:
  1. the route's override, if there is one;
  2. otherwise a Task Queue the Workflow code set explicitly;
  3. otherwise the home Task Queue, i.e. where it would have gone without the route.

  A Child Workflow listed in `ChildWorkflows` starts with its own route. Any other Child Workflow starts on its home Task Queue with no route.

Executions without a route are not changed. Changing routing never causes non-determinism, because replay compares Activity IDs and types, not Task Queues.

Register the interceptor and propagator on the client. Workers created from that client inherit the interceptor, and registering it twice is harmless. **Every Worker that runs these Workflows needs both**, production and dev alike.

### Safety

This mechanism can send production Activity inputs to a laptop, so it is locked down by default:

- A zero `Policy` honors nothing. Production Workers need `ROUTING_OVERRIDES_ENABLED=true`.
- Override Task Queues must match `^dev-[a-z0-9][a-z0-9-]*$` (`Policy.AllowedQueue`).
- `Policy.AllowedOwners` (`ROUTING_ALLOWED_OWNERS`) restricts `Route.Owner`. The owner is self-declared: treat it as an audit label, not authentication.
- `HomeTaskQueue` arrives in a header, so Workers use it only if it is a home queue in the catalog or listed in `ROUTING_HOME_QUEUES`.
- Only the start header counts. Without that rule, an Update or Signal sent with a routed context could reroute an execution that is already running.
- `OverrideScheduleToStartTimeout` makes an execution fail fast when the dev Worker isn't running.
- Activities that stay on production really run. Use executions whose side effects are acceptable, or a staging Namespace.

### Activity names

Route keys are **registered type names**, i.e. what the Server schedules. `NewCatalog` in [order.go](./order.go) is built from the same list the Worker registers. The starter uses it to reject unregistered names:

```
route rejected: activity "FetchAccount" is not in the catalog (it is registered as account.Fetch; use the registered name)
```

Be aware of how the Go SDK resolves names:

- A function registered with `Name` resolves to that name only if the Worker running the Workflow registered it.
- Struct methods registered with a `Name` prefix (`payments.`) get no alias. Call them by their registered name (`"payments.Charge"`).
- `WorkflowReplayer` cannot register Activities. A Workflow that calls a custom-named Activity by function reference therefore fails replay.

### Tests

```
go test ./example/
```
