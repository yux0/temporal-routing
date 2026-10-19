# temporal-routing

Per-run task-queue routing for Temporal Go workers. For a single workflow run you choose:

- where its **workflow tasks** go: your laptop or production;
- where each **activity** goes, by registered type name;
- the same choices for each **child workflow**, as a nested route.

Everything you don't override goes where production would have sent it.

Typical use: run new workflow code under a debugger on your laptop while the activities it calls still run on production workers, inside the production network.

```go
ctx := routing.WithRoute(ctx, routing.Route{
    Owner:             "yu",
    WorkflowTaskQueue: "dev-yu",                                   // workflow code runs on my laptop
    Activities:        map[string]string{"SendEmail": "dev-yu"},  // and so does this activity
    ChildWorkflows: map[string]routing.Route{                     // child Settle: local, with its own overrides
        "Settle": {WorkflowTaskQueue: "dev-yu", Activities: map[string]string{"ChargeCard": "dev-yu"}},
    },
})
run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: "orders"}, OrderWorkflow, in)
```

## How it works

1. The **client interceptor** checks the route against the `Policy` and the `Catalog`. It records `HomeTaskQueue` (here `orders`) and moves the run to `WorkflowTaskQueue`.
2. The **context propagator** writes the route into the run's start header (`x-routing`). The route is part of history, so replay is deterministic.
3. On the worker, the **workflow interceptor** sets the task queue of each activity and child workflow:

| Activity | Queue |
|---|---|
| listed in `Route.Activities` | the override queue |
| workflow code set an explicit `TaskQueue` | that queue |
| otherwise | the run's `HomeTaskQueue` (where production would have sent it), else the catalog home queue |

| Child workflow | Queue | Route |
|---|---|---|
| listed in `Route.ChildWorkflows` | its `WorkflowTaskQueue`, else its home queue | its own nested route |
| not listed | its home queue (explicit queue, else the parent's home, else the catalog) | none, so it runs exactly as in production |

Runs without a route that aren't on a dev queue are not touched.

Changing routing never breaks replay: the SDK compares activity ID and type on replay, not the task queue. `TestIntegrationRoutedHistoryReplays` replays a routed history with the same rules, a changed catalog, routing disabled, and no routing layer at all.

## Wiring

Register the interceptor and propagator on the client. Workers created from that client inherit the interceptor, because it is also a `WorkerInterceptor`. Registering it on the worker as well is harmless, since the layer is safe to stack.

```go
catalog := routing.NewCatalog()
policy, _ := routing.PolicyFromEnv()           // production: overrides off unless ROUTING_OVERRIDES_ENABLED=true

c, _ := client.Dial(client.Options{
    ContextPropagators: []workflow.ContextPropagator{routing.NewContextPropagator(policy)},
    Interceptors:       []interceptor.ClientInterceptor{routing.NewInterceptor(routing.Options{Policy: policy, Catalog: catalog})},
})
w := worker.New(c, "orders", worker.Options{})
catalog.RegisterWorkflow(w, OrderWorkflow, workflow.RegisterOptions{}, "orders")
catalog.RegisterActivity(w, FetchAccount, activity.RegisterOptions{Name: "acct.fetch"}, "orders")
catalog.RegisterActivity(w, &Payments{}, activity.RegisterOptions{Name: "pay."}, "orders")
```

**Every worker that runs these workflows needs the propagator and interceptor: production and dev.** If a production worker lacks them, a run it executes ignores activity overrides that point at your laptop.

### Dev worker

Same code, polling your dev queue, with overrides enabled:

```go
policy := routing.Policy{Enabled: true, OverrideScheduleToStartTimeout: 2 * time.Minute}
// ...same client setup...
w := worker.New(c, "dev-yu", worker.Options{})
// register the workflows and/or activities you are working on
```

Set `TEMPORAL_DEBUG=1` when running it under Delve. This turns off the SDK's 1-second deadlock detector, so breakpoints don't fail workflow tasks.

### Starting routed runs: `cmd/devstart`

Connection settings come from the standard Temporal env config (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`, TLS settings, profiles), the same as the `temporal` CLI.

```sh
go run ./cmd/devstart -type OrderWorkflow -task-queue orders -local -input '{"orderID":"42"}'
go run ./cmd/devstart -type OrderWorkflow -task-queue orders -act SendEmail=local
go run ./cmd/devstart -type OrderWorkflow -task-queue orders -route route.json -wait
```

`local` means `dev-$USER`. `-route` takes a JSON `Route`, for nested child routes. `devstart` has no catalog, so it can't check activity names. Add a catalog in your own starter if you want that check.

## Safety

This layer can pull production activity inputs to a laptop, so it is locked down by default:

- `Policy{}` (the zero value) honors nothing. Workers need `Enabled: true`, e.g. `ROUTING_OVERRIDES_ENABLED=true`.
- Override queues must match `^dev-[a-z0-9][a-z0-9-]*$` (`Policy.AllowedQueue`). Clients reject other queues; workers drop them and log a warning.
- `Policy.AllowedOwners` restricts `Route.Owner`. The owner is self-declared, so treat it as an audit label rather than authentication.
- `HomeTaskQueue` also arrives in a header. Workers only use it if it is a home queue in the catalog or listed in `Policy.AllowedHomeQueues` (`ROUTING_HOME_QUEUES`).
- **Only the start header counts.** The SDK also extracts signal, query and update headers into handler contexts. Without a guard, an update sent with a routed context would reroute that handler's activities (`TestIntegrationUpdateHeaderCannotReroute`, verified by disabling the guard).
- `OverrideScheduleToStartTimeout` makes a run fail fast instead of hanging when your dev worker isn't running.
- Activities that stay on production really execute. Route test runs only through workflows whose production side effects are acceptable, or point the run at a staging namespace.

## Activity names

Route keys are the **registered type name**: the string the server schedules. The SDK resolves names like this:

| Registered as | Called as | Scheduled type |
|---|---|---|
| `RegisterActivity(SendEmail)` | `SendEmail` | `SendEmail` |
| `RegisterActivityWithOptions(FetchAccount, {Name: "acct.fetch"})` | `FetchAccount` | `acct.fetch`, **only if the worker running the workflow registered it**; otherwise `FetchAccount` |
| same | `"acct.fetch"` | `acct.fetch` |
| `RegisterActivity(&Payments{})` | `p.Charge` | `Charge` |
| `RegisterActivityWithOptions(&Payments{}, {Name: "pay."})` | `p.Charge` | `Charge`. **The SDK records no alias for struct prefixes** |
| same | `"pay.Charge"` | `pay.Charge` |

`Catalog.RegisterActivity` computes names the same way the SDK does (`TestCatalogMatchesSDKResolution` checks this against the SDK for each row). With a catalog:

- the client rejects a route that uses a Go name where a custom name is registered, and names the right one (`activity "FetchAccount" is not in the catalog (it is registered as acct.fetch; ...)`);
- the worker logs an error when a workflow schedules a Go name that was registered under a custom name. The type is never rewritten, because that would break replay of in-flight runs.

Related SDK limitation: `WorkflowReplayer` has no `RegisterActivity`, so a workflow that calls a custom-named activity by function reference **fails replay** (`TestIntegrationReplayerCannotResolveActivityAlias`). If you replay-test such workflows, call those activities by their registered string name.

## Example

[`example/`](example) is a runnable order workflow with a child workflow. It has a production/dev worker (`example/worker`, `-dev` for your machine) and a starter (`example/starter`) that prints which task queue ran each step. See [example/README.md](example/README.md).

## Tests

```sh
go test -short ./...        # unit tests (testsuite)
go test ./...               # plus integration tests on a Temporal dev server (downloaded on first run)
```

The integration tests run production workers (`orders`, `children`) and a "laptop" worker (`dev-yu`) with distinct identities. They then check, from the recorded history, which queue each task was scheduled on and which worker ran it.
