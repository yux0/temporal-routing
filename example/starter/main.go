package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/interceptor"

	"github.com/yux0/temporal-routing"
	"github.com/yux0/temporal-routing/example"
)

// overrides collects repeated Name=queue flags. The queue "local" means the
// developer's dev queue.
type overrides map[string]string

func (o overrides) String() string { return fmt.Sprint(map[string]string(o)) }

func (o overrides) Set(v string) error {
	name, queue, ok := strings.Cut(v, "=")
	if !ok || name == "" || queue == "" {
		return fmt.Errorf("want Name=queue, got %q", v)
	}
	o[name] = queue
	return nil
}

// resolveLocal replaces the queue name "local" with the dev queue at every
// level of r, so route files work for any developer.
func resolveLocal(r *routing.Route, resolve func(string) string) {
	r.WorkflowTaskQueue = resolve(r.WorkflowTaskQueue)
	for name, q := range r.Activities {
		r.Activities[name] = resolve(q)
	}
	for name, child := range r.ChildWorkflows {
		resolveLocal(&child, resolve)
		r.ChildWorkflows[name] = child
	}
}

func main() {
	acts, children := overrides{}, overrides{}
	owner := strings.ToLower(os.Getenv("USER"))
	local := flag.Bool("local", false, "run OrderWorkflow's workflow tasks on the dev queue")
	routeFile := flag.String("route", "", "JSON file with a full routing.Route; the other flags are applied on top")
	orderID := flag.String("order", "order-42", "order ID")
	flag.StringVar(&owner, "owner", owner, "route owner; the dev queue is dev-<owner>")
	flag.Var(acts, "act", "ActivityType=queue override for OrderWorkflow (repeatable)")
	flag.Var(children, "child", "ChildWorkflowType=queue to run that child's workflow tasks elsewhere (repeatable)")
	flag.Parse()

	devQueue := "dev-" + owner
	resolve := func(q string) string {
		if q == "local" {
			return devQueue
		}
		return q
	}

	var route routing.Route
	if *routeFile != "" {
		b, err := os.ReadFile(*routeFile)
		if err != nil {
			log.Fatalln("Unable to read route file", err)
		}
		if err := json.Unmarshal(b, &route); err != nil {
			log.Fatalln("Invalid route file", err)
		}
	}
	route.Owner = owner
	if *local {
		route.WorkflowTaskQueue = devQueue
	}
	for name, q := range acts {
		if route.Activities == nil {
			route.Activities = map[string]string{}
		}
		route.Activities[name] = resolve(q)
	}
	for name, q := range children {
		if route.ChildWorkflows == nil {
			route.ChildWorkflows = map[string]routing.Route{}
		}
		child := route.ChildWorkflows[name]
		child.WorkflowTaskQueue = q
		route.ChildWorkflows[name] = child
	}
	resolveLocal(&route, resolve)
	// A run on the dev queue executes OrderWorkflowNext, whose new activities
	// exist only on the dev worker.
	if route.WorkflowTaskQueue == devQueue {
		if added := example.RouteDevOnlyActivities(&route, devQueue); len(added) > 0 {
			log.Println("Routing dev-only activities to", devQueue, added)
		}
	}

	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatalln("Unable to load client options", err)
	}
	// This client starts routed runs, so it opts in to sending routes. The
	// catalog makes it reject route keys that are not registered type names.
	policy := routing.Policy{Enabled: true}
	opts.ContextPropagators = append(opts.ContextPropagators, routing.NewContextPropagator(policy))
	opts.Interceptors = append(opts.Interceptors,
		interceptor.ClientInterceptor(routing.NewInterceptor(routing.Options{Policy: policy, Catalog: example.NewDevCatalog(devQueue)})))

	// The client is a heavyweight object that should be created once per process.
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	workflowOptions := client.StartWorkflowOptions{
		ID:        fmt.Sprintf("task-routing-%s-%d", owner, time.Now().UnixNano()),
		TaskQueue: example.TaskQueue,
	}
	ctx := routing.WithRoute(context.Background(), route)
	we, err := c.ExecuteWorkflow(ctx, workflowOptions, example.OrderWorkflow, *orderID)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())

	var steps []string
	if err := we.Get(context.Background(), &steps); err != nil {
		log.Fatalln("Unable to get workflow result", err)
	}
	for _, s := range steps {
		log.Println(s)
	}
}
