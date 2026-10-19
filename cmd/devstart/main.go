// Command devstart starts a workflow run with a routing.Route, so parts of the
// run execute on a local dev worker.
//
// Connection settings come from the standard Temporal environment config
// (TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY, TLS settings, or a
// profile in the Temporal config file), the same as the temporal CLI.
//
// Examples:
//
//	# Run the workflow code locally; every activity still runs in production.
//	devstart -type OrderWorkflow -task-queue orders -local -input '{"orderID":"42"}'
//
//	# Workflow in production, one activity locally.
//	devstart -type OrderWorkflow -task-queue orders -act SendEmail=local
//
//	# Full route, including child workflows, from a JSON file.
//	devstart -type OrderWorkflow -task-queue orders -route route.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/user"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/interceptor"

	"github.com/yux0/temporal-routing"
)

type activityFlags map[string]string

func (a activityFlags) String() string { return fmt.Sprint(map[string]string(a)) }

func (a activityFlags) Set(v string) error {
	name, queue, ok := strings.Cut(v, "=")
	if !ok || name == "" || queue == "" {
		return fmt.Errorf("want ActivityType=queue, got %q", v)
	}
	a[name] = queue
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "devstart:", err)
		os.Exit(1)
	}
}

func run() error {
	owner := os.Getenv("USER")
	if u, err := user.Current(); owner == "" && err == nil {
		owner = u.Username
	}
	acts := activityFlags{}
	var (
		wfType    = flag.String("type", "", "workflow type to start (required)")
		taskQueue = flag.String("task-queue", "", "the workflow's normal task queue (required)")
		id        = flag.String("id", "", "workflow ID (default dev-<owner>-<type>-<time>)")
		input     = flag.String("input", "", "workflow argument as JSON")
		inputArgs = flag.Bool("input-args", false, "treat -input as a JSON array, one element per workflow argument")
		local     = flag.Bool("local", false, "run the workflow tasks on the dev queue")
		devQueue  = flag.String("dev-queue", "", "dev task queue (default dev-<owner>)")
		routeFile = flag.String("route", "", "JSON file with a full routing.Route; flags override its fields")
		wait      = flag.Bool("wait", false, "wait for the result and print it")
	)
	flag.StringVar(&owner, "owner", owner, "route owner")
	flag.Var(acts, "act", "ActivityType=queue override; queue \"local\" means the dev queue (repeatable)")
	flag.Parse()

	if *wfType == "" || *taskQueue == "" {
		flag.Usage()
		return errors.New("-type and -task-queue are required")
	}
	if *devQueue == "" {
		*devQueue = "dev-" + strings.ToLower(owner)
	}

	var route routing.Route
	if *routeFile != "" {
		b, err := os.ReadFile(*routeFile)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &route); err != nil {
			return fmt.Errorf("%s: %w", *routeFile, err)
		}
	}
	route.Owner = owner
	if *local {
		route.WorkflowTaskQueue = *devQueue
	}
	for name, q := range acts {
		if q == "local" {
			q = *devQueue
		}
		if route.Activities == nil {
			route.Activities = map[string]string{}
		}
		route.Activities[name] = q
	}

	var args []any
	if *input != "" {
		if *inputArgs {
			var raw []json.RawMessage
			if err := json.Unmarshal([]byte(*input), &raw); err != nil {
				return fmt.Errorf("-input with -input-args must be a JSON array: %w", err)
			}
			for _, r := range raw {
				args = append(args, r)
			}
		} else {
			if !json.Valid([]byte(*input)) {
				return errors.New("-input is not valid JSON")
			}
			args = append(args, json.RawMessage(*input))
		}
	}

	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		return fmt.Errorf("load Temporal client config: %w", err)
	}
	// This client only starts routed runs, so it opts in to sending routes.
	policy := routing.Policy{Enabled: true}
	opts.ContextPropagators = append(opts.ContextPropagators, routing.NewContextPropagator(policy))
	opts.Interceptors = append(opts.Interceptors, interceptor.ClientInterceptor(routing.NewInterceptor(routing.Options{Policy: policy})))
	c, err := client.Dial(opts)
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer c.Close()

	if *id == "" {
		*id = fmt.Sprintf("dev-%s-%s-%d", strings.ToLower(owner), *wfType, time.Now().Unix())
	}
	ctx := routing.WithRoute(context.Background(), route)
	wr, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: *id, TaskQueue: *taskQueue}, *wfType, args...)
	if err != nil {
		return err
	}
	routeJSON, _ := json.Marshal(route)
	fmt.Printf("started %s run %s\nroute %s\n", wr.GetID(), wr.GetRunID(), routeJSON)
	if !*wait {
		return nil
	}
	var result any
	if err := wr.Get(context.Background(), &result); err != nil {
		return err
	}
	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(out))
	return nil
}
