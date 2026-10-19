package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/envconfig"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	"github.com/yux0/temporal-routing"
	"github.com/yux0/temporal-routing/example"
)

func main() {
	dev := flag.Bool("dev", false, "run as the developer's worker on the dev-$USER task queue")
	flag.Parse()

	queue := example.TaskQueue
	identity := "prod"
	register := example.Register
	catalog := example.NewCatalog()
	var policy routing.Policy
	if *dev {
		// The dev worker runs your branch: OrderWorkflowNext and CheckFraud.
		queue = "dev-" + strings.ToLower(os.Getenv("USER"))
		identity = queue
		register = example.RegisterDev
		catalog = example.NewDevCatalog(queue)
		policy = routing.Policy{Enabled: true, OverrideScheduleToStartTimeout: time.Minute}
	} else {
		// Production honors routes only when ROUTING_OVERRIDES_ENABLED=true.
		var err error
		if policy, err = routing.PolicyFromEnv(); err != nil {
			log.Fatalln("Invalid routing policy", err)
		}
	}

	opts, err := envconfig.LoadDefaultClientOptions()
	if err != nil {
		log.Fatalln("Unable to load client options", err)
	}
	// Workers created from this client inherit the routing interceptor.
	opts.ContextPropagators = append(opts.ContextPropagators, routing.NewContextPropagator(policy))
	opts.Interceptors = append(opts.Interceptors,
		interceptor.ClientInterceptor(routing.NewInterceptor(routing.Options{Policy: policy, Catalog: catalog})))

	// The client and worker are heavyweight objects that should be created once per process.
	c, err := client.Dial(opts)
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	hostname, _ := os.Hostname()
	w := worker.New(c, queue, worker.Options{Identity: fmt.Sprintf("%s@%s", identity, hostname)})
	register(w)

	log.Printf("Polling task queue %q (route overrides enabled: %v)", queue, policy.Enabled)
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
