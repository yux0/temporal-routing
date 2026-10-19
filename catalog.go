package routing

import (
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Catalog records the registered type name and home task queue of every
// activity and workflow. Build the same catalog in every worker binary that
// runs the workflows, including workers that do not host the activities, so
// routed runs can send each task back to its home queue.
type Catalog struct {
	mu sync.RWMutex
	// activities and workflows map registered type name → home task queue.
	activities map[string]string
	workflows  map[string]string
	// goNames maps a Go function or method short name to the registered type
	// names it was registered under, when those differ.
	goNames map[string][]string
	// queues is the set of every home queue, so lookups from workflow code do
	// not iterate a map.
	queues map[string]struct{}
}

// NewCatalog returns an empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		activities: map[string]string{},
		workflows:  map[string]string{},
		goNames:    map[string][]string{},
		queues:     map[string]struct{}{},
	}
}

// RegisterActivity registers af on w with opts and records the resulting type
// names with homeQueue. af may be a function or a pointer to a struct whose
// exported methods are activities, exactly as for RegisterActivityWithOptions.
func (c *Catalog) RegisterActivity(w worker.ActivityRegistry, af any, opts activity.RegisterOptions, homeQueue string) {
	w.RegisterActivityWithOptions(af, opts)
	c.AddActivity(af, opts, homeQueue)
}

// AddActivity records af like RegisterActivity without registering it. Use it
// in workflow workers that call activities hosted by another worker.
func (c *Catalog) AddActivity(af any, opts activity.RegisterOptions, homeQueue string) []string {
	if homeQueue == "" {
		panic("routing: AddActivity requires a home task queue")
	}
	names := activityNames(af, opts)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queues[homeQueue] = struct{}{}
	for _, n := range names {
		c.activities[n.registered] = homeQueue
		if n.goName != n.registered && !slices.Contains(c.goNames[n.goName], n.registered) {
			c.goNames[n.goName] = append(c.goNames[n.goName], n.registered)
		}
	}
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = n.registered
	}
	return out
}

// RegisterWorkflow registers wf on w with opts and records its type name with
// homeQueue, so unconfigured children of a routed run go back to homeQueue.
func (c *Catalog) RegisterWorkflow(w worker.WorkflowRegistry, wf any, opts workflow.RegisterOptions, homeQueue string) {
	w.RegisterWorkflowWithOptions(wf, opts)
	c.AddWorkflow(wf, opts, homeQueue)
}

// AddWorkflow records wf like RegisterWorkflow without registering it.
func (c *Catalog) AddWorkflow(wf any, opts workflow.RegisterOptions, homeQueue string) string {
	if homeQueue == "" {
		panic("routing: AddWorkflow requires a home task queue")
	}
	goName := shortFuncName(wf)
	name := goName
	if opts.Name != "" {
		name = opts.Name
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queues[homeQueue] = struct{}{}
	c.workflows[name] = homeQueue
	return name
}

// ActivityHomeQueue returns the home task queue of a registered activity type.
func (c *Catalog) ActivityHomeQueue(name string) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	q, ok := c.activities[name]
	return q, ok
}

// WorkflowHomeQueue returns the home task queue of a registered workflow type.
func (c *Catalog) WorkflowHomeQueue(name string) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	q, ok := c.workflows[name]
	return q, ok
}

// KnowsQueue reports whether q is the home queue of any recorded activity or
// workflow.
func (c *Catalog) KnowsQueue(q string) bool {
	if c == nil || q == "" {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.queues[q]
	return ok
}

// RegisteredNamesFor returns the registered type names a Go function or method
// name was registered under, when they differ from the Go name. A workflow
// that schedules activity type goName while this returns names is calling the
// activity by function without the custom name being resolvable.
func (c *Catalog) RegisteredNamesFor(goName string) []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Clone(c.goNames[goName])
}

// Validate reports route keys that are not registered activity or workflow
// types in the catalog, at every level of the route.
func (c *Catalog) Validate(r Route) error {
	if c == nil {
		return nil
	}
	var problems []string
	c.mu.RLock()
	c.validateInto(r, "", &problems)
	c.mu.RUnlock()
	if len(problems) == 0 {
		return nil
	}
	slices.Sort(problems)
	return fmt.Errorf("routing: route rejected: %s", strings.Join(problems, "; "))
}

// validateInto must be called with c.mu held for reading.
func (c *Catalog) validateInto(r Route, path string, problems *[]string) {
	for name := range r.Activities {
		if _, ok := c.activities[name]; ok {
			continue
		}
		msg := fmt.Sprintf("%sactivity %q is not in the catalog", path, name)
		if alts := c.goNames[name]; len(alts) > 0 {
			msg += fmt.Sprintf(" (it is registered as %s; use the registered name)", strings.Join(alts, ", "))
		}
		*problems = append(*problems, msg)
	}
	for name, child := range r.ChildWorkflows {
		if _, ok := c.workflows[name]; !ok {
			*problems = append(*problems, fmt.Sprintf("%schild workflow %q is not in the catalog", path, name))
		}
		c.validateInto(child, path+"child "+name+": ", problems)
	}
}

type activityName struct {
	goName     string
	registered string
}

// activityNames mirrors how the SDK names activities at registration: a
// function is named opts.Name, or its short Go name; each exported method of a
// struct pointer is named opts.Name + method name.
func activityNames(af any, opts activity.RegisterOptions) []activityName {
	t := reflect.TypeOf(af)
	if t == nil {
		panic("routing: nil activity")
	}
	if t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct {
		var names []activityName
		for i := 0; i < t.NumMethod(); i++ {
			m := t.Method(i)
			if m.PkgPath != "" {
				continue
			}
			if opts.SkipInvalidStructFunctions && !validActivitySignature(m.Type) {
				continue
			}
			names = append(names, activityName{goName: m.Name, registered: opts.Name + m.Name})
		}
		return names
	}
	if t.Kind() != reflect.Func {
		panic(fmt.Sprintf("routing: activity must be a function or struct pointer, got %v", t))
	}
	goName := shortFuncName(af)
	registered := goName
	if opts.Name != "" {
		registered = opts.Name
	}
	return []activityName{{goName: goName, registered: registered}}
}

func validActivitySignature(t reflect.Type) bool {
	errType := reflect.TypeOf((*error)(nil)).Elem()
	n := t.NumOut()
	return (n == 1 || n == 2) && t.Out(n-1) == errType
}

// shortFuncName matches the SDK's getFunctionName: the last dot-separated
// element of the runtime name, without the "-fm" method value suffix.
func shortFuncName(fn any) string {
	if s, ok := fn.(string); ok {
		return s
	}
	full := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	elems := strings.Split(full, ".")
	return strings.TrimSuffix(elems[len(elems)-1], "-fm")
}
