package registry

import (
	"fmt"
	"sync"

	"openetl-go/internal/etl/core"
)

var (
	mu                sync.RWMutex
	sources           = map[string]core.Source{}
	sinks             = map[string]core.Sink{}
	transforms        = map[string]core.Transform{}
	sourceBuilders    = map[string]func(config map[string]any) (core.Source, error){}
	sinkBuilders      = map[string]func(config map[string]any) (core.Sink, error){}
	transformBuilders = map[string]func(config map[string]any) (core.Transform, error){}
)

func RegisterSource(name string, builder func(config map[string]any) (core.Source, error)) {
	mu.Lock()
	defer mu.Unlock()
	sourceBuilders[name] = builder
}

func RegisterSink(name string, builder func(config map[string]any) (core.Sink, error)) {
	mu.Lock()
	defer mu.Unlock()
	sinkBuilders[name] = builder
}

func RegisterTransform(name string, builder func(config map[string]any) (core.Transform, error)) {
	mu.Lock()
	defer mu.Unlock()
	transformBuilders[name] = builder
}

func BuildSource(sourceType string, config map[string]any) (core.Source, error) {
	mu.RLock()
	defer mu.RUnlock()
	builder, ok := sourceBuilders[sourceType]
	if !ok {
		return nil, fmt.Errorf("unknown source type: %s", sourceType)
	}
	return builder(config)
}

func HasSource(sourceType string) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, ok := sourceBuilders[sourceType]
	return ok
}

func BuildSink(sinkType string, config map[string]any) (core.Sink, error) {
	mu.RLock()
	defer mu.RUnlock()
	builder, ok := sinkBuilders[sinkType]
	if !ok {
		return nil, fmt.Errorf("unknown sink type: %s", sinkType)
	}
	return builder(config)
}

func HasSink(sinkType string) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, ok := sinkBuilders[sinkType]
	return ok
}

func BuildTransform(transformType string, config map[string]any) (core.Transform, error) {
	mu.RLock()
	defer mu.RUnlock()
	builder, ok := transformBuilders[transformType]
	if !ok {
		return nil, fmt.Errorf("unknown transform type: %s", transformType)
	}
	return builder(config)
}

func HasTransform(transformType string) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, ok := transformBuilders[transformType]
	return ok
}

func SourceTypes() []string {
	mu.RLock()
	defer mu.RUnlock()
	var names []string
	for name := range sourceBuilders {
		names = append(names, name)
	}
	return names
}

func SinkTypes() []string {
	mu.RLock()
	defer mu.RUnlock()
	var names []string
	for name := range sinkBuilders {
		names = append(names, name)
	}
	return names
}

func TransformTypes() []string {
	mu.RLock()
	defer mu.RUnlock()
	var names []string
	for name := range transformBuilders {
		names = append(names, name)
	}
	return names
}
