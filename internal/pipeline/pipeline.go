package pipeline

import (
	"context"
	"sync"

	"github.com/silasms/stream-data-pipeline/internal/domain"
)

type HandlerFn func(e *domain.Event) (*domain.Event, error)
type FilterFn func(e *domain.Event) bool

type Pipeline struct {
	workers        int
	inChan         chan *domain.Event
	outChan        chan *domain.Event
	transforms     []HandlerFn
	filters        []FilterFn
	processedCount int64
	droppedCount   int64
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewPipeline(workers, bufferSize int) *Pipeline {
	if workers <= 0 {
		workers = 4
	}
	if bufferSize <= 0 {
		bufferSize = 1000
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Pipeline{
		workers: workers,
		inChan:  make(chan *domain.Event, bufferSize),
		outChan: make(chan *domain.Event, bufferSize),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (p *Pipeline) AddTransform(fn HandlerFn) *Pipeline {
	p.transforms = append(p.transforms, fn)
	return p
}

func (p *Pipeline) AddFilter(fn FilterFn) *Pipeline {
	p.filters = append(p.filters, fn)
	return p
}
