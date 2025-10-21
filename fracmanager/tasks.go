package fracmanager

import (
	"context"
	"sync"
)

// task represents a cancellable background task with synchronization
// Used for managing long-running operations like offloading fractions.
// Lifecycle: Created via Tasks.Run(), cancelled via Tasks.Cancel(), cleaned up on completion.
type task struct {
	wg     sync.WaitGroup     // Synchronizes task completion
	ctx    context.Context    // Context for cancellation
	cancel context.CancelFunc // Function to cancel the task
}

// TaskManager manages a collection of running background tasks
// Provides safe concurrent access to task tracking and cancellation.
type TaskManager struct {
	mu      sync.Mutex
	running map[string]*task // Map of task ID to task instance
}

func NewTaskManager() *TaskManager {
	return &TaskManager{
		running: make(map[string]*task),
	}
}

// Run starts a new background task with the given ID and context.
// The task will be automatically removed when completed.
func (t *TaskManager) Run(id string, ctx context.Context, action func(ctx context.Context)) *task {
	task := &task{}
	task.ctx, task.cancel = context.WithCancel(ctx)

	t.mu.Lock()
	t.running[id] = task
	t.mu.Unlock()

	task.wg.Add(1)
	go func() {
		defer func() {
			t.mu.Lock()
			delete(t.running, id)
			t.mu.Unlock()

			task.wg.Done()
		}()

		action(task.ctx)
	}()

	return task
}

// Cancel cancels and waits for completion of a task by ID
// Returns immediately if task with given ID doesn't exist.
func (t *TaskManager) Cancel(id string) {
	t.mu.Lock()
	task, ok := t.running[id]
	t.mu.Unlock()

	if ok {
		task.cancel()
		task.wg.Wait()
	}
}
