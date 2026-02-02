package queuer

import (
	"context"
	"database/sql"
	"log"
	"notifyx/models"
	"sync"
)

// Notifier interface defines the contract for all notification types
type Notifier interface {
	Process(ctx context.Context, task *models.Task) error
}

type TaskQueue struct {
	tasks           chan *models.Task
	retryQueue      chan *models.Task
	deadLetterQueue chan *models.Task
	workerCount     int
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	db              *sql.DB
	notifiers       map[models.TaskType]Notifier
}

func InitTaskQueue(db *sql.DB, workerCount, bufferSize int, notifiers map[models.TaskType]Notifier) *TaskQueue {
	ctx, cancel := context.WithCancel(context.Background())
	return &TaskQueue{
		tasks:           make(chan *models.Task, bufferSize),
		retryQueue:      make(chan *models.Task, bufferSize/2),
		deadLetterQueue: make(chan *models.Task, bufferSize/4),
		workerCount:     workerCount,
		ctx:             ctx,
		cancel:          cancel,
		db:              db,
		notifiers:       notifiers,
	}
}

func (tq *TaskQueue) Start() {
	log.Printf("Starting TaskQueue with workers: %d", tq.workerCount)
	for i := 0; i < tq.workerCount; i++ {
		tq.wg.Add(1)
		go tq.worker(i)
	}
}

func (tq *TaskQueue) worker(id int) {
	defer tq.wg.Done()
	for {
		select {
		case task, ok := <-tq.tasks:
			if !ok {
				log.Printf("Worker %d: task channel closed", id)
				return
			}
			tq.processTask(id, task)

		case <-tq.ctx.Done():
			log.Printf("Worker %d: shutdown, Ctx reached", id)
			return
		}
	}
}

func (tq *TaskQueue) processTask(id int, task *models.Task) {
	// Get the appropriate notifier based on task type
	notifier, exists := tq.notifiers[task.Type]
	if !exists {
		log.Printf("Worker %d: no notifier found for task type %s", id, task.Type)
		tq.deadLetterQueue <- task
		return
	}

	// Process the task using the notifier's Process method
	err := notifier.Process(tq.ctx, task)
	if err != nil {
		log.Printf("Worker %d: failed to process task %s: %v", id, task.ID, err)

		// Retry logic
		if task.RetryCount < task.MaxRetries {
			task.RetryCount++
			tq.retryQueue <- task
		} else {
			tq.deadLetterQueue <- task
		}
		return
	}

	log.Printf("Worker %d: successfully processed task %s", id, task.ID)
}

func (tq *TaskQueue) Enqueue(task *models.Task) {
	tq.tasks <- task
}

// Stop gracefully shuts down the task queue
func (tq *TaskQueue) Stop() {
	log.Println("Stopping TaskQueue...")

	// Signal all workers to stop
	tq.cancel()

	// Close task channels
	close(tq.tasks)
	close(tq.retryQueue)
	close(tq.deadLetterQueue)

	// Wait for all workers to finish
	tq.wg.Wait()

	log.Println("TaskQueue stopped successfully")
}
