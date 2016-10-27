package domain

import (
	"sync"
	"sync/atomic"

	"straas.io/bridger/logger"

	"github.com/tevino/abool"
)

type eventPubProxy func(topic string, args ...interface{}) error

// Worker is abstraction of task handler
type Worker struct {
	IsTerminated *abool.AtomicBool
	Machine      Machinery
	Name         string
	ChTask       chan Task
	// use for test
	NumOfDone int32

	chQuit     chan bool
	isBusy     int32
	mutex      *sync.Mutex
	onHandTask Task
	pub        eventPubProxy
}

// NewWorker creates a new Worker
func NewWorker(
	name string,
	machine Machinery,
	em eventPubProxy,
) *Worker {

	return &Worker{
		Name:         name,
		ChTask:       make(chan Task),
		chQuit:       make(chan bool),
		Machine:      machine,
		IsTerminated: abool.New(),
		pub:          em,
		mutex:        &sync.Mutex{},
	}
}

// Start makes worker start working.
// 1. queue self into the worker queue.
// 2. fetch task from self task queue.
// 3. finish the task then go to 1.
func (w *Worker) Start(workerQueue chan *Worker) {
	logger.Debugf("Start: worker[%s]", w.Name)

	for {
		// queue self into the worker queue
		workerQueue <- w

		select {
		// fetch task from self task queue.
		case task := <-w.ChTask:
			w.pub(WorkerEv, WorkerEvTaskReceived, w, task)

			event := WorkerEvTaskSuccess
			status := "completed"

			ok := w.Machine.Run(task)
			if !ok {
				if err := w.Machine.Rollback(task); err != nil {
					logger.Warnf("err: %s", err)
				}

				event = WorkerEvTaskFail
				status = "error"
			}

			w.pub(WorkerEv, event, w, task)
			w.pub(WorkerEv, WorkerEvTaskCompleted, w, task)
			w.pub(TxBatchEv, TxBatchEvTranscoded, task.JobID, task.Context, status)

		// terminate the worker
		case <-w.chQuit:
			logger.Debugf("Stop: worker[%s]", w.Name)
			w.Machine.Terminate()
			w.IsTerminated.Set()
			return
		}
	}
}

// Stop makes worker stop handling task after last finished
func (w *Worker) Stop() {
	go func() {
		w.chQuit <- true
	}()
}

// IsBusy checks if worker is busy
func (w *Worker) IsBusy() bool {
	val := atomic.LoadInt32(&w.isBusy)
	return val == 1
}

// SetBusy marks worker busy or not
func (w *Worker) SetBusy(b bool) {
	if b {
		atomic.StoreInt32(&w.isBusy, 1)
		return
	}
	atomic.StoreInt32(&w.isBusy, 0)
}

// SetOnHandTask sets onHandTask
func (w *Worker) SetOnHandTask(t Task) error {
	return LockOperation(
		w.mutex,
		func() error {
			w.onHandTask = t
			return nil
		},
	)
}

// GetOnHandTask gets onHandTask
func (w *Worker) GetOnHandTask() Task {
	var result Task
	LockOperation(
		w.mutex,
		func() error {
			result = w.onHandTask
			return nil
		},
	)
	return result
}
