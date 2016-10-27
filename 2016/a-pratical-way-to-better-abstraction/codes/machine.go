package domain

import (
	"fmt"
	"time"

	bp "straas.io/base/polling"
	"straas.io/bridger/logger"

	"github.com/iKala/gosak/formatutil"
	"github.com/iKala/gosak/timeutil"
	"github.com/pkg/errors"
)

// FileConvStatusQuerier alias query file conv status function
type FileConvStatusQuerier func(host string, task Task) (VodInfo, error)

// PlaylistConvStatusQuerier alias query playlist conv status function
type PlaylistConvStatusQuerier func(host string, task Task) ([]VodInfo, error)

// VideoInfoUpdater alias update video function
type VideoInfoUpdater func(task Task, status VodInfo) (string, error)

type machineAllocator func(diskSize int64, machineType string) (string, error)
type machineStarter func(host string, task Task) (string, error)
type machineTerminator func(name string) error
type videoInfoDeleter func(task Task) (string, error)

// VodInfo contains info about VOD transcoding
type VodInfo struct {
	TaskID         string      `json:"taskId"`
	StartTime      string      `json:"startTime"`
	HashCode       string      `json:"hashCode"`
	Filename       string      `json:"filename"`
	Status         string      `json:"status"`
	Progress       int         `json:"progress"`
	ConvResultInfo interface{} `json:"convResultInfo"`
}

// Machinery abstracts the behaviors of machine
type Machinery interface {
	GetName() string
	SetWorker(worker *Worker)
	Allocate() error
	Run(task Task) bool
	Rollback(task Task) error
	Terminate() error
}

// MachineComponents includes external components needed by machine
type MachineComponents struct {
	Allocator            machineAllocator
	FileConvPollable     bp.Pollable
	FileQuerier          FileConvStatusQuerier
	InfoDeleter          videoInfoDeleter
	InfoUpdater          VideoInfoUpdater
	PlaylistConvPollable bp.Pollable
	PlaylistQuerier      PlaylistConvStatusQuerier
	Starter              machineStarter
	Terminator           machineTerminator
}

// Machine implements Machinery interface.
type Machine struct {
	worker *Worker

	Name        string
	DiskSize    int64
	MachineType string
	VodStatus   string
	ElapsedTime int
	Progress    int

	allocator            machineAllocator
	fileConvPollable     bp.Pollable
	fileQuerier          FileConvStatusQuerier
	infoDeleter          videoInfoDeleter
	infoUpdater          VideoInfoUpdater
	playlistConvPollable bp.Pollable
	playlistQuerier      PlaylistConvStatusQuerier
	starter              machineStarter
	terminator           machineTerminator
}

// NewMachine creates a new machine
func NewMachine(
	diskSize int64,
	machineType string,
	components MachineComponents,
) Machinery {

	return &Machine{
		DiskSize:             diskSize,
		MachineType:          machineType,
		allocator:            components.Allocator,
		fileConvPollable:     components.FileConvPollable,
		fileQuerier:          components.FileQuerier,
		infoDeleter:          components.InfoDeleter,
		infoUpdater:          components.InfoUpdater,
		playlistConvPollable: components.PlaylistConvPollable,
		playlistQuerier:      components.PlaylistQuerier,
		starter:              components.Starter,
		terminator:           components.Terminator,
	}
}

// GetName gets the machine name
func (m *Machine) GetName() string {
	return m.Name
}

// SetWorker sets back reference from machine to worker
func (m *Machine) SetWorker(w *Worker) {
	m.worker = w
}

// Allocate allocates machine
func (m *Machine) Allocate() error {
	instanceName, err := m.allocator(m.DiskSize, m.MachineType)
	if err != nil {
		return err
	}
	m.Name = instanceName
	return nil
}

// Run runs the task, if success return true, else return false
func (m *Machine) Run(task Task) bool {
	defer timeutil.TimeTrack(time.Now(),
		fmt.Sprintf("Machine[%s] finish Task[%s]", m.Name, task.ID))

	err := m.start(task)
	if err != nil {
		logger.Warnf("err: %s", err)
		return false
	}

	err = m.patrol(task)
	if err != nil {
		logger.Warnf("err: %s", err)
		return false
	}

	return true
}

// Rollback clean dirty states when task is not done successfully
func (m *Machine) Rollback(task Task) error {
	_, err := m.infoDeleter(task)

	if err != nil {
		return errors.Wrap(err, "Rollback fails")
	}

	return nil
}

// Terminate terminates the machine
func (m *Machine) Terminate() error {
	return m.terminator(m.Name)
}

// start starts the machine
func (m *Machine) start(task Task) error {
	ctx, _ := formatutil.JSONStringToMap(task.Context)
	logger.Debugf(
		"start: machine[%s], task[%s], vodID[%s], fileType[%s]",
		m.Name, task.ID, ctx["videoID"], ctx["sourceType"],
	)

	_, err := m.starter(m.Name, task)
	return err
}

// patrol checks the status of machine periodically
func (m *Machine) patrol(task Task) error {
	switch GetSrcType(task) {

	case SrcTypeFile:
		pollable := m.fileConvPollable.New(m, task, m.fileQuerier, m.infoUpdater)
		dp := bp.DefaultPoller{
			Interval:       30 * time.Second,
			MaxElapsedTime: 4 * time.Hour,
			Pollable:       pollable,
			Logger:         logger.GetLogger(),
		}
		dp.Poll()
		_, err := pollable.Result()
		return err

	case SrcTypePlaylist:
		pollable := m.playlistConvPollable.New(m, task, m.playlistQuerier, m.infoUpdater)
		dp := bp.DefaultPoller{
			Interval:       30 * time.Second,
			MaxElapsedTime: 4 * time.Hour,
			Pollable:       pollable,
			Logger:         logger.GetLogger(),
		}
		dp.Poll()
		_, err := pollable.Result()
		return err
	}

	return nil
}
