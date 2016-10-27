package middleware

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"straas.io/base/errors"
	"straas.io/external"
	"straas.io/external/rtc"
	"straas.io/middle/config"
	"straas.io/middle/endpoint"
	"straas.io/middle/logger"

	"cloud.google.com/go/datastore"
	"github.com/ahmetalpbalkan/go-linq"
	"github.com/iKala/gogoo"
	"github.com/iKala/gosak/flowutil"
	"github.com/iKala/gosak/formatutil"
	"github.com/iKala/gosak/timeutil"
	"github.com/iKala/gotools/etcd"
	cache "github.com/patrickmn/go-cache"
	"github.com/satori/go.uuid"
	"github.com/xeipuuv/gojsonschema"
	compute "google.golang.org/api/compute/v1"
)

const (
	// DefaultMachineType defines default level of VM
	DefaultMachineType = "g1-small"

	// PremiumMachineType defines level for premium broadcaster
	PremiumMachineType = "n1-highcpu-4"

	// RtcOutputTranscode is for premium owner, the stream will be transcoded.
	// This needs higher level machine (PremiumMachineType)
	RtcOutputTranscode = "transcode"

	// RtcOutputRemux is default, the stream will not be transcoded. (DefaultMachineType)
	RtcOutputRemux = "remux"

	defaultNumberOfCPUs       = 4
	creatingInstanceLimit     = 4 // prevent numerous instance creation
	wakingInstanceLimit       = 4 // prevent numerous instance waking
	diskSpaceMinLimit         = 15
	spareInstanceMaxThreshold = 20
	defaultDiskSizeGb         = 60

	instanceReadyStreamTimeout = 3 * time.Minute
	affinityTimeout            = 3 * time.Minute
	snapshotReadyTimeout       = 3 * time.Minute
	gceMinChargedInterval      = 10 * time.Minute

	etcdInstanceLockDir = "instance_lock"
	etcdInstanceLockTTL = 10 * time.Second
	instanceKind        = "Instance"
)

// NumberOfCreatingInstance records the number of instance which is under creating
var NumberOfCreatingInstance int32

// NumberOfWakingInstance records the number of instance which is under waking
var NumberOfWakingInstance int32

// TxInstanceOperator execute transaction on the instance
type TxInstanceOperator func(instance *Instance)

// InstanceFilter defines the criteria to filter instance
type InstanceFilter func(vm *compute.Instance) bool

// DiskFilter defines the criteria to filter disk
type DiskFilter func(vm *compute.Disk) bool

// VMPatcher patches VM
type VMPatcher func(vm *compute.Instance, instanceName string)

// DiskProvider provides disk
type DiskProvider func(diskName string) error

// NotRespondingInstanceDetector defines how to judge responseness of instance
type NotRespondingInstanceDetector func(instance Instance) bool

// InstanceState enums state of instance
type InstanceState int

const (
	// STOPPED instance turned off
	STOPPED InstanceState = iota
	// STOPPING instance is going to be turned off
	STOPPING
	// PREPARED instance is turned on but not being used
	PREPARED
	// OCCUPIED instance is turned on and being used
	OCCUPIED
)

var instanceState = [...]string{
	"stopped",
	"stopping",
	"prepared",
	"occupied",
}

func (status InstanceState) String() string {
	return instanceState[status]
}

// ByAffinityTime is used to sort instance by affinityTime (desc)
type ByAffinityTime []Instance

func (a ByAffinityTime) Len() int           { return len(a) }
func (a ByAffinityTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByAffinityTime) Less(i, j int) bool { return a[i].AffinityTime > a[j].AffinityTime }

// Instance acts as data binding object of datastore `Instance`
type Instance struct {
	Key             *datastore.Key `datastore:"-"`
	AffinityChannel string         `datastore:"affinityChannel"`
	AffinityTime    int            `datastore:"affinityTime"`
	WakeTime        int            `datastore:"wakeTime"`
	Snapshot        string         `datastore:"snapshot"`
	IP              string         `datastore:"ip"`
	State           string         `datastore:"state"`
	Zone            string         `datastore:"zone"`
	MachineType     string         `datastore:"machineType"`
}

// InstanceManager manages the lifecycle of GCE VM instance
type InstanceManager struct {
	*config.AppContext `inject:""`
	*gogoo.GoGoo       `inject:""`
	*endpoint.Manager  `inject:""`
	*KeyValueManager   `inject:""`

	external.RTC
	Etcd *etcd.Wrapper

	zone                    string
	instanceTemplate        []byte
	tupleCache              *cache.Cache
	SupportInstanceManagers []*InstanceManager
}

// Setup acts like constructor
func (im *InstanceManager) Setup() {
	logger.Debug("InstanceManager setup ...")
	im.zone = im.AppContext.Zone

	im.instanceTemplate, _ = ioutil.ReadAll(config.LoadAsset(im.AppContext.InstanceTemplatePath))

	// Initialize cache
	im.tupleCache = cache.New(cache.NoExpiration, -1)

	// Initialize latest_snapshot
	latestSnapshot, _ := im.GetLatestSnapshot()
	im.tupleCache.Set("latest_snapshot", latestSnapshot, cache.NoExpiration)

	im.RTC = rtc.NewRTC(logger.GetLogger())
	im.Etcd = etcd.NewWrapper(im.EtcdServers)

	// Setup SupportInstanceManagers
	im.SupportInstanceManagers = im.setupSupportInstanceManagers(
		[]string{"asia-east1-b", "asia-east1-c"}, *im)
}

// Kind returns datastore kind
func (im *InstanceManager) Kind() string {
	return instanceKind + im.Gds.SuffixOfKind
}

// BuildKey builds datastore key
func (im *InstanceManager) BuildKey(keyName string) *datastore.Key {
	return im.Gds.BuildKey(im.Kind(), keyName)
}

// GetVM gets VM by instance name.
// If the corresponding instance record existed, use zone of instance record, else use zone of config
func (im *InstanceManager) GetVM(instanceName string) (*compute.Instance, error) {
	var zone string

	instance := &Instance{}
	if err := im.Gds.Get(im.BuildKey(instanceName), instance); err != nil {
		// record not existed, use zone of config
		zone = im.zone
	} else {
		zone = instance.Zone
	}

	vm, err := im.Gce.GetVM(im.ProjectID, zone, instanceName)
	if err != nil {
		logger.Warnf("VM not existed: instance[%s]", instanceName)

		return nil, err
	}

	return vm, nil
}

// CreateInstance creates a new VM and its record
func (im *InstanceManager) CreateInstance(machineType string) {
	// Avoid instance creation burst
	if atomic.LoadInt32(&NumberOfCreatingInstance) >= creatingInstanceLimit {
		logger.Warnf("Don't create, too many creating: number[%d]",
			atomic.LoadInt32(&NumberOfCreatingInstance))
		return
	}

	if !im.isCPUAbundant(im.GetNumberOfCPUs, im.GetCPUConsumed()) {
		return
	}

	// Record creating action
	atomic.AddInt32(&NumberOfCreatingInstance, 1)
	defer atomic.AddInt32(&NumberOfCreatingInstance, -1)

	instanceName := im.BuildInstanceName(im.InstanceNamePrefix)
	err := im.createVMBySnapshot(
		instanceName,
		im.GetVMPatcher(machineType),
		im.GetDiskProvider(defaultDiskSizeGb))
	if err != nil {
		logger.Warnf("Fail create instance: instance[%s], err[%s]", instanceName, err)

		return
	}

	instanceReadyObserver := make(chan bool)
	go im.ProbeInstanceReadyStream(instanceName, instanceReadyObserver)

	done := <-instanceReadyObserver
	if !done {
		logger.Warnf("Timeout instance ready observer: instance[%s]", instanceName)

		return
	}

	im.Gce.AttachTags(im.ProjectID, im.zone, instanceName, im.AppContext.InstanceTags)
	im.Gce.AttachTags(im.ProjectID, im.zone, instanceName, []string{"running"})

	// Record the created instance to datastore
	cachedSnaphost, _ := im.tupleCache.Get("latest_snapshot")
	vm, _ := im.GetVM(instanceName)

	newInstance := &Instance{
		Key:          im.BuildKey(instanceName),
		AffinityTime: timeutil.TimeToEpoch(time.Now()),
		Snapshot:     cachedSnaphost.(string),
		WakeTime:     timeutil.TimeToEpoch(time.Now()),
		IP:           im.Gce.GetNetworkIP(vm),
		State:        PREPARED.String(),
		Zone:         im.zone,
		MachineType:  machineType}

	im.Gds.Put(newInstance.Key, newInstance)
}

// CreateDedicatedInstance creates customized dedicated VM and its record
func (im *InstanceManager) CreateDedicatedInstance(channel string, patcher VMPatcher, diskProvider DiskProvider) {
	// Avoid instance creation burst
	if atomic.LoadInt32(&NumberOfCreatingInstance) >= creatingInstanceLimit {
		logger.Warnf("Don't create, too many creating: number[%d]",
			atomic.LoadInt32(&NumberOfCreatingInstance))
		return
	}

	if !im.isCPUAbundant(im.GetNumberOfCPUs, im.GetCPUConsumed()) {
		return
	}

	// Record creating action
	atomic.AddInt32(&NumberOfCreatingInstance, 1)
	defer atomic.AddInt32(&NumberOfCreatingInstance, -1)

	instanceName := im.BuildInstanceName(im.InstanceNamePrefix)
	err := im.createVMBySnapshot(instanceName, patcher, diskProvider)
	if err != nil {
		logger.Warnf("Fail create instance by snapshot: instance[%s], err[%s]", instanceName, err)

		return
	}

	instanceReadyObserver := make(chan bool)
	go im.ProbeInstanceReadyStream(instanceName, instanceReadyObserver)

	done := <-instanceReadyObserver
	if !done {
		logger.Warnf("Timeout instance ready observer: instance[%s]", instanceName)

		return
	}

	// Record the created instance to datastore
	cachedSnaphost, _ := im.tupleCache.Get("latest_snapshot")
	vm, _ := im.GetVM(instanceName)
	machineType := formatutil.GetLastSplit(vm.MachineType, "/")

	newInstance := &Instance{
		Key:          im.BuildKey(instanceName),
		AffinityTime: timeutil.TimeToEpoch(time.Now()),
		Snapshot:     cachedSnaphost.(string),
		WakeTime:     timeutil.TimeToEpoch(time.Now()),
		IP:           im.Gce.GetNetworkIP(vm),
		State:        channel,
		Zone:         im.zone,
		MachineType:  machineType}

	im.Gds.Put(newInstance.Key, newInstance)
}

// WakeInstance changes instance state from `stopped` -> `prepared`
func (im *InstanceManager) WakeInstance(instanceName string) {
	defer timeutil.TimeTrack(time.Now(), fmt.Sprintf("Wake instance[%s]", instanceName))

	// Avoid instance waking burst
	if atomic.LoadInt32(&NumberOfWakingInstance) >= wakingInstanceLimit {
		logger.Warnf("Don't wake, too many waking: number[%d]",
			atomic.LoadInt32(&NumberOfWakingInstance))
		return
	}

	logger.Infof("Wake: instance[%s]", instanceName)

	instance := &Instance{}
	if err := im.Gds.Get(im.BuildKey(instanceName), instance); err != nil {
		return
	}

	// Record waking action
	atomic.AddInt32(&NumberOfWakingInstance, 1)
	defer atomic.AddInt32(&NumberOfWakingInstance, -1)

	preparedChecker := func(projectID, zone, instanceName string) (bool, error) {
		// Check it is running
		instanceRunningObserver := make(chan bool)
		go im.Gce.ProbeVMRunning(projectID, zone, instanceName, instanceRunningObserver)

		if done := <-instanceRunningObserver; !done {
			return false, fmt.Errorf("VM not running: instance[%s]", instanceName)
		}

		// Check it is ready for stream
		instanceReadyObserver := make(chan bool)
		go im.ProbeInstanceReadyStream(instanceName, instanceReadyObserver)

		if done := <-instanceReadyObserver; !done {
			return false, fmt.Errorf("VM not ready to stream: instance[%s]", instanceName)
		}

		return true, nil
	}

	if _, err := im.Gce.StartVM(im.ProjectID, instance.Zone, instanceName, preparedChecker); err != nil {
		logger.Warnf("Fail Wake: instance[%s], err[%s]", instanceName, err)
		return
	}

	// Change instance state: `stopped` -> `prepared`
	var stoppedToPrepared TxInstanceOperator = func(instance *Instance) {
		instance.State = PREPARED.String()
		instance.WakeTime = timeutil.TimeToEpoch(time.Now())
		instance.AffinityChannel = ""
	}
	if err := im.TxOperateInstance(im.BuildKey(instanceName), stoppedToPrepared); err != nil {
		logger.Warnf("Fail Wake (tx operation err): instance[%s], err[%s]", instanceName, err)
	}
	im.Gce.AttachTags(im.ProjectID, instance.Zone, instanceName, []string{"running"})
}

// SleepInstance changes instance state from `prepared` -> `stopping` -> `stopped`
func (im *InstanceManager) SleepInstance(instanceName string, observer chan<- bool) {
	instance := &Instance{}
	if err := im.Gds.Get(im.BuildKey(instanceName), instance); err != nil {
		observer <- false
		return
	}

	if time.Now().Sub(timeutil.EpochToTime(instance.WakeTime)) < gceMinChargedInterval {
		logger.Infof("Don't Sleep (less than min charged interval): instance[%s]", instanceName)
		observer <- false
		return
	}

	// Tx: `prepared` to `stopping` (avoid the anchor take it as `prepared` during stopping)
	var preparedToStopping TxInstanceOperator = func(instance *Instance) {
		instance.State = STOPPING.String()
	}
	if err := im.TxOperateInstance(im.BuildKey(instanceName), preparedToStopping); err != nil {
		logger.Warnf("Fail Sleep (tx operation err): instance[%s], err[%s]", instanceName, err)
		observer <- false
		return
	}

	// Try to aquire the instance lock (maybe held by `GetAvailableInstance`)
	err := im.DisLock(instanceName)
	if err != nil {
		logger.Debugf("Instance lock is held: instance[%s]", instanceName)
		observer <- false
		return
	}
	defer im.Unlock(instanceName)
	// :~)

	logger.Infof("Sleep: instance[%s]", instanceName)
	observer <- true

	// Stop instance
	stoppedChecker := func(projectID, zone, instanceName string) (bool, error) {
		instanceStoppedObserver := make(chan bool)
		go im.Gce.ProbeVMStopped(projectID, zone, instanceName, instanceStoppedObserver)

		done := <-instanceStoppedObserver
		if !done {
			return false, fmt.Errorf("VM not stopped: instance[%s]", instanceName)
		}

		return true, nil
	}

	if _, err := im.Gce.StopVM(im.ProjectID, instance.Zone, instanceName, stoppedChecker); err == nil {
		var stoppingToStopped TxInstanceOperator = func(instance *Instance) {
			// Change state to `stopped`
			instance.State = STOPPED.String()
		}
		if err := im.TxOperateInstance(im.BuildKey(instanceName), stoppingToStopped); err != nil {
			logger.Warnf("Fail Sleep (tx operation err): instance[%s], err[%s]", instanceName, err)
		}
		im.Gce.DetachTags(im.ProjectID, instance.Zone, instanceName, []string{"running"})
	} else {
		logger.Warnf("Fail Sleep: instance[%s], err[%s]", instanceName, err)
	}
}

// createVMBySnapshot creates a new instance by snapshot (RTC server snapshot)
func (im *InstanceManager) createVMBySnapshot(instanceName string, patcher VMPatcher, diskProvider DiskProvider) error {
	if err := diskProvider(instanceName); err != nil {
		return err
	}

	// Create vm by that disk
	vm, err := im.Gce.InitVMFromTemplate(im.instanceTemplate, im.zone)
	if err != nil {
		return err
	}

	patcher(vm, instanceName)

	if err := im.Gce.NewVM(im.ProjectID, im.zone, vm); err != nil {
		return err
	}

	return nil
}

// createVMByImage creates a new instance by snapshot (RTC server image)
func (im *InstanceManager) createVMByImage(instanceName, imageName string, sizeGb int64) error {
	logger.Tracef("Create VM by image: instance[%s], image[%s]", instanceName, imageName)

	vm, err := im.Gce.InitVMFromTemplate(im.instanceTemplate, im.zone)
	if err != nil {
		return err
	}

	vm.Name = instanceName
	diskName := instanceName
	for _, attachDisk := range vm.Disks {
		attachDisk.InitializeParams = &compute.AttachedDiskInitializeParams{
			DiskName:    diskName,
			DiskSizeGb:  sizeGb,
			SourceImage: fmt.Sprintf("global/images/%s", imageName),
		}
	}

	if err := im.Gce.NewVM(im.ProjectID, im.zone, vm); err != nil {
		return err
	}

	return nil
}

// Attach changes instance state from `prepared` to `occupied`
func (im *InstanceManager) Attach(instanceName, channel string) error {
	logger.Infof("Attach: channel[%s], instance[%s]", channel, instanceName)

	preparedToOccupied := func(instance *Instance) {
		if instance.State == channel {
			return
		}
		instance.State = OCCUPIED.String()
		instance.AffinityChannel = channel
	}

	err := im.TxOperateInstance(im.BuildKey(instanceName), preparedToOccupied)
	if err != nil {
		return errors.Wrapf(err, "Attach fails: channel[%s], instance[%s]", channel, instanceName)
	}

	return nil
}

// Detach changes instance state from `occupied` to `prepared`
func (im *InstanceManager) Detach(instanceName, affinityChannel string) error {
	logger.Infof("Detach: affinityChannel[%s], instance[%s]", affinityChannel, instanceName)

	occupiedToPrepared := func(instance *Instance) {
		if instance.State == affinityChannel {
			return
		}
		instance.AffinityChannel = affinityChannel
		instance.AffinityTime = timeutil.TimeToEpoch(time.Now())
		instance.State = PREPARED.String()
	}

	if err := im.TxOperateInstance(im.BuildKey(instanceName), occupiedToPrepared); err != nil {
		logger.Warnf("Fail Detach (tx operation err): channel[%s], instance[%s], err[%s]", affinityChannel, instanceName, err)
		return err
	}

	return nil
}

// RecycleInstance deletes vm instance and its datastore record
func (im *InstanceManager) RecycleInstance(instanceName string) {
	vmInstance, _ := im.GetVM(instanceName)
	if vmInstance == nil {
		logger.Debugf("Not found instance: instance[%s]", instanceName)
		return
	}

	if im.IsReadyForClose(instanceName) {
		logger.Debugf("Recycle instance: instance[%s]", instanceName)

		// Delete instance datastore record
		im.Gds.Delete(im.BuildKey(instanceName))

		// Delete vm instance
		arr := strings.Split(vmInstance.Zone, "/")
		im.Gce.DeleteVM(im.ProjectID, arr[len(arr)-1], instanceName)
	}
}

// BuildDiskURI builds complete disk URI
func (im *InstanceManager) BuildDiskURI(diskName string) string {
	return fmt.Sprintf("projects/%s/zones/%s/disks/%s", im.ProjectID, im.zone, diskName)
}

// detachAffinity detaches affinity from instance entity
func (im *InstanceManager) detachAffinity(instanceName string) error {
	logger.Infof("detachAffinity: instance[%s]", instanceName)

	clearAffinity := func(instance *Instance) {
		instance.AffinityChannel = ""
		instance.AffinityTime = timeutil.TimeToEpoch(time.Now())
	}

	if err := im.TxOperateInstance(im.BuildKey(instanceName), clearAffinity); err != nil {
		logger.Warnf("Fail ClearAffinity (tx operation err): instance[%s], err[%s]", instanceName, err)
		return err
	}

	return nil
}

// ClearAffinity clears affinity of some channel
func (im *InstanceManager) ClearAffinity(channel string) {
	query := datastore.NewQuery(im.Kind()).
		Filter("affinityChannel =", channel)

	result := &[]*Instance{}
	im.Gds.GetAll(query, result)
	instances := *result

	for _, ins := range instances {
		im.detachAffinity(ins.Key.Name())
	}
}

// ProbeInstanceReadyStream call RTC server API to make sure it is ready to serve stream
func (im *InstanceManager) ProbeInstanceReadyStream(instanceName string, observer chan<- bool) {
	startTime := time.Now()

	for {
		if time.Now().Sub(startTime) > instanceReadyStreamTimeout {
			logger.Warnf("Instance Ready Timeout: instance[%s]", instanceName)
			observer <- false

			break
		}

		if err := im.assertInstanceReady(instanceName); err != nil {
			time.Sleep(10 * time.Second)
			continue
		}

		logger.Infof("Instance Ready Stream: name[%s]", instanceName)
		observer <- true

		break
	}
}

// ProbeSnapshotReady probes if the snapshot is READY
func (im *InstanceManager) ProbeSnapshotReady(snapshotName string, observer chan<- bool) {
	startTime := time.Now()

	for {
		if time.Now().Sub(startTime) > snapshotReadyTimeout {
			logger.Warnf("Snapshot Ready Timeout: snapshot[%s]", snapshotName)
			observer <- false

			break
		}

		snp, err := im.Gce.GetSnapshot(im.ProjectID, snapshotName)
		if err != nil {
			logger.Warnf("%s", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if snp.Status != "READY" {
			time.Sleep(5 * time.Second)
			continue
		}

		logger.Infof("Snapshot Ready: snapshot[%s]", snapshotName)
		observer <- true

		break
	}
}

// GetDedicatedInstances returns those instances whose `state` equals to some channel
func (im *InstanceManager) GetDedicatedInstances(channel string) []*Instance {
	query := datastore.NewQuery(im.Kind()).
		Filter("state =", channel)

	result := &[]*Instance{}
	im.Gds.GetAll(query, result)
	instances := *result

	reentrantValidator := func(body string) bool {
		liveObjChannel := im.Manager.GetLiveChannel(body)
		if liveObjChannel == channel || liveObjChannel == "" {
			return true
		}
		return false
	}

	dedicatedInstances := []*Instance{}
	for _, instance := range instances {
		if err := im.assertInstanceReady(instance.Key.Name(), im.diskSpaceValidator, reentrantValidator); err == nil {
			dedicatedInstances = append(dedicatedInstances, instance)
		}
	}

	return dedicatedInstances
}

// IsTranscodingStarted checks if some RTC instance starts transcoding for some channel
func (im *InstanceManager) IsTranscodingStarted(instanceName, channelID string) bool {
	//TODO logic should be changed, when one RTC serves multiple client
	startTranscodingValidator := func(body string) bool {
		liveObjChannel := im.Manager.GetLiveChannel(body)
		if liveObjChannel == channelID {
			return true
		}
		return false
	}
	err := im.assertInstanceReady(instanceName, startTranscodingValidator)
	return err == nil
}

// GetReservedInstance gets the reserved instance
func (im *InstanceManager) GetReservedInstance(channelID string) *Instance {
	// Query instances with specific affinity channel
	cachedSnaphost, _ := im.tupleCache.Get("latest_snapshot")
	query := datastore.NewQuery(im.Kind()).
		Filter("snapshot =", cachedSnaphost).
		Filter("affinityChannel =", channelID)

	result := &[]*Instance{}
	im.Gds.GetAll(query, result)
	instances := *result

	// Find the suitable reserved instance
	i := 0
	var reservedInstance *Instance
	for _, instance := range instances {
		if !(instance.State == PREPARED.String() || instance.State == OCCUPIED.String()) {
			continue
		}

		instanceName := instance.Key.Name()
		// Choose the first match
		if i == 0 {
			logger.Infof("Found reserved instance: channel[%s], instance[%s]", channelID, instanceName)
			reservedInstance = instance

			i++
			continue
		}

		go func() {
			// One channel doesn't allow more than one affinity channel
			logger.Warnf(
				"Clear wrong reserved instance: channel[%s], instance[%s]",
				channelID, instanceName)

			clearAffinity := func(instance *Instance) {
				instance.AffinityChannel = ""
			}
			if err := im.TxOperateInstance(im.BuildKey(instanceName), clearAffinity); err != nil {
				logger.Warnf(
					"Fail ClearAffinity (tx operation err): channel[%s], instance[%s], err[%s]",
					channelID, instanceName, err)
			}
		}()
	}

	if reservedInstance != nil {
		if err := im.assertInstanceReady(reservedInstance.Key.Name(), im.diskSpaceValidator); err != nil {
			reservedInstance = nil
		}
	}

	return reservedInstance
}

// GetSpareInstance returns the first matched spare instance
func (im *InstanceManager) GetSpareInstance(
	channel string, checkTime time.Time, preparedInstances []Instance) *Instance {

	reentrantValidator := func(body string) bool {
		liveObjChannel := im.Manager.GetLiveChannel(body)
		if liveObjChannel == channel || liveObjChannel == "" {
			return true
		}
		return false
	}

	for _, instance := range preparedInstances {
		if im.isInstanceReserved(instance, checkTime) {
			continue
		}

		if err := im.assertInstanceReady(instance.Key.Name(), im.diskSpaceValidator, reentrantValidator); err == nil {
			return &instance
		}
	}

	return nil
}

func (im *InstanceManager) isInstanceReserved(instance Instance, checkTime time.Time) bool {
	if instance.AffinityChannel != "" {
		if checkTime.Sub(timeutil.EpochToTime(instance.AffinityTime)) < affinityTimeout {
			return true
		}
	}
	return false
}

// GetSpareInstances returns the all matched spare instances
func (im *InstanceManager) GetSpareInstances(
	channel string, checkTime time.Time, preparedInstances []Instance) []Instance {

	spareInstances := []Instance{}

	reentrantValidator := func(body string) bool {
		liveObjChannel := im.Manager.GetLiveChannel(body)
		if liveObjChannel == channel || liveObjChannel == "" {
			return true
		}
		return false
	}

	for _, instance := range preparedInstances {
		// Check if the affinityTime is timeout
		if im.isInstanceReserved(instance, checkTime) {
			continue
		}

		if err := im.assertInstanceReady(instance.Key.Name(), im.diskSpaceValidator, reentrantValidator); err != nil {
			continue
		}

		spareInstances = append(spareInstances, instance)
	}

	return spareInstances
}

func (im InstanceManager) diskSpaceValidator(body string) bool {
	space := im.getDiskFreeSpace(body)
	if space < diskSpaceMinLimit {
		logger.Warnf("Disk outage: space[%d]", space)

		return false
	}
	return true
}

func (im *InstanceManager) filterPreparedInstances(preparedInstances []Instance, machineType string) []Instance {
	result := []Instance{}

	for _, instance := range preparedInstances {
		if instance.MachineType == machineType {
			result = append(result, instance)
		}
	}

	return result
}

// getInstanceByState gets instances by specified state
func (im *InstanceManager) getInstancesByState(state string) ([]*datastore.Key, []Instance) {
	cachedSnaphost, _ := im.tupleCache.Get("latest_snapshot")
	query := datastore.NewQuery(im.Kind()).
		Filter("snapshot =", cachedSnaphost).
		Filter("state =", state)

	keys, err := im.Gds.GetKeysOnly(query)
	if err != nil {
		logger.Warnf("Error: %s", err)
		return nil, nil
	}

	preparedInstances := make([]Instance, len(keys))
	im.Gds.GetMulti(keys, preparedInstances)
	for i := 0; i < len(keys); i++ {
		preparedInstances[i].Key = keys[i]
	}

	return keys, preparedInstances
}

// Checks if RTC server works normally according to the response of RTC /monitoring API
func (im *InstanceManager) assertInstanceReady(
	instanceName string,
	responseValidators ...func(string) bool,
) error {

	body, err := im.Monitoring(instanceName)
	if err != nil {
		return err
	}

	err = im.ValidateMonitoringResponse(body)
	if err != nil {
		return errors.Wrap(err, "Invalid JSON schema of monitoring response")
	}

	isValid := formatutil.StringValidate(body, responseValidators...)
	if !isValid {
		return errors.Errorf("Response validation fails: body[%s]", body)
	}

	return nil
}

// GetAvailableInstance coordinates the available instance for anchor (reserved first, then spare)
func (im *InstanceManager) GetAvailableInstance(channelID, strategy, machineType string) *Instance {
	defer timeutil.TimeTrack(time.Now(), "GetAvailableInstance")

	// Dedicated
	if dedicatedInstances := im.GetDedicatedInstances(channelID); len(dedicatedInstances) > 0 {
		chosenInstance := dedicatedInstances[rand.Intn(len(dedicatedInstances))]
		logger.Infof("Chosen dedicated instance: instance[%s]", chosenInstance.Key.Name())

		return chosenInstance
	}

	if strategy == "spareOnly" {
		goto spareOnly
	}

	// Reserved
	if reservedInstance := im.GetReservedInstance(channelID); reservedInstance != nil {
		return reservedInstance
	}

spareOnly:
	// Spare
	_, preparedInstances := im.getInstancesByState(PREPARED.String())
	preparedInstances = im.filterPreparedInstances(preparedInstances, machineType)
	if spareInstance := im.GetSpareInstance(channelID, time.Now(), preparedInstances); spareInstance != nil {
		logger.Infof("Chosen spare instance: channel[%s], instance[%s], type[%s]",
			channelID, spareInstance.Key.Name(), machineType)

		return spareInstance
	}

	logger.Infof("No available instance: channel[%s], type[%s]", channelID, machineType)
	return nil
}

// GetNumOfInstances gets number of instances by state & machine type
func (im *InstanceManager) GetNumOfInstances(state, machineType string) int {
	_, r := im.getInstancesByState(state)
	r = im.filterPreparedInstances(r, machineType)

	return len(r)
}

func (im *InstanceManager) getDiskFreeSpace(response string) int {
	result, _ := formatutil.JSONStringToMap(response)

	system := result["system"].(map[string]interface{})

	spaceString := system["spaceFree"]
	arr := strings.Split(spaceString.(string), ".")
	space, _ := strconv.Atoi(arr[0])

	return space
}

// GetCPUConsumed gets total used CPU amount
// TODO not only n1-highcpu-4, need consider g1-small
func (im *InstanceManager) GetCPUConsumed() int {
	count, _ := im.Gds.GetCount(datastore.NewQuery(im.Kind()))

	return defaultNumberOfCPUs * count
}

// BuildInstanceName builds the name of instance with a random number
func (im *InstanceManager) BuildInstanceName(prefix string) string {
	splits := strings.Split(uuid.NewV4().String(), "-")
	name := fmt.Sprintf("%s-%s", prefix, splits[len(splits)-1])

	return name
}

// IsBrokenSession checks RTC server status, if status is `error` then return true
func (im *InstanceManager) IsBrokenSession(s *Session) bool {
	ip, err := im.GetIP(s.Key.Name(), "internal")
	if err != nil {
		return true
	}
	host := ip

	isBadStatus := func(...interface{}) bool {
		return !im.Manager.IsLive(host, s.Channel, im.Monitoring)
	}

	isBrokenSession := flowutil.MultipleConfirm(3, 10*time.Second, isBadStatus)
	if isBrokenSession {
		logger.Infof("Broken session detected: channel[%s], session[%s]", s.Channel, s.Key.Name())
	}

	return isBrokenSession
}

// IsNotResponding return if the instance is not responding
func (im *InstanceManager) IsNotResponding(instance Instance) bool {
	instanceName := instance.Key.Name()

	isInstanceNotReady := func(...interface{}) bool {
		err := im.assertInstanceReady(instanceName)
		return err != nil
	}

	isNotResponding := flowutil.MultipleConfirm(3, 1*time.Minute, isInstanceNotReady)
	if isNotResponding {
		logger.Infof("Not responding instance detected: instance[%s]", instanceName)
	}

	return isNotResponding
}

// IsReadyForClose asks if RTC server is ready to sleep
func (im *InstanceManager) IsReadyForClose(instanceName string) bool {
	readyForCloseChecker := func(body string) bool {
		if err := im.ValidateMonitoringResponse(body); err != nil {
			logger.Debugf("err: %s", err)
			// Assume the instance is ready if non-valid response got
			return false
		}

		result, _ := formatutil.JSONStringToMap(body)
		system := result["system"].(map[string]interface{})
		readyForClose := system["readyForClose"].(bool)

		return readyForClose
	}

	err := im.assertInstanceReady(instanceName, readyForCloseChecker)
	return err == nil
}

// ResetNotRespondingInstances resets those not responding instances
func (im *InstanceManager) ResetNotRespondingInstances(
	isNotResponding NotRespondingInstanceDetector) {

	logger.Infof("[cron] Reset not responding instance")

	_, preparedInstances := im.getInstancesByState(PREPARED.String())

	for _, instance := range preparedInstances {
		go func(instance Instance) {
			if isNotResponding(instance) {
				im.Gce.ResetInstance(im.ProjectID, instance.Zone, instance.Key.Name())
			}
		}(instance)
	}
}

// deleteInstancesWithOldSnapshot deletes instances with old snapshot
func (im *InstanceManager) deleteInstancesWithOldSnapshot() {
	latestSnapshot, _ := im.GetLatestSnapshot()

	query := datastore.NewQuery(im.Kind())

	result := &[]*Instance{}
	im.Gds.GetAll(query, result)
	instances := *result

	for _, instance := range instances {
		go func(instance *Instance) {
			if instance.Snapshot != latestSnapshot {
				if instance.State == PREPARED.String() {
					if im.IsReadyForClose(instance.Key.Name()) {
						logger.Infof("Delete instance with old snapshot: instance[%s]", instance.Key.Name())
						im.Gds.Delete(instance.Key)
						im.Gce.DeleteVM(im.ProjectID, instance.Zone, instance.Key.Name())
					}
				}

				if instance.State == STOPPED.String() {
					logger.Infof("Delete instance with old snapshot: instance[%s]", instance.Key.Name())
					im.Gds.Delete(instance.Key)
					im.Gce.DeleteVM(im.ProjectID, instance.Zone, instance.Key.Name())
				}
			}
		}(instance)
	}
}

// CreateNewInstancesWhenNewSnapshotDeployed creates N new instance with new snapshot
func (im *InstanceManager) CreateNewInstancesWhenNewSnapshotDeployed(number int) {
	latestSnapshot, _ := im.GetLatestSnapshot()

	// Only apply once when snapshot deployed
	cachedSnaphost, _ := im.tupleCache.Get("latest_snapshot")
	logger.Infof("cachedSnaphost: %s", cachedSnaphost)

	if cachedSnaphost != latestSnapshot {
		logger.Infof("New snapshot detected !!")

		snapshotReadyObserver := make(chan bool)
		go im.ProbeSnapshotReady(latestSnapshot, snapshotReadyObserver)

		done := <-snapshotReadyObserver
		if !done {
			logger.Warnf("Timeout snapshot ready observer: snapshot[%s]", latestSnapshot)

			return
		}

		im.batchCreateInstance(number)

		im.tupleCache.Set("latest_snapshot", latestSnapshot, cache.NoExpiration)
	}
}

// batchCreateInstance creates N instances at once
func (im *InstanceManager) batchCreateInstance(number int) {
	logger.Infof("Batch create instance: number[%d]", number)

	if im.IsLh() {
		for i := 0; i < number; i++ {
			go im.SupportInstanceManagers[rand.Intn(len(im.SupportInstanceManagers))].
				CreateInstance(DefaultMachineType)
		}
	}
	for i := 0; i < number; i++ {
		go im.SupportInstanceManagers[rand.Intn(len(im.SupportInstanceManagers))].
			CreateInstance(PremiumMachineType)
	}
}

// HouseKeep acts as resource coordinator periodically
func (im *InstanceManager) HouseKeep(safeSpareInstanceCount int, machineType string) {
	// Prepare for new snapshot deployed
	im.CreateNewInstancesWhenNewSnapshotDeployed(5)

	// Delete instances with old snapshot
	im.deleteInstancesWithOldSnapshot()

	minThreshold, err := im.GetMinNumOfSpareInstance()
	if err == nil && safeSpareInstanceCount < minThreshold {
		safeSpareInstanceCount = minThreshold
	}
	if safeSpareInstanceCount > spareInstanceMaxThreshold {
		safeSpareInstanceCount = spareInstanceMaxThreshold
	}

	logger.Infof("HouseKeep: safeCount[%d], machine[%s]", safeSpareInstanceCount, machineType)

	_, preparedInstances := im.getInstancesByState(PREPARED.String())
	preparedInstances = im.filterPreparedInstances(preparedInstances, machineType)
	spareInstances := im.GetSpareInstances("", time.Now(), preparedInstances)
	spareCount := len(spareInstances)

	diff := spareCount - safeSpareInstanceCount
	if diff > 0 {
		logger.Infof("Too Much instances: diff[%d]", diff)

		cnt := diff
		for _, i := range spareInstances {
			if cnt == 0 {
				break
			}

			if im.IsReadyForClose(i.Key.Name()) {
				sleepObserver := make(chan bool)
				go im.SleepInstance(i.Key.Name(), sleepObserver)
				if done := <-sleepObserver; !done {
					continue
				}
				cnt--
			} else {
				logger.Infof("Not ReadyForClose: instance[%s]", i.Key.Name())
			}
		}

		return
	}

	if diff < 0 {
		// Too few spare instances, wake some for backup
		logger.Infof("Too Few instances: diff[%d]", diff)

		numOfWake := -diff

		query := datastore.NewQuery(im.Kind()).
			Filter("state =", STOPPED.String()).
			Filter("machineType =", machineType)
		result := &[]*Instance{}
		im.Gds.GetAll(query, result)
		instances := *result
		shuffle(instances)

		// Found enough
		if len(instances) >= numOfWake {
			for i := 0; i < numOfWake; i++ {
				go im.WakeInstance(instances[i].Key.Name())
			}
			return
		}

		// Found but not enough, wake all and create some
		if len(instances) < numOfWake {
			for _, instance := range instances {
				go im.WakeInstance(instance.Key.Name())
			}

			for i := 0; i < numOfWake-len(instances); i++ {
				go im.SupportInstanceManagers[rand.Intn(len(im.SupportInstanceManagers))].
					CreateInstance(machineType)
			}
			return
		}

		// Not found
		if len(instances) == 0 {
			for i := 0; i < numOfWake; i++ {
				go im.SupportInstanceManagers[rand.Intn(len(im.SupportInstanceManagers))].
					CreateInstance(machineType)
			}
			return
		}
	}

	return
}

// GetLatestSnapshot gets the latest snapshot of the project
func (im *InstanceManager) GetLatestSnapshot() (string, error) {
	snapshots, err := im.Gce.ListSnapshots(im.ProjectID)
	if err != nil {
		return "", err
	}

	latestSnapshot, err2 := im.Gce.GetLatestSnapshot(im.SnapshotNamePrefix, snapshots)
	if err2 != nil {
		return "", err2
	}

	return latestSnapshot.Name, nil
}

// UpdateInstanceIP updates ephemeral IP of VM instance
func (im *InstanceManager) UpdateInstanceIP() {
	logger.Infof("[cron] Update instance IP")

	query := datastore.NewQuery(im.Kind())
	result := &[]*Instance{}
	im.Gds.GetAll(query, result)

	for _, instance := range *result {
		vm, _ := im.GetVM(instance.Key.Name())
		if vm == nil {
			continue
		}

		ip := im.Gce.GetNetworkIP(vm)
		if !formatutil.IsIP(ip) {
			continue
		}

		if instance.IP != ip {
			// The IP will change when stopped instance starts again
			logger.Infof("Ephemeral IP changes: instance[%s], original[%s], new[%s]", instance.Key.Name(), instance.IP, ip)

			// Update instance IP
			var updateIP TxInstanceOperator = func(instance *Instance) {
				instance.IP = ip
			}
			im.TxOperateInstance(instance.Key, updateIP)
		}
	}
}

// GetIP gets the instance IP.
// Datastore first or query google API to get ephemeral IP.
func (im *InstanceManager) GetIP(instanceName, target string) (string, error) {
	instance := &Instance{}
	im.Gds.Get(im.BuildKey(instanceName), instance)

	ip, err := im.GetIPByInstance(instance, target)
	if err != nil {
		return im.GetIPByName(instanceName, target)
	}

	return ip, err
}

// GetIPByName gets IP by the name of instance
func (im *InstanceManager) GetIPByName(instanceName, target string) (string, error) {
	vm, _ := im.GetVM(instanceName)
	if vm == nil {
		return "", fmt.Errorf("Fail to get VM: instance[%s]", instanceName)
	}

	var ip string
	if target == "internal" {
		ip = im.Gce.GetNetworkIP(vm)
	}
	if target == "external" {
		ip = im.Gce.GetNatIP(vm)
	}

	if formatutil.IsIP(ip) {
		return ip, nil
	}

	return "", fmt.Errorf("Fail to get IP from VM: instance[%s]", instanceName)
}

// GetIPByInstance gets IP by the instance
func (im *InstanceManager) GetIPByInstance(instance *Instance, target string) (string, error) {
	if formatutil.IsIP(instance.IP) {
		return instance.IP, nil
	}

	return "", fmt.Errorf("Not valid IP[%s]", instance.IP)
}

// CheckInstanceState checks instance state and correct it if something wrong
func (im *InstanceManager) CheckInstanceState() {
	logger.Infof("[cron] Check instance state")

	query := datastore.NewQuery(im.Kind())
	result := &[]*Instance{}
	im.Gds.GetAll(query, result)
	instances := *result

	// Iterate all instances
	for _, instance := range instances {
		switch instance.State {
		case OCCUPIED.String(), PREPARED.String(), STOPPING.String():
			im.auditTerminated(instance)
		}

		switch instance.State {
		case STOPPING.String(), STOPPED.String():
			im.auditRunning(instance)
		}

		// Fix wrong occupied instance (actually `prepared` one)
		if instance.State == OCCUPIED.String() {
			if im.IsReadyForClose(instance.Key.Name()) {
				logger.Debugf("Fix wrong occupied instance, change to prepared: instance[%s]", instance.Key.Name())
				im.Detach(instance.Key.Name(), instance.AffinityChannel)
			}
		}
	}
}

// auditTerminated audits those `TERMINATED` VM but with wrong instance state (OCCUPIED, PREPARED, STOPPING)
func (im *InstanceManager) auditTerminated(instance *Instance) {
	if vm, err := im.Gce.GetVM(im.ProjectID, instance.Zone, instance.Key.Name()); err == nil {
		// If the VM status is `TERMINATED`, change the state of instance to `stopped`
		if vm.Status == "TERMINATED" {
			logger.Debugf("Fix wrong instance state, change to stopped: instance[%s], state[%s]", instance.Key.Name(), instance.State)

			var toStopped TxInstanceOperator = func(instance *Instance) {
				instance.State = STOPPED.String()
				instance.IP = "missing"
			}
			im.TxOperateInstance(instance.Key, toStopped)
		}
	}
}

// auditRunning audits those `RUNNING` VM but with wrong instance state (STOPPING, STOPPED)
func (im *InstanceManager) auditRunning(instance *Instance) {
	instanceName := instance.Key.Name()

	vm, err := im.Gce.GetVM(im.ProjectID, instance.Zone, instanceName)
	if err != nil {
		logger.Warnf("err: %s", err)
		return
	}

	if vm.Status != "RUNNING" {
		return
	}

	err = im.assertInstanceReady(instanceName)
	if err != nil {
		// delete record
		im.Gds.Delete(im.BuildKey(instanceName))

		// delete VM
		arr := strings.Split(vm.Zone, "/")
		im.Gce.DeleteVM(im.ProjectID, arr[len(arr)-1], instanceName)
	}

	logger.Debugf(
		"Change state to prepared: instance[%s], state[%s]",
		instanceName, instance.State,
	)

	var toPrepared TxInstanceOperator = func(instance *Instance) {
		instance.State = PREPARED.String()
		instance.IP = im.Gce.GetNetworkIP(vm)
	}
	im.TxOperateInstance(instance.Key, toPrepared)
}

// DeleteOrphanInstanceByZone deletes those VMs without corresponding datastore records
func (im *InstanceManager) DeleteOrphanInstanceByZone(zone string) {
	op := func(item interface{}) interface{} {
		VM := item.(*compute.Instance)

		// This prevent from deleting the VM instance during creation
		if time.Now().Sub(
			timeutil.EpochToTime(timeutil.ParseTime(VM.CreationTimestamp))) < 5*time.Minute {
			return nil
		}

		logger.Infof(
			"Delete orphan: project[%s], zone[%s], instance[%s]",
			im.ProjectID, zone, VM.Name,
		)
		im.Gce.DeleteVM(im.ProjectID, zone, VM.Name)

		return VM.Name
	}

	VMs := im.ListRtcVms(zone)
	im.IterateOrphanInstance(op, VMs)
}

// IterateOrphanInstance apply operation on those VMs without corresponding datastore records
func (im *InstanceManager) IterateOrphanInstance(
	op func(s interface{}) interface{},
	VMs []*compute.Instance) {

	w := func(item interface{}) bool {
		VM := item.(*compute.Instance)
		emptyInstance := Instance{}
		result := &Instance{}
		im.Gds.Get(im.BuildKey(VM.Name), result)
		if *result == emptyInstance {
			return true
		}
		return false
	}

	linq.From(VMs).Where(w).Select(op).Results()
}

// DeleteRecordOfNotExistedVM deletes those records without corresponding vm instances
func (im *InstanceManager) DeleteRecordOfNotExistedVM() {
	op := func(s interface{}) interface{} {
		instance := s.(*Instance)

		if instance.State == OCCUPIED.String() {
			return nil
		}

		logger.Debugf("Delete record without VM: instance[%s]", instance.Key.Name())

		return im.Gds.Delete(im.BuildKey(instance.Key.Name()))
	}

	im.IterateRecordOfNotExistedVM(op)
}

// IterateRecordOfNotExistedVM apply operation on those datastore records without corresponding VMs
func (im *InstanceManager) IterateRecordOfNotExistedVM(
	op func(s interface{}) interface{},
) {
	query := datastore.NewQuery(im.Kind())
	instances := []*Instance{}
	im.Gds.GetAll(query, &instances)

	w := func(item interface{}) bool {
		instance := item.(*Instance)
		if _, err := im.Gce.GetVM(im.ProjectID, instance.Zone, instance.Key.Name()); err != nil {
			return true
		}
		return false
	}

	linq.From(instances).Where(w).Select(op).Results()
}

// TxOperateInstance do read->operate->write with transaction guarantee
func (im *InstanceManager) TxOperateInstance(
	key *datastore.Key, instanceOperator TxInstanceOperator,
) error {

	tx := im.Gds.GetTx()

	instance := &Instance{}
	if err := tx.Get(key, instance); err != nil {
		return err
	}

	instanceOperator(instance)

	if _, err := tx.Put(key, instance); err != nil {
		return err
	}

	if _, err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// IsSnapshotNewerThan check if the instance uses the snapshot created after specified time
func (im *InstanceManager) IsSnapshotNewerThan(instanceName, dateTime string) bool {
	result := &Instance{}
	im.Gds.Get(im.BuildKey(instanceName), result)
	instance := *result

	splits := strings.Split(instance.Snapshot, "-")
	snapshotTime := splits[len(splits)-1]

	if snapshotTime > dateTime {
		return true
	}

	return false
}

// GetNotUsedDiskFilter returns filter which filters those disks not being attached to VM
func (im *InstanceManager) GetNotUsedDiskFilter() DiskFilter {
	return func(disk *compute.Disk) bool {
		if !strings.Contains(disk.Name, im.InstanceNamePrefix) {
			return false
		}

		if len(disk.Users) == 0 {
			return true
		}

		return false
	}
}

func (im *InstanceManager) isCPUAbundant(
	quota func() (int, error),
	consumed int,
) bool {
	q, err := quota()
	if err != nil {
		logger.Warnf("get quota fails: err[%s]", err)
		return true
	}

	if consumed > q {
		logger.Warnf("CPU Over quota!!")
		return false
	}

	return true
}

// ListFilteredDisksByZone lists all filtered disks by zone
func (im *InstanceManager) ListFilteredDisksByZone(zone string, filter DiskFilter) []*compute.Disk {
	list, _ := im.Gce.ListDisks(im.ProjectID, zone)

	result := []*compute.Disk{}
	for _, disk := range list.Items {
		if !filter(disk) {
			continue
		}

		result = append(result, disk)
	}

	return result
}

// ListRtcVms lists all RTC VMs by zone
func (im *InstanceManager) ListRtcVms(zone string) []*compute.Instance {
	list, err := im.Gce.ListVMsWithFilter(
		im.ProjectID,
		zone,
		fmt.Sprintf("name eq %s.*", im.InstanceNamePrefix),
	)

	if err != nil {
		return []*compute.Instance{}
	}

	return list.Items
}

// GetDiskProvider returns size-customized disk provider
func (im *InstanceManager) GetDiskProvider(sizeGb int64) DiskProvider {
	return func(diskName string) error {
		// Get the latest snapshot
		cachedSnaphost, _ := im.tupleCache.Get("latest_snapshot")

		// Create disk by snapshot
		snapshotURL := fmt.Sprintf("global/snapshots/%s", cachedSnaphost)

		if err := im.Gce.NewDisk(im.ProjectID, im.zone, diskName, snapshotURL, sizeGb); err != nil {
			return err
		}

		return nil
	}
}

// GetVMPatcher returns VM name and disk name customized VM patcher
func (im *InstanceManager) GetVMPatcher(machineType string) VMPatcher {
	if machineType == "" {
		machineType = DefaultMachineType
	}
	return func(vm *compute.Instance, instanceName string) {
		patched := im.Gce.PatchInstanceMachineType(vm.MachineType, machineType)
		vm.MachineType = patched

		// Assign name and disk for the instance
		vm.Name = instanceName
		diskName := instanceName
		for _, attachDisk := range vm.Disks {
			attachDisk.Source = fmt.Sprintf("projects/%s/zones/%s/disks/%s", im.ProjectID, im.zone, diskName)
			attachDisk.InitializeParams = nil
		}
	}
}

// ValidateMonitoringResponse validates RTC monitoring API response
func (im *InstanceManager) ValidateMonitoringResponse(response string) error {
	schema, _ := ioutil.ReadAll(config.LoadAsset("/config/asset/rtc-monitor-response-schema.json"))
	schemaJSON := string(schema[:])

	schemaLoader := gojsonschema.NewStringLoader(schemaJSON)
	documentLoader := gojsonschema.NewStringLoader(response)

	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		logger.Debugf("response: %s", response)
		return err
	}

	if !result.Valid() {
		return errors.Errorf("Not valid response: error[%+v]", result.Errors())
	}

	return nil
}

// DisLock acts as distributed mutex lock on instance entiry
func (im *InstanceManager) DisLock(lock string) error {
	ns := im.AppContext.KeyMapOfEtcd[etcdInstanceLockDir].(string)
	key := ns + lock

	if err := im.Etcd.Lock(key, etcdInstanceLockTTL); err != nil {
		return errors.Wrapf(err, "key: %s", key)
	}
	return nil
}

// Unlock unlocks distributed mutex lock on instance entiry
func (im *InstanceManager) Unlock(lock string) error {
	ns := im.AppContext.KeyMapOfEtcd[etcdInstanceLockDir].(string)
	key := ns + lock

	return im.Etcd.Unlock(key)
}

func (im *InstanceManager) setZone(zone string) {
	im.zone = zone
}

func (im *InstanceManager) setupSupportInstanceManagers(zones []string, sample InstanceManager) []*InstanceManager {
	supportInstanceManagers := []*InstanceManager{}

	for _, z := range zones {
		support := sample
		support.setZone(z)
		supportInstanceManagers = append(supportInstanceManagers, &support)
	}

	return supportInstanceManagers
}

func shuffle(arr []*Instance) {
	t := time.Now()
	rand.Seed(int64(t.Nanosecond())) // no shuffling without this line

	for i := len(arr) - 1; i > 0; i-- {
		j := rand.Intn(i)
		arr[i], arr[j] = arr[j], arr[i]
	}
}
