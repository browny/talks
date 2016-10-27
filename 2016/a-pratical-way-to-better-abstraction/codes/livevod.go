package polling

import (
	"strings"

	"straas.io/base/errors"
	bp "straas.io/base/polling"
	"straas.io/external"
	"straas.io/external/rtc"
	"straas.io/middle/config"
	"straas.io/middle/domain"
	"straas.io/middle/logger"

	"github.com/antonholmquist/jason"
)

const (
	vodUploadCompleted      = 1
	minDurationForGenMp4Sec = 120
)

// ReqBridgerOp interfaces operations by ReqBridger
type ReqBridgerOp interface {
	Transcoded(host string, req []domain.TranscodedReq) (string, error)
}

// ReqCMSOp interfaces operations by ReqCMS
type ReqCMSOp interface {
	UpdateVideo(accountID, videoID string, payload interface{}) (string, error)
}

// VodStatusPollableImpl queries RTC periodically for Vod status
type VodStatusPollableImpl struct {
	completed bool

	host      string
	accountID string
	ownerID   string
	videoID   string
	gID       string

	*config.AppContext `inject:""`
	ReqBridgerOp       `inject:"polling.VodStatusPollableImpl.ReqBridgerOp"`
	ReqCMSOp           `inject:"polling.VodStatusPollableImpl.ReqCMSOp"`

	external.RTC
}

// Setup sets up FetchPremiumPollableImpl
func (vsp *VodStatusPollableImpl) Setup() {
	logger.Debug("VodStatusPollableImpl setup ...")

	vsp.RTC = rtc.NewRTC(logger.GetLogger())
}

// New implements New method of Pollable interface
func (vsp *VodStatusPollableImpl) New(args ...interface{}) bp.Pollable {
	return &VodStatusPollableImpl{
		completed:  false,
		host:       args[0].(string),
		accountID:  args[1].(string),
		ownerID:    args[2].(string),
		videoID:    args[3].(string),
		gID:        args[4].(string),
		AppContext: vsp.AppContext,
	}
}

// Query implements Query method of Pollable interface
func (vsp *VodStatusPollableImpl) Query() (interface{}, error) {
	resp, err := vsp.LiveStatus(vsp.host, vsp.ownerID, vsp.gID)
	if err != nil {
		return "", err
	}

	return resp, nil
}

// Derive implements Derive method of Pollable interface
func (vsp *VodStatusPollableImpl) Derive(response interface{}) map[interface{}]int {
	events := make(map[interface{}]int)

	r := response.(string)
	if isUploadCompleted(r) {
		events[r] = vodUploadCompleted
	}

	return events
}

// EventHandler implements EventHandler method of Pollable interface
func (vsp *VodStatusPollableImpl) EventHandler(event int, context interface{}) error {
	resp := context.(string)

	_, err := vsp.UpdateVideo(
		vsp.accountID, vsp.videoID, getUpdateVodReq(resp),
	)
	if err != nil {
		return err
	}

	err = vsp.GenMp4(vsp.BridgerHost, resp)
	if err != nil {
		logger.Warnf("err: %s", err)
	}

	vsp.completed = true
	return nil
}

// Completed implements Completed method of Pollable interface
func (vsp *VodStatusPollableImpl) Completed() bool {
	return vsp.completed
}

// Result implements Result method of Pollable interface
func (vsp *VodStatusPollableImpl) Result() (interface{}, error) {
	return nil, nil
}

// GenMp4 calls bridger to generate MP4 file for download
func (vsp *VodStatusPollableImpl) GenMp4(host, resp string) error {
	dur := getDuration(resp)
	if dur < int64(minDurationForGenMp4Sec) {
		logger.Debugf("Short duration, not generate Mp4: resp[%s]", resp)
		return nil
	}

	req := []domain.TranscodedReq{
		domain.TranscodedReq{
			VideoID:  vsp.videoID,
			FileType: "playlist",
			FilePath: buildSrcPathOfPlaylistConv(
				getVodDestination(resp), getStreamURL(resp),
			),
		},
	}

	logger.Debug("Call bridger to generate mp4")

	_, err := vsp.Transcoded(host, req)
	if err != nil {
		return errors.Wrap(err, "GenMp4 fails")
	}

	return nil
}

func getVodDestination(resp string) string {
	v, _ := jason.NewValueFromBytes([]byte(resp))
	arr, _ := v.Array()

	obj, _ := arr[0].Object()
	vd, _ := obj.GetString("vodDestination")

	return vd
}

func getDuration(resp string) int64 {
	v, _ := jason.NewValueFromBytes([]byte(resp))
	arr, _ := v.Array()

	obj, _ := arr[0].Object()
	dur, _ := obj.GetInt64("convResultInfo", "duration")

	return dur
}

func getStreamURL(resp string) string {
	v, _ := jason.NewValueFromBytes([]byte(resp))
	arr, _ := v.Array()

	obj, _ := arr[0].Object()
	su, _ := obj.GetString("convResultInfo", "stream_url")

	return su
}

func isUploadCompleted(resp string) bool {
	v, _ := jason.NewValueFromBytes([]byte(resp))

	arr, _ := v.Array()
	if len(arr) < 1 {
		return false
	}

	obj, _ := arr[0].Object()
	status, err := obj.GetString("status")
	if err != nil {
		return false
	}

	if status == "completed" {
		return true
	}

	return false
}

func getUpdateVodReq(resp string) []byte {
	v, _ := jason.NewValueFromBytes([]byte(resp))

	arr, _ := v.Array()
	obj, _ := arr[0].Object()

	convResult, _ := obj.GetObject("convResultInfo")
	req, _ := convResult.MarshalJSON()
	return req
}

func buildSrcPathOfPlaylistConv(vodDest, streamURL string) string {
	result := strings.Split(vodDest, "/")

	spls := strings.Split(streamURL, "/")
	// append tail part of streamURL to vodDest
	for i := len(result); i < len(spls); i++ {
		result = append(result, spls[i])
	}

	return strings.Join(result, "/")
}
