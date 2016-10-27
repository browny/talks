// Package rest represents the REST layer
package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"straas.io/base/errors"
	bp "straas.io/base/polling"
	"straas.io/base/rest"
	"straas.io/bridger/interfaces/requester"
	"straas.io/external"
	"straas.io/middle/config"
	"straas.io/middle/domain"
	"straas.io/middle/endpoint"
	"straas.io/middle/logger"
	mw "straas.io/middle/middleware"
	"straas.io/middle/polling"
	"straas.io/middle/usecases/dao"

	"github.com/antonholmquist/jason"
	"github.com/bluele/slack"
	"github.com/gorilla/mux"
	"github.com/iKala/gogoo"
	"github.com/iKala/gosak/collectionutil"
	"github.com/iKala/gosak/flowutil"
	"github.com/iKala/gosak/formatutil"
	"github.com/iKala/gosak/timeutil"
	"github.com/iKala/gotools/etcd"
	"github.com/unrolled/render"
)

const (
	okRtmp               = "0"
	errRtmpFailStreamKey = "-1"

	// RtmpPublish
	errRtmpNoAvailableInstance = "-2"
	errRtmpNotPaired           = "-4"
	errRtmpFailStateChange     = "-5"
	errRtmpConcurrentServing   = "-6"
	errRtmpNotFoundServing     = "-7"
	errRtmpExpiredKey          = "-8"

	// RtmpUnpublish
	errRtmpNoSessionInstance   = "-2"
	errRtmpFailStopRtmp        = "-3"
	errRtmpFailUpdateVodRecord = "-4"

	// RtmpPlay
	errRtmpBadSourceIP = "-2"

	// Pub/Unpub notified slack room
	slackNotifiedRoom = "kb-notify"

	rtcPort           = "8000"
	rtmpPlayWhitelist = "rtmp_play_white_list"
	pierceOwnerID     = "pierceOwnerId"
)

// GetVersion gets the version info from query parameter `version`
func GetVersion(req *http.Request) int {
	version := req.URL.Query().Get("version")
	if version == "" {
		return 0
	}
	numOfVersion, err := strconv.Atoi(version)
	if err != nil {
		return 0
	}

	return numOfVersion
}

// RTMPStreamInfo represents single entry of response from RTMP stream info API
type RTMPStreamInfo struct {
	ServerIP  string `json:"serverIp"`
	Channel   string `json:"channel"`
	Key       string `json:"key"`
	PublishIP string `json:"publishIp"`
	Bitrate   int    `json:"bitrate"`
	Play      bool   `json:"play"`
	PlayIP    string `json:"playIp"`
}

// IsQualified implements `Qualifiable` interface for `GetQualifiedItems`
func (r RTMPStreamInfo) IsQualified(params ...interface{}) bool {
	inputKey := params[0].(string)
	if r.Key == inputKey {
		return true
	}

	return false
}

// HandlerWrapper manages all http error handling
type HandlerWrapper func(http.ResponseWriter, *http.Request) *rest.Error

func (fn HandlerWrapper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if restErr := fn(w, r); restErr != nil {
		if restErr.InternalError != nil {
			logger.Warnf("[rest] err: %s", restErr.InternalError)
		}
		http.Error(w, restErr.ExternalError.Error(), restErr.Code)
	}
}

// jsonResponse as json response type
type jsonResponse map[string]interface{}

func (res jsonResponse) String() string {
	b, err := json.Marshal(res)
	if err != nil {
		return ""
	}
	return string(b)
}

// StreamDaoOp interfaces operations by StreamDao
type StreamDaoOp interface {
	Ensure(in domain.Stream) (*domain.Stream, error)
	Renew(in domain.Stream) (*domain.Stream, error)
	DeleteByOwner(owner string) error
	FindByKey(key string) (*domain.Stream, error)
	Update(key string, in domain.Stream) (*domain.Stream, error)
	GetWhitelistIPs(owner string) []string
}

// RtmpGroupOp interfaces operations by RtmpGroup
type RtmpGroupOp interface {
	Get(channel string) string
	Toggle(channel string) error
	Delete(channel string) error
}

// Handler includes all http handler methods
type Handler struct {
	*config.AppContext             `inject:""`
	*endpoint.Manager              `inject:""`
	*gogoo.GoGoo                   `inject:""`
	*mw.InstanceManager            `inject:""`
	*mw.KeyValueManager            `inject:""`
	*mw.SessionManager             `inject:""`
	*polling.VodStatusPollableImpl `inject:""`
	*render.Render                 `inject:""`
	*requester.ReqCMS              `inject:""`

	StreamDaoOp         `inject:"rest.Handler.StreamDaoOp"`
	dao.StreamValidator `inject:"rest.Handler.StreamValidator"`
	external.RTC        `inject:"rest.Handler.RTC"`
	RtmpGroupOp         `inject:"rest.Handler.RtmpGroupOp"`

	Etcd *etcd.Wrapper
}

// Setup sets up Handler
func (h *Handler) Setup() {
	logger.Debug("REST Handler setup ...")

	h.Etcd = etcd.NewWrapper(h.EtcdServers)
}

// HealthCheck is called by Google cloud to do health check
func (h *Handler) HealthCheck(w http.ResponseWriter, req *http.Request) *rest.Error {
	fmt.Fprintf(w, "OK")
	return nil
}

// ComputeEngineHealthCheck is called by google cloud monitoring to check compute engine api health
func (h *Handler) ComputeEngineHealthCheck(
	w http.ResponseWriter, req *http.Request,
) *rest.Error {

	_, err := h.InstanceManager.Gce.ListSnapshots(h.AppContext.ProjectID)
	if err != nil {
		return &rest.Error{
			ExternalError: errors.Errorf("compute engine health check error"),
			Code:          http.StatusInternalServerError,
		}
	}

	fmt.Fprintf(w, "OK")
	return nil
}

// GetServerAddressByChannel gets IP address of RTC server by channel
func (h *Handler) GetServerAddressByChannel(w http.ResponseWriter, req *http.Request) *rest.Error {
	vars := mux.Vars(req)
	channelID := vars["channel_id"]

	// Get session by channel
	serving := h.SessionManager.GetServingSession(channelID)
	if serving == nil {
		logger.Tracef("No session found by channel: channel[%s], from[%s]", channelID, req.RemoteAddr)
		fmt.Fprintf(w, "{\"error\":404,\"reason\":\"No active session: '%s'\"}", channelID)

		return nil
	}

	// Get server internal address
	internalIP := serving.IP
	if !formatutil.IsIP(internalIP) {
		vm, err := h.InstanceManager.GetVM(serving.Key.Name())
		if err != nil {
			logger.Debugf("Fail get ip from vm: channel[%s], vm[%s]", channelID, serving.Key.Name())
			fmt.Fprintf(w, "{\"error\":404,\"reason\":\"Fail get ip from vm: '%s'\"}", channelID)
			return nil
		}
		internalIP = h.InstanceManager.Gce.GetNetworkIP(vm)

		if !formatutil.IsIP(internalIP) {
			logger.Debugf("Invalid ip: channel[%s], ip[%s]", channelID, internalIP)
			fmt.Fprintf(w, "{\"error\":404,\"reason\":\"Invalid ip: '%s'\"}", channelID)
			return nil
		}
	}

	fmt.Fprint(w, jsonResponse{"internalserver": internalIP, "port": rtcPort})

	return nil
}

// ListLiveChannels lists all on air channels
func (h *Handler) ListLiveChannels(w http.ResponseWriter, req *http.Request) *rest.Error {
	channels, err := h.SessionManager.ListLiveChannels()
	if err != nil {
		return &rest.Error{
			ExternalError: err,
			Code:          http.StatusInternalServerError,
		}
	}

	data := struct {
		ReturnCode int      `json:"returnCode"`
		Channels   []string `json:"channels"`
	}{
		0,
		channels,
	}

	h.Render.JSON(w, http.StatusOK, data)
	return nil
}

// ListSessions lists all sessions
func (h *Handler) ListSessions(w http.ResponseWriter, req *http.Request) *rest.Error {
	hash := req.URL.Query().Get("hash")
	rtmpIP := req.URL.Query().Get("rtmp-ip")

	if hash != "" && rtmpIP != "" {
		session := h.SessionManager.GetSessionByHashAndRtmpSource(hash, rtmpIP)
		if session == nil {
			return &rest.Error{
				ExternalError: errors.Errorf("session not found"),
				Code:          http.StatusNotFound,
			}
		}
		err := json.NewEncoder(w).Encode(session)
		if err != nil {
			return &rest.Error{
				ExternalError: errors.Errorf("json encode error"),
				Code:          http.StatusInternalServerError,
			}
		}
		return nil
	}

	sessions := h.SessionManager.ListAllSessions()
	err := json.NewEncoder(w).Encode(sessions)
	if err != nil {
		return &rest.Error{
			ExternalError: errors.Errorf("json encode error"),
			Code:          http.StatusInternalServerError,
		}
	}

	return nil
}

// GetStreamKey gets stream key
func (h *Handler) GetStreamKey(w http.ResponseWriter, req *http.Request) *rest.Error {
	var owner string
	if owner = req.URL.Query().Get("owner"); owner == "" {
		//TODO deprecate cid
		owner = req.URL.Query().Get("cid")
	}

	if owner == "" {
		return &rest.Error{
			ExternalError: errors.Errorf("owner of stream not found"),
			Code:          http.StatusBadRequest,
		}
	}

	cmsAccount := req.URL.Query().Get("cms_account")
	life, err := strconv.Atoi(req.URL.Query().Get("life"))
	if err != nil {
		life = 0
	}

	renew := req.URL.Query().Get("renew")
	prohibit := req.URL.Query().Get("prohibit")

	sp := domain.Stream{
		Channel:    owner,
		Owner:      owner,
		CMSAccount: cmsAccount,
		Life:       life,
	}

	var writeResponse = func(w http.ResponseWriter, s *domain.Stream) {
		h.Render.JSON(w, http.StatusOK, jsonResponse{
			"channel":         s.Channel,
			"owner":           s.Owner,
			"cms_account":     s.CMSAccount,
			"life":            s.Life,
			"first_used_time": s.FirstUsedTime,
			"streamkey":       s.Key.Name(),
		})
	}

	// When renew the stream key, we drop rtmp to drop RTMP connection then
	// stop RTC transcoding.
	if renew == "1" {
		if prohibit == "1" {
			logger.Infof("The owner is prohibited: owner[%s]", owner)
		}

		existedSessions := h.SessionManager.GetSessionsByChannel(owner)
		for _, s := range existedSessions {
			h.Manager.DropRtmpSession(s.SourceIP, s.Hash, owner)

			event := endpoint.RtmpEvent{
				Stream:        s.Hash,
				ServerAddress: s.SourceIP,
			}
			copiedW := w
			h.RtmpUnpublish(copiedW, &event)
		}

		s, err := h.StreamDaoOp.Renew(sp)
		if err != nil {
			logger.Debugf("Fail RenewStreamByChannelId: err[%s]", err)
		} else {
			writeResponse(w, s)
		}

		return nil
	}

	s, err := h.StreamDaoOp.Ensure(sp)
	if err != nil {
		return &rest.Error{
			ExternalError: errors.Errorf("ensure stream error"),
			InternalError: err,
			Code:          http.StatusInternalServerError,
		}
	}
	writeResponse(w, s)

	return nil
}

// DeleteStream deletes stream
func (h *Handler) DeleteStream(w http.ResponseWriter, req *http.Request) *rest.Error {
	vars := mux.Vars(req)
	owner := vars["owner_id"]

	err := h.StreamDaoOp.DeleteByOwner(owner)
	if err != nil {
		return &rest.Error{
			ExternalError: err,
			Code:          http.StatusInternalServerError,
		}
	}

	fmt.Fprintf(w, "OK")
	return nil
}

// ProcessRtmpEvent processes the callback issued from RTMP server
func (h *Handler) ProcessRtmpEvent(w http.ResponseWriter, req *http.Request) *rest.Error {
	decoder := json.NewDecoder(req.Body)
	event := &endpoint.RtmpEvent{}
	err := decoder.Decode(&event)
	if err != nil {
		return &rest.Error{
			ExternalError: errors.Errorf("fail to decode request"),
			Code:          http.StatusBadRequest,
		}
	}

	switch event.Action {
	case "on_publish":
		h.RtmpPublish(w, event)
	case "on_unpublish":
		h.RtmpUnpublish(w, event)
	case "on_play":
		h.RtmpPlay(w, event)
	case "on_stop":
		h.RtmpStop(w, event)
	case "on_close":
	default:
		// do nothing
	}

	return nil
}

// isNotPairedPublish checks not paired publish by query `/streamInfo` api of RTMP manager.
// If the number of connection for some stream key > 1, means the publish is not paired one.
func (h *Handler) isNotPairedPublish(inputKey string) bool {
	// Get all stream info from RTMP manager
	body, err := h.Manager.RtmpStreamInfo(nil)
	if err != nil {
		logger.Debugf("Fail RtmpStreamInfo: err[%s]", err)
		return false
	}

	count := h.GetCountOfRTMPStreamInfoByKey(body, inputKey)

	if count > 1 {
		return true
	}
	return false
}

// This func judge if the incoming RTMP publish should be looked as `serving`.
// 1. It decides if the source RTMP IP is from the main group (by `isFromRtmpMainGroup`)
// 2. Then it check the meaning the main group stands for.
// When the value are "" or "serving", be in the main group represents it should be looked as `serving`.
// When the value is "backup", be in the main group represents it should be looked as `backup`.
func (h *Handler) checkIsServing(channel, rtmpHostIP string) bool {
	value := h.RtmpGroupOp.Get(channel)

	if value == "serving" || value == "" {
		return h.isFromRtmpMainGroup(rtmpHostIP)
	} else if value == "backup" {
		return !h.isFromRtmpMainGroup(rtmpHostIP)
	}

	return true
}

func (h *Handler) isFromRtmpMainGroup(rtmpHostIP string) bool {
	if h.isFromWowzaServer(rtmpHostIP) {
		return true
	}

	body, err := h.Manager.RtmpServerInfo()
	if err != nil {
		logger.Debugf("Fail RtmpServerInfo: err[%s]", err)
		return true
	}

	rtmpGroup := endpoint.GetRtmpMainGroup(body)
	if collectionutil.InStringSlice(rtmpGroup, rtmpHostIP) {
		return true
	}

	return false
}

// RtmpPublish processes the publish event of RTMP
func (h *Handler) RtmpPublish(w http.ResponseWriter, event *endpoint.RtmpEvent) {
	stream, err := h.StreamDaoOp.FindByKey(event.Stream)
	if err != nil {
		fmt.Fprintf(w, errRtmpFailStreamKey)
		return
	}

	if !h.StreamValidator.ValidateForRtmpPublish(*stream, time.Now()) {
		fmt.Fprintf(w, errRtmpExpiredKey)
		return
	}

	defer timeutil.TimeTrack(time.Now(), fmt.Sprintf("RtmpPublish channel[%s]", stream.Owner))

	rtmpHostIP := event.ServerAddress
	go func() {
		// Slack notification
		if collectionutil.InStringSlice(mw.SlackNotifiedChannels, stream.Owner) {
			postSlackMessage(
				h.Etcd.GetValue(h.AppContext.KeyMapOfEtcd["slack_token"].(string)),
				slackNotifiedRoom,
				fmt.Sprintf("Publish: env[%s], channel[%s], rtmp[%s], url[%s]",
					h.Env,
					stream.Owner,
					rtmpHostIP,
					fmt.Sprintf("%s/channel/%s", h.AppContext.HostName, stream.Owner)))
		}
	}()

	// Not paired publish checking
	if h.isNotPairedPublish(event.Stream) {
		logger.Warnf("Not paired RTMP event: channel[%s]", stream.Owner)
		fmt.Fprintf(w, errRtmpNotPaired)
		return
	}
	// :~)

	isServing := h.checkIsServing(stream.Owner, rtmpHostIP)

	// Prevent multiple serving sessions (concurrent publish)
	if isServing {
		lock := fmt.Sprintf("%s-%t", stream.Owner, isServing)
		err := h.SessionManager.Lock(lock)
		if err != nil {
			fmt.Fprintf(w, errRtmpConcurrentServing)
			return
		}
		defer h.SessionManager.Unlock(lock)
	}
	// :~)

	isPremium := h.isPremium(stream.Owner)

	logger.Infof("RTMP Publish: serving[%t], channel[%s], premium[%t], hash[%s], source[%s], rtmp[%s]",
		isServing, stream.Owner, isPremium, event.Stream, event.IP, rtmpHostIP)

	// Get available instance
	strategy := ""
	if !isServing {
		strategy = "spareOnly"
	}

	var outputMode string
	var machineType string
	if isPremium {
		machineType = mw.PremiumMachineType
		outputMode = mw.RtcOutputTranscode
	} else {
		machineType = mw.DefaultMachineType
		outputMode = mw.RtcOutputRemux
	}

	instance := h.InstanceManager.GetAvailableInstance(stream.Owner, strategy, machineType)
	if instance == nil {
		// There is no available instance, return first then create instance
		fmt.Fprintf(w, errRtmpNoAvailableInstance)

		go h.InstanceManager.HouseKeep(
			h.SessionManager.GetSafeSpareInstanceCount(
				h.SessionManager.GetSessionCount), machineType)
		return
	}

	instanceName := instance.Key.Name()

	// Try to aquire the instance lock (maybe held by `SleepInstance`)
	err = h.InstanceManager.DisLock(instanceName)
	if err != nil {
		logger.Debugf("Instance lock is held: instance[%s]", instanceName)
		fmt.Fprintf(w, errRtmpNoAvailableInstance)
		return
	}
	// :~)

	logger.Infof("Got available instance: channel[%s], instance[%s], machine[%s]",
		stream.Owner, instanceName, instance.MachineType)

	event.Valid = true

	// Record instance & session entity
	newSession := &mw.Session{
		Key:       h.SessionManager.BuildKey(instanceName),
		Hash:      event.Stream,
		Channel:   stream.Owner,
		Time:      timeutil.TimeToEpoch(time.Now()),
		IP:        instance.IP,
		Protocol:  "rtmp",
		IsServing: isServing,
		SourceIP:  rtmpHostIP}

	// Command RTC server to pull stream from RTMP server
	rtmpURL := fmt.Sprintf("rtmp://%s/app/%s", rtmpHostIP, event.Stream)
	vodEnabled := true
	if collectionutil.InStringSlice(mw.VodDisabledChannels, stream.Owner) {
		vodEnabled = false
	}

	if isServing {
		m := map[string]interface{}{
			pierceOwnerID: strings.Join(
				collectionutil.TrimEmptyString([]string{stream.CMSAccount, stream.Owner}),
				"=")}
		hook := formatutil.JSONMapToString(m)

		ctx := domain.SessionCtx{
			Channel:      stream.Owner,
			Account:      stream.CMSAccount,
			ServingState: domain.Single,
			VODDestination: strings.TrimSuffix(
				h.AppContext.VODDestinationBucket+"/"+stream.CMSAccount, "/",
			),
			ThumbDestination: strings.TrimSuffix(
				h.AppContext.ThumbDestinationBucket+"/"+stream.CMSAccount, "/",
			),
			RtmpURL:           rtmpURL,
			OutputMode:        outputMode,
			IsVodEnabled:      vodEnabled,
			NotificationHooks: string(hook),
			TsLiveDuration:    h.TsLiveDuration,
		}

		go h.Manager.StartPullFromRtmp(instance.IP, ctx)
	} else {
		// Setup backup session
		// 1. Fetch the serving session context
		// 2. Use the context to command backup RTC to pull stream
		// 3. Notify the serving instance, its serving session changes to SERVING_WITH_BACKUP
		serving := h.SessionManager.GetServingSession(stream.Owner)
		if serving == nil {
			logger.Warnf("Backup but not found serving: channel[%s]", stream.Owner)
			fmt.Fprintf(w, errRtmpNotFoundServing)
			return
		}

		monitor := func(ip string) (string, error) {
			return h.Monitoring(ip)
		}
		ctx, _ := h.SessionManager.BuildContext(serving, monitor, domain.Backup)
		ctx.RtmpURL = rtmpURL
		//TODO below info should be fetched from /monitoring API
		ctx.VODDestination = strings.TrimSuffix(
			h.AppContext.VODDestinationBucket+"/"+stream.CMSAccount, "/",
		)
		ctx.ThumbDestination = strings.TrimSuffix(
			h.AppContext.ThumbDestinationBucket+"/"+stream.CMSAccount, "/",
		)
		ctx.OutputMode = outputMode
		ctx.IsVodEnabled = vodEnabled
		ctx.NotificationHooks = stream.CMSAccount + "=" + stream.Owner
		ctx.TsLiveDuration = h.TsLiveDuration
		// :~)

		go h.Manager.StartPullFromRtmp(instance.IP, ctx)

		if err := h.Manager.ServingStateChange(
			serving.IP,
			domain.SessionCtx{Channel: serving.Channel, ServingState: domain.Master}); err != nil {

			fmt.Fprintf(w, errRtmpFailStateChange)
			return
		}
	}
	// :~)

	go func() {
		defer h.InstanceManager.Unlock(instanceName)

		// check if RTC is successfully started by /startPullFromRtmp call
		checkTranscodingStarted := func() bool {
			return h.InstanceManager.IsTranscodingStarted(newSession.Key.Name(), newSession.Channel)
		}
		started := flowutil.RunUntilSuccess(3, checkTranscodingStarted, 3*time.Second)
		if !started {
			logger.Debugf("Don't allocate session, RTC doesn't run: channel[%s]", newSession.Channel)
			// Tell RTMP to drop the connection
			if err := h.Manager.DropRtmpSession(rtmpHostIP, event.Stream, stream.Owner); err != nil {
				logger.Warnf("DropRtmpSession fails: %s", err)
			}
			return
		}
		// :~)

		runnable := func() bool {
			err := h.SessionManager.AllocateSession(newSession, h.InstanceManager.Attach)
			if err != nil {
				logger.Warnf("err: %s", err)
				return false
			}

			return true
		}
		success := flowutil.RunUntilSuccess(3, runnable, 1*time.Second)

		if !success {
			// Tell RTC stop pull from RTMP
			vodStitching := 0
			if isServing {
				vodStitching = 1
			}
			h.Manager.StopPullFromRtmp(instance.IP, stream.Owner, vodStitching)

			// Tell RTMP to drop the connection
			err := h.Manager.DropRtmpSession(rtmpHostIP, event.Stream, stream.Owner)
			if err != nil {
				logger.Warnf("DropRtmpSession fails: %s", err)
			}

			return
		}

		h.updateThumbnail(newSession.Key.Name(), stream.CMSAccount, stream.Owner)

		// Record 1st publish
		if stream.FirstUsedTime == 0 {
			stream.FirstUsedTime = timeutil.TimeToEpoch(time.Now())
			h.StreamDaoOp.Update(stream.Key.Name(), *stream)
		}
	}()

	fmt.Fprintf(w, okRtmp)
	return
}

// TODO make LH follow up
func (h *Handler) updateThumbnail(host, account, owner string) {
	if !h.IsStraas() {
		return
	}

	runnable := func() bool {
		r, err := h.Monitoring(host)
		if err != nil {
			logger.Warnf("err: %s", err)
			return false
		}

		// get live status from RTC
		gid, _ := h.GetLiveGID(r)
		resp, err := h.LiveStatus(host, owner, gid)
		if err != nil {
			logger.Warnf("err: %s", err)
			return false
		}

		// update thumbnail to CMS
		payload := getUpdateLiveReq(resp)
		_, err = h.ReqCMS.UpdateLiveInfo(account, owner, payload)
		if err != nil {
			logger.Warnf("err: %s", err)
			return false
		}

		logger.Debugf("Thumbnail updated")
		return true
	}

	flowutil.RunUntilSuccess(3, runnable, 1*time.Second)
}

// RtmpUnpublish processes unpublish event of RTMP
func (h *Handler) RtmpUnpublish(w http.ResponseWriter, event *endpoint.RtmpEvent) {
	stream, err := h.StreamDaoOp.FindByKey(event.Stream)
	if err != nil {
		fmt.Fprintf(w, errRtmpFailStreamKey)
		return
	}

	rtmpHostIP := event.ServerAddress

	// Get session
	session := h.SessionManager.GetSessionByRtmpSource(stream.Channel, rtmpHostIP)
	if session == nil {
		fmt.Fprintf(w, errRtmpNoSessionInstance)
		return
	}

	// Slack notification
	if collectionutil.InStringSlice(mw.SlackNotifiedChannels, stream.Channel) {
		go postSlackMessage(
			h.Etcd.GetValue("/middle/slack/token"),
			slackNotifiedRoom,
			fmt.Sprintf("Unpublish: env[%s], channel[%s], rtmp[%s], url[%s]",
				h.Env,
				stream.Channel,
				rtmpHostIP,
				fmt.Sprintf("%s/channel/%s", h.AppContext.HostName, stream.Channel)))
	}

	logger.Infof("RTMP Unpublish: serving[%t], channel[%s], hash[%s], source[%s], rtmp[%s]",
		session.IsServing, session.Channel, session.Hash, event.IP, rtmpHostIP)
	event.Valid = true

	vodStitching := 0
	if session.IsServing {
		err := h.SessionManager.ExchangeRoles(session.Channel, h.SessionManager.SetRole)
		if err == nil {
			h.RtmpGroupOp.Toggle(stream.Channel)
		} else {
			vodStitching = 1
			h.RtmpGroupOp.Delete(stream.Channel)
		}
	} else {
		// protect concurrent unpub both serv and back
		lock := fmt.Sprintf("%s-%t", session.Channel, session.IsServing)
		err := h.SessionManager.Lock(lock)
		if err != nil {
			// this unpub of back session is stopped bcz lock acquire lost.
			// it will cause RTC still try to pull the stopped RTMP.
			// it will be released until cron job of middle called `ReleaseBrokenSessions`.
			logger.Warnf("lock session fails: lock[%s]", lock)
			return
		}
		defer h.SessionManager.Unlock(lock)
		// :~)
	}

	resp, err := h.Manager.StopPullFromRtmp(session.IP, session.Channel, vodStitching)
	if err != nil {
		logger.Debugf("Fail StopPullFromRtmp: err[%s]", err)
		fmt.Fprintf(w, errRtmpFailStopRtmp)
		return
	}

	// Polling RTC until the VOD has been uploaded
	if h.IsStraas() {
		go func() {
			host := session.Key.Name()
			gid, err := h.Manager.GetLiveGID(resp)
			if err != nil {
				fmt.Fprintf(w, errRtmpFailUpdateVodRecord)
				return
			}

			// acquire a videoID from CMS
			videoID, err := h.ReqCMS.AskVideoID(
				stream.CMSAccount, external.CreateVideoReq{LiveID: stream.Owner})
			if err != nil {
				logger.Warnf("err :%s", err)
				return
			}

			// start polling RTC
			pollable := h.VodStatusPollableImpl.New(
				host, stream.CMSAccount, stream.Owner, videoID, gid)
			dp := bp.DefaultPoller{
				Interval:       30 * time.Second,
				MaxElapsedTime: 30 * time.Minute,
				Pollable:       pollable,
				Logger:         logger.GetLogger(),
			}

			dp.Poll()
		}()
	}

	// Release session
	runnable := func() bool {
		return h.SessionManager.ReleaseSession(session, h.InstanceManager.Detach)
	}
	flowutil.RunUntilSuccess(3, runnable, 5*time.Second)

	fmt.Fprintf(w, okRtmp)
	return
}

// RtmpPlay checks if the ip is allowed to pull stream from RTMP server
func (h *Handler) RtmpPlay(w http.ResponseWriter, event *endpoint.RtmpEvent) {
	key, ok := h.ValidateForRtmpPlay(event.Stream, time.Now())
	if !ok {
		fmt.Fprintf(w, "-1")
		return
	}

	stream, err := h.StreamDaoOp.FindByKey(key)
	if err != nil {
		fmt.Fprintf(w, errRtmpFailStreamKey)
		return
	}

	// Check the play is permitted
	// Check order: global whitelist -> channel whitelist -> GCE internal network
	start := h.AppContext.InstanceIPRangeStart
	end := h.AppContext.InstanceIPRangeEnd
	inGlobalWhiteList := collectionutil.InStringSlice(h.CommonConfig.GetStringSlice(rtmpPlayWhitelist), event.IP)
	inChannelWhiteList := collectionutil.InStringSlice(h.StreamDaoOp.GetWhitelistIPs(stream.Owner), event.IP)
	inInternalNetwork := formatutil.CheckIPInRange(event.IP, net.ParseIP(start), net.ParseIP(end))

	if inGlobalWhiteList || inChannelWhiteList || inInternalNetwork {
		logger.Infof("RTMP Play: channel[%s], hash[%s], source[%s], rtmp[%s]",
			stream.Channel, event.Stream, event.IP, event.ServerAddress)

		fmt.Fprintf(w, okRtmp)
		return
	}

	// The source IP is not allowed
	fmt.Fprintf(w, errRtmpBadSourceIP)
	return
}

// RtmpStop processes stop event of RTMP
func (h *Handler) RtmpStop(w http.ResponseWriter, event *endpoint.RtmpEvent) {
	stream, err := h.StreamDaoOp.FindByKey(event.Stream)
	if err != nil {
		fmt.Fprintf(w, errRtmpFailStreamKey)
		return
	}

	rtmpHostIP := event.ServerAddress

	// Get session
	session := h.SessionManager.GetSessionByRtmpSource(stream.Channel, rtmpHostIP)
	if session == nil {
		fmt.Fprintf(w, errRtmpNoSessionInstance)
		return
	}

	if event.IP != session.IP {
		logger.Debugf("RtmpStop NOT triggered by RTC: channel[%s], source[%s]", session.Channel, session.IP)
		return
	}

	logger.Infof("RTMP Stop: channel[%s], hash[%s], source[%s], rtmp[%s]",
		session.Channel, session.Hash, session.IP, rtmpHostIP)
	event.Valid = true

	fmt.Fprintf(w, okRtmp)
	return
}

// Broadcast handles RTC broadcast from mobile. preserve all vods currently
func (h *Handler) Broadcast(w http.ResponseWriter, req *http.Request) *rest.Error {
	defer timeutil.TimeTrack(time.Now(), fmt.Sprintf("RTC Broadcast"))

	vars := mux.Vars(req)
	sessionID := vars["session_id"]
	channelID := req.URL.Query().Get("file_name")
	channelType := req.URL.Query().Get("type")

	values := req.URL.Query()
	values.Add("thumbnailDestination", h.AppContext.ThumbDestinationBucket)
	values.Add("vodDestination", h.AppContext.VODDestinationBucket)
	values.Add("isVodEnabled", "true")
	values.Add("highestResolution", h.Manager.GetResolutionLimit("", channelID))
	values.Add("outputMode", "transcode")
	req.URL.RawQuery = values.Encode()

	// validation
	if channelID == "" || channelType == "" {
		return &rest.Error{
			ExternalError: errors.Errorf("invalid parameters"),
			Code:          http.StatusBadRequest,
		}
	}

	var instanceName, protocol, host string
	var instance *mw.Instance

	logger.Infof("RTC broadcast: channel[%s]", channelID)

	// Clean existed session (due to abnormal termination)
	h.SessionManager.ReleaseSessionsByChannel(channelID, h.InstanceManager.Detach)

	instance = h.InstanceManager.GetAvailableInstance(channelID, "", mw.PremiumMachineType)
	if instance == nil {
		// There is no available instance, return first then create instance
		fmt.Fprint(w, jsonResponse{
			"return_code": 101,
			"error": jsonResponse{
				"reason": "RTC server not yet ready"}})

		go h.InstanceManager.HouseKeep(
			h.SessionManager.GetSafeSpareInstanceCount(
				h.SessionManager.GetSessionCount), mw.PremiumMachineType)

		return nil
	}

	instanceName = instance.Key.Name()

	// Try to aquire the instance lock (maybe held by `SleepInstance`)
	err := h.InstanceManager.DisLock(instanceName)
	if err != nil {
		logger.Debugf("Instance lock is held: instance[%s]", instanceName)
		fmt.Fprintf(w, errRtmpNoAvailableInstance)
		return nil
	}
	defer h.InstanceManager.Unlock(instanceName)
	// :~)

	logger.Infof("Get available instance: channel[%s], instance[%s]", channelID, instanceName)

	protocol = "rtc"
	ip := instance.IP
	host = fmt.Sprintf("%s:%s", ip, rtcPort)

	// Relay request
	res, body, err := h.Manager.RelayRequestToRtc(host, "http", req)
	if err != nil {
		fmt.Fprint(w, jsonResponse{
			"return_code": -1,
			"error": jsonResponse{
				"reason": err}})

		return nil
	}

	var resMap map[string]interface{}
	if res.StatusCode == http.StatusOK {
		resMap, _ = formatutil.JSONStringToMap(body)

		id, ok := resMap["id"]
		if !ok || id == "-1" {
			logger.Warnf("Bad response of broadcast")

			fmt.Fprint(w, jsonResponse{
				"return_code": 401,
				"error": jsonResponse{
					"reason": "RTC server return id == -1"}})

			return nil
		}

		// Streamming server response correctly
		resMap["return_code"] = 0

		newSession := &mw.Session{
			Key:       h.SessionManager.BuildKey(instanceName),
			Hash:      sessionID,
			Channel:   channelID,
			Time:      timeutil.TimeToEpoch(time.Now()),
			IP:        ip,
			Protocol:  protocol,
			IsServing: true}

		// Allocate session
		err := h.SessionManager.AllocateSession(
			newSession,
			h.InstanceManager.Attach)
		if err != nil {
			// If allocate session fails, call RTC server terminate API
			h.Manager.Terminate(host, sessionID, channelID, "audio_and_video")
		}
	}

	copyHeader(w.Header(), res.Header)
	w.Write(formatutil.JSONMapToString(resMap))
	w.WriteHeader(res.StatusCode)

	return nil
}

// Terminate handles RTC termination
func (h *Handler) Terminate(w http.ResponseWriter, req *http.Request) *rest.Error {
	sessionID := req.URL.Query().Get("gid")
	channelID := req.URL.Query().Get("cid")
	channelType := req.URL.Query().Get("type")

	// Validation
	if sessionID == "" || channelID == "" || channelType == "" {
		return &rest.Error{
			ExternalError: errors.Errorf("invalid parameters"),
			Code:          http.StatusBadRequest,
		}
	}

	session := h.SessionManager.GetSessionByHash(sessionID)
	if session == nil {
		return &rest.Error{
			ExternalError: errors.Errorf("session not existed: session[%s]", sessionID),
			Code:          http.StatusNotFound,
		}
	}

	var host = ""
	logger.Infof("RTC terminate: channel[%s]", channelID)

	ip := session.IP
	host = fmt.Sprintf("%s:%s", ip, rtcPort)

	// Relay to RTC server
	res, body, err := h.Manager.RelayRequestToRtc(host, "http", req)
	if err != nil {
		fmt.Fprint(w, jsonResponse{"return_code": -1, "error": jsonResponse{
			"reason": err}})

		return nil
	}

	if res.StatusCode == http.StatusOK {
		go h.SessionManager.ReleaseSession(session, h.InstanceManager.Detach)

		copyHeader(w.Header(), res.Header)
		w.WriteHeader(res.StatusCode)
		fmt.Fprint(w, jsonResponse{"return_code": 0})

		return nil
	}

	copyHeader(w.Header(), res.Header)
	w.WriteHeader(res.StatusCode)
	w.Write([]byte(body))

	return nil
}

// SetSdp sets sdp for RTC
func (h *Handler) SetSdp(w http.ResponseWriter, req *http.Request) *rest.Error {
	vars := mux.Vars(req)
	sessionID := vars["session_id"]

	session := h.SessionManager.GetSessionByHash(sessionID)
	if session == nil {
		return &rest.Error{
			ExternalError: errors.Errorf("session not existed: session[%s]", sessionID),
			Code:          http.StatusNotFound,
		}
	}

	var res *http.Response
	var body string
	var err error

	// Relay to RTC server
	logger.Infof("RTC setsdp: channel[%s], session[%s]", session.Channel, session.Key.Name())

	ip := session.IP
	host := fmt.Sprintf("%s:%s", ip, rtcPort)
	res, body, err = h.Manager.RelayRequestToRtc(host, "http", req)

	if err != nil {
		fmt.Fprint(w, jsonResponse{"return_code": -1, "error": jsonResponse{
			"reason": err}})

		return nil
	}

	copyHeader(w.Header(), res.Header)
	w.WriteHeader(res.StatusCode)
	w.Write([]byte(body))

	return nil
}

// DropNotPulledRTMP checks all RTMP connections and drop those without RTC pulling from
func (h *Handler) DropNotPulledRTMP() {
	logger.Debug("[cron] DropNotPulledRTMP")

	// Get all stream info from RTMP manager
	body, err := h.Manager.RtmpStreamInfo(nil)
	if err != nil {
		logger.Debugf("Fail RtmpStreamInfo: err[%s]", err)
		return
	}

	allRTMPStreamInfo := h.unmarshalRTMPStreamInfoResponse(body)
	for _, info := range allRTMPStreamInfo {
		go func(info RTMPStreamInfo) {
			if info.Play {
				return
			}

			// don't drop RTMP connection immediately, confirm more times
			isNotPulledRTMP := func(...interface{}) bool {
				body, err := h.Manager.RtmpStreamInfo(map[string]string{"channel": info.Channel})

				if err != nil {
					logger.Debugf("Fail RtmpStreamInfo: err[%s]", err)
					return false
				}

				result := h.unmarshalRTMPStreamInfoResponse(body)
				return !(result[0].Play)
			}

			if flowutil.MultipleConfirm(3, 5*time.Second, isNotPulledRTMP) {
				logger.Debugf("Not pulled RTMP found: streamInfo[%+v]", info)

				if err := h.Manager.DropRtmpSession(info.ServerIP, info.Key, info.Channel); err != nil {
					logger.Warnf("Drop not pulled RTMP session fails: %s", err)
				}
			}
			// :~)

		}(info)
	}
}

func (h *Handler) unmarshalRTMPStreamInfoResponse(jsonString string) []RTMPStreamInfo {
	var dat []RTMPStreamInfo

	err := json.Unmarshal([]byte(jsonString), &dat)
	if err != nil {
		logger.Debugf("Unmarshal error: jsonString[%s], error[%s]", jsonString, err)
	}

	return dat
}

// GetCountOfRTMPStreamInfoByKey counts how many live RTMP connection is with the same key as the `inputKey`
// according to the response returned by RTMP `streamInfo` API
func (h *Handler) GetCountOfRTMPStreamInfoByKey(jsonString, inputKey string) int {
	streamInfo := h.unmarshalRTMPStreamInfoResponse(jsonString)

	qualifiables := make([]collectionutil.Qualifiable, len(streamInfo))
	for i, v := range streamInfo {
		qualifiables[i] = v
	}

	count, _ := collectionutil.GetQualifiedItems(qualifiables, inputKey)

	return count
}

func (h *Handler) isFromWowzaServer(src string) bool {
	if collectionutil.InStringSlice(h.AppContext.WowzaServers, src) {
		return true
	}

	return false
}

func (h *Handler) isPremium(owner string) bool {
	if h.IsStraas() {
		return true
	}

	if collectionutil.InStringSlice(polling.PremiumOwners, owner) {
		return true
	}

	return false
}

// BuildRouter builds path routing
func BuildRouter(restHandler Handler) *mux.Router {
	// Define http route
	router := mux.NewRouter()

	/**
	* RTC API
	 */
	router.Handle("/broadcaster/{session_id}",
		HandlerWrapper(restHandler.Broadcast)).Methods("GET")

	router.Handle("/terminate",
		HandlerWrapper(restHandler.Terminate)).Methods("GET")

	router.Handle("/setsdp/{session_id}",
		HandlerWrapper(restHandler.SetSdp)).Methods("GET")
	// :~)

	/**
	* RTMP API
	 */
	router.Handle("/rtmp/streamkey",
		HandlerWrapper(restHandler.GetStreamKey)).Methods("GET")
	router.Handle("/rtmp/streamkey/{owner_id}",
		HandlerWrapper(restHandler.DeleteStream)).Methods("DELETE")

	router.Handle("/rtmp/event",
		HandlerWrapper(restHandler.ProcessRtmpEvent)).Methods("POST")
	// :~)

	/**
	* Others
	 */
	router.Handle("/api/{channel_id}",
		HandlerWrapper(restHandler.GetServerAddressByChannel)).Methods("GET")

	router.Handle("/live-channels",
		HandlerWrapper(restHandler.ListLiveChannels)).Methods("GET")

	router.Handle("/sessions",
		HandlerWrapper(restHandler.ListSessions)).Methods("GET")

	router.Handle("/healthcheck",
		HandlerWrapper(restHandler.HealthCheck)).Methods("GET")

	router.Handle("/healthcheck/computeengine",
		HandlerWrapper(restHandler.ComputeEngineHealthCheck)).Methods("GET")
	// :~)

	return router
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		if strings.ToUpper(k) == "CONTENT-LENGTH" {
			continue
		}

		for i, v := range vv {
			if i == 0 {
				dst.Set(k, v)
			} else {
				dst.Add(k, v)
			}
		}
	}
}

// postSlackMessage posts message to some channel
func postSlackMessage(token, channelName, message string) error {
	api := slack.New(token)

	channel, err := api.FindChannelByName(channelName)
	if err != nil {
		log.Printf("Fail to find slack channel: slackChannel[%s], err[%s]",
			channelName, err.Error())

		return err
	}

	options := &slack.ChatPostMessageOpt{
		AsUser:   true,
		Username: "notify-gril",
	}

	return api.ChatPostMessage(channel.Id, message, options)
}

func getUpdateLiveReq(resp string) interface{} {
	v, _ := jason.NewValueFromBytes([]byte(resp))

	arr, _ := v.Array()
	obj, _ := arr[0].Object()

	thum, _ := obj.GetObject("liveThumbnailSet")

	m := map[string]interface{}{}
	m["thumbnail_set"] = map[string]interface{}{
		"current": thum,
	}

	return m
}
