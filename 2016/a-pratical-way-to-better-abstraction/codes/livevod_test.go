package polling

import (
	"log"
	"testing"

	"straas.io/middle/logger"
	"straas.io/middle/polling/mocks"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

var testLiveStatusResp = `
	[
	  {
	    "cid": "100592",
	    "gid": "h856p0aosf9a4i",
	    "startTime": "1476436464",
	    "hashCode": "1476436464h856p0aosf9a4i",
	    "vodDestination": "gs://straasio-vod-transcoded-staging-as/companyId",
	    "thumbnailDestination": "gs://straasio-video-thumbnail-staging-as/companyId",
	    "liveThumbnailSet": { },
	    "status": "completed",
	    "convResultInfo": {
	      "sourceType": "liveEvent",
	      "duration": 87,
	      "resolution": "_1080p",
	      "accomplished": true,
	      "live_started_at": "2016-10-14T09:14:24.000Z",
	      "live_ended_at": "2016-10-14T09:15:08.000Z",
	      "thumbnail_set": { },
	      "stream_url": "https://vod-alpha.straas.net/companyId/100592/147649a4i/playlist.m3u8"
	    },
	    "totalSize": "42544387"
	  }
	]`

func TestVodStatusPollableTestSuite(t *testing.T) {
	suite.Run(t, new(VodStatusPollableTestSuite))
}

type VodStatusPollableTestSuite struct {
	suite.Suite

	// mocks
	reqBridgerMock *mocks.ReqBridgerOp

	tested *VodStatusPollableImpl
}

func (s *VodStatusPollableTestSuite) SetupSuite() {
	log.Println("======== VodStatusPollable Test Starts ========")

	logger.Setup("local")

	s.reqBridgerMock = &mocks.ReqBridgerOp{}
	s.tested = &VodStatusPollableImpl{
		ReqBridgerOp: s.reqBridgerMock,
	}
}

func (s *VodStatusPollableTestSuite) Test01_buildSrcPathOfPlaylistConv() {
	// case straas
	vodDest := "gs://straasio-vod-transcoded-staging-as/cw.com.tw"
	streamURL := "https://vod-alpha.straas.net/cw.com.tw/Xc4zjj9S/1476688468xcph1z4n2k7nl8fr/playlist.m3u8"

	result := buildSrcPathOfPlaylistConv(vodDest, streamURL)

	s.Equal(
		"gs://straasio-vod-transcoded-staging-as/cw.com.tw/Xc4zjj9S/1476688468xcph1z4n2k7nl8fr/playlist.m3u8",
		result,
	)

	// case lh
	vodDest = "gs://lh-rtc-record-alpha-as"
	streamURL = "https://vod-alpha.straas.net/100592/1474862998qtiqod9h0qh93sor/playlist.m3u8"

	result = buildSrcPathOfPlaylistConv(vodDest, streamURL)

	s.Equal(
		"gs://lh-rtc-record-alpha-as/100592/1474862998qtiqod9h0qh93sor/playlist.m3u8",
		result,
	)
}

func (s *VodStatusPollableTestSuite) Test02_isUploadCompleted() {
	resp := testLiveStatusResp

	isCompleted := isUploadCompleted(resp)
	s.Equal(true, isCompleted)

	resp = `[{"status":"not completed"}]`
	isCompleted = isUploadCompleted(resp)
	s.Equal(false, isCompleted)

	resp = `[]`
	isCompleted = isUploadCompleted(resp)
	s.Equal(false, isCompleted)

	resp = `not json`
	isCompleted = isUploadCompleted(resp)
	s.Equal(false, isCompleted)
}

func (s *VodStatusPollableTestSuite) Test03_getVodDestination() {
	resp := testLiveStatusResp

	vd := getVodDestination(resp)
	s.Equal("gs://straasio-vod-transcoded-staging-as/companyId", vd)
}

func (s *VodStatusPollableTestSuite) Test04_getStreamURL() {
	resp := testLiveStatusResp

	url := getStreamURL(resp)
	s.Equal("https://vod-alpha.straas.net/companyId/100592/147649a4i/playlist.m3u8", url)
}

func (s *VodStatusPollableTestSuite) Test05_getUpdateVodReq() {
	resp := testLiveStatusResp

	req := getUpdateVodReq(resp)
	log.Printf("req: %+v", string(req))
}

func (s *VodStatusPollableTestSuite) Test06_getDuration() {
	resp := testLiveStatusResp

	dur := getDuration(resp)
	s.Equal(int64(87), dur)
}

func (s *VodStatusPollableTestSuite) Test07_GenMp4() {
	resp := `[{"convResultInfo":{"duration":35}}]`
	s.reqBridgerMock.On("Transcoded", mock.Anything, mock.Anything).Return("", nil).Once()

	// case: duration too short
	err := s.tested.GenMp4(mock.Anything, resp)
	s.Nil(err)
	s.reqBridgerMock.AssertNumberOfCalls(s.T(), "Transcoded", 0)

	// case: duration is ok (> 120)
	resp = `[{"convResultInfo":{"duration":130}}]`
	err = s.tested.GenMp4(mock.Anything, resp)
	s.Nil(err)
	s.reqBridgerMock.AssertNumberOfCalls(s.T(), "Transcoded", 1)
}

func (s *VodStatusPollableTestSuite) TearDownSuite() {
	log.Println("======== VodStatusPollable Test TearDown  ========")
}
