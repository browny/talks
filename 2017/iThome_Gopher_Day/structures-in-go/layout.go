package main

var repoLayout = `
export GOPATH=/Users/brownylin/go:/Users/brownylin/wego

./wego
	./script					#-- common orchestration script --#
	./regression				#-- integration test --#
	./src
		./glide.yml
		./vendor				#-- not in vcs (generated by glide install) --#
		./github.com/...
		./straas.io/
			./base				#-- common utilities --#
			    ./metric
			    ./limiter
			    ...
			./external			#-- common external dependency --#
			    ./fluent
			    ./etcd
			    ...
			./<modules>			#-- all microservices modules --#
`

var moduleLayout = `
./middle					#-- resource coordinator module --#
	./config				#-- viper based config --#
	./dep					#-- dependency weaving --#
	./domain
		./entity.go
		./enum.go
		./req.go
	./usecases
	    ./audit.go				#-- auditing states of resource entity --#
		./bookkeeper.go			#-- manage transition of resource entity states--#
		./reporter.go			#-- metrics, tracing, etc... --#
	./interfaces
		./cloud
		./etcd
		./requester				#-- outward http request --#
		./rest					#-- REST API entry --#
	./main
		./main.go
`

var buildImage = `
FROM golang:1.7.1

# install glide
RUN \
  go get github.com/Masterminds/glide

COPY script/wego-reactor/glide.yaml /go/src/glide.yaml
WORKDIR /go/src

RUN \
  glide install && \
  glide rebuild && \
  rm /go/src/glide.yaml
`

var runImage = `
FROM wego-golang-run:latest

COPY build/middle /build/middle
USER serviceacc

ENTRYPOINT ["/build/middle"]
`

var golangRunImage = `
FROM debian:jessie

RUN \
  echo 'install pkgs' && \
  apt-get update && \
  apt-get install -y ca-certificates curl && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*
`

var naive = `
./controller/dispatcher.go
./controller/dao/machine.go
./controller/req/machine.go

package controller

import (
	straas.io/middle/controller/dao
	straas.io/middle/controller/req
)

type Dispatcher struct {
	machineDao *dao.MachineDao
	machineReq *req.MachineReq
}
`

var naive = `
func (d *Dispatcher) GetAvailableMachine() *dao.Machine {

	machines, _ := d.machineDao.List()

	for _, m := range machines {
		if m.State != "spare" {
			continue
		}

		if !d.machineReq.IsHealthy(m.Host) {
			continue
		}

		return m
	}

	return nil
}
`

var clean = `
./domain/entity.go
./usecases/dispatch.go
./usecases/dao/machine.go
./interfaces/req/machine.go

package domain
type Machine struct {
	...
}

package dao
type Machine struct {
	func (m *MachineDao) List() ([]*domain.Machine, error) {
		...
	}
}

package req
type Machine struct {
	func (m *Machine) IsHealthy(host string) bool {
		...
	}
}
`

var clean = `
package usecases

type MachineDao interface {
	List() ([]*domain.Machine, error)
}

type MachineReq interface {
	func (m *Machine) IsHealthy(host string) bool
}

type Dispatcher struct {
	machineDao MachineDao
	machineReq MachineReq
}
`

var clean = `
func (d *Dispatcher) GetAvailableMachine() *domain.Machine {

	machines, _ := d.machineDao.List()

	for _, m := range machines {
		if m.State != "spare" {
			continue
		}

		if !d.machineReq.IsHealthy(m.Host) {
			continue
		}

		return m
	}

	return nil
}
`

var test = `
func (s *DispatcherTestSuite) Test_GetAvailableMachine() {
	machineDaoMock := mocks.MachineDao{}
	machineReqMock := mocks.MachineReq{}

	tested := Dispatcher{
		machineDao: machineDaoMock
		machineReq: machineReqMock
	}

	testList := []*domain.Machine{
		&domain.Transcoder{Host: "1.1.1.1", State: "busy"},
		&domain.Transcoder{Host: "2.2.2.2", State: "spare"},
		&domain.Transcoder{Host: "3.3.3.3", State: "spare"},
	}

	machineDaoMock.On("List").Return(testList, nil).Once()
	machineReqMock.On("IsHealthy", mock.Anything).Return(
		func(host string) bool {
			if host == "2.2.2.2" { return false }
			return true
		}
	).Times(2)

	result := tested.GetAvailableMachine()
	s.NotNil(result)
	s.Equal("3.3.3.3", result.Host)

	machineDaoMock.AssertExpectations(s.T())
	machineReqMock.AssertExpectations(s.T())
}
`
