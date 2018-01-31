package jjdiscovery

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/RangelReale/go.uuid"
	"github.com/blang/semver"
	consul "github.com/hashicorp/consul/api"
)

type Server struct {
	consulcli *consul.Client
	checkchan chan bool
	regchan   chan regdata
	services  map[string]*serverService
	m         sync.Mutex

	opts serverOptions
}

type serverOptions struct {
	ttlSec  int
	logfunc LogFunc
}

func NewServer(consulcli *consul.Client, opts ...ServerOption) *Server {
	ret := &Server{
		consulcli: consulcli,
		checkchan: make(chan bool),
		regchan:   make(chan regdata, 10),
		services:  make(map[string]*serverService),
		opts: serverOptions{
			ttlSec: 30,
		},
	}

	for _, opt := range opts {
		opt(&ret.opts)
	}

	// start ttl and registration service
	go ret.check()

	return ret
}

func (s *Server) DeregisterAll() {
	var dr []regdata

	s.m.Lock()
	for _, svc := range s.services {
		for _, a := range svc.addressList {
			dr = append(dr, regdata{service: svc.service, sid: a.sid})
		}
	}
	s.m.Unlock()

	for _, rd := range dr {
		err := s.Deregister(rd.service, rd.sid)
		if err != nil {
			s.log(fmt.Sprintf("[server.deregister_all][ERROR] service %s:%s could not be deregistered: %v", rd.service, rd.sid, err))
		}
	}
}

func (s *Server) Close(deregister bool) {
	// stop check thread
	s.checkchan <- true

	// de-register
	if deregister {
		s.DeregisterAll()
	}
}

func (s *Server) Register(service string, address string, port int, version string) (string, error) {
	s.m.Lock()
	defer s.m.Unlock()

	// parse version
	ver, err := semver.Make(version)
	if err != nil {
		return "", err
	}

	// create random sid
	sid := uuid.NewV4().String()

	// store in map
	svc, ok := s.services[service]
	if !ok {
		svc = &serverService{
			service:     service,
			addressList: make(map[string]*serverServiceAddress),
		}
		s.services[service] = svc
	}

	svc.addressList[sid] = &serverServiceAddress{
		sid:        sid,
		address:    address,
		port:       port,
		version:    ver,
		registered: false,
		lastCheck:  time.Now(),
	}

	// queue to register
	s.regchan <- regdata{service: service, sid: sid}

	return sid, nil
}

func (s *Server) Deregister(service string, sid string) error {
	s.m.Lock()
	defer s.m.Unlock()

	svc, ok := s.services[service]
	if !ok {
		return errors.New("Service not found")
	}

	sa, ok := svc.addressList[sid]
	if !ok {
		return errors.New("Sid not found")
	}

	err := s.consulcli.Agent().ServiceDeregister(sa.sid)

	// remove local reference before returning error
	delete(svc.addressList, sid)

	if len(svc.addressList) == 0 {
		delete(s.services, service)
	}

	return err
}

func (s *Server) check() {
	finished := false
	had_check := true
	for !finished {
		delay := time.Second * time.Duration(s.opts.ttlSec/4)
		if had_check {
			delay = time.Second * 1
		}

		select {
		case <-s.checkchan:
			finished = true
		case rd := <-s.regchan:
			s.do_register(rd)
		case <-time.After(delay):
			had_check = s.do_check()
		}
	}
}

func (s *Server) do_check() bool {
	s.m.Lock()
	defer s.m.Unlock()

	return s.do_check_item()
}

func (s *Server) do_check_item() bool {
	// find the oldest item
	var oldest *serverServiceAddress
	var oldestservice string
	var oldestTime time.Time

	for _, svc := range s.services {
		for _, a := range svc.addressList {
			if a.registered && time.Since(a.lastCheck) >= time.Second*time.Duration(s.opts.ttlSec) && (oldest == nil || a.lastCheck.Before(oldestTime)) {
				oldest = a
				oldestservice = svc.service
				oldestTime = a.lastCheck
			}
		}
	}

	if oldest != nil {
		s.log(fmt.Sprintf("[server.do_check] service %s:%s update ttl", oldestservice, oldest.sid))

		oldest.lastCheck = time.Now()

		err := s.consulcli.Agent().UpdateTTL(fmt.Sprintf("service:%s", oldest.sid), "", consul.HealthPassing)
		if err != nil {
			s.log(fmt.Sprintf("[server.do_check][ERROR] service %s:%s error in update ttl, re-registering: %v", oldestservice, oldest.sid, err))

			// error, re-register
			oldest.registered = false

			// queue to register
			s.regchan <- regdata{service: oldestservice, sid: oldest.sid}
		}
		return true
	}
	return false
}

func (s *Server) do_register(reg regdata) {
	s.m.Lock()
	defer s.m.Unlock()

	s.log(fmt.Sprintf("[server.do_register] registering service %s:%s", reg.service, reg.sid))

	service, ok := s.services[reg.service]
	if !ok {
		s.log(fmt.Sprintf("[server.do_register][ERROR] service %s:%s not found", reg.service, reg.sid))
		return
	}

	address, ok := service.addressList[reg.sid]
	if !ok {
		s.log(fmt.Sprintf("[server.do_register][ERROR] service %s:%s address not found", reg.service, reg.sid))
		return
	}

	if address.registered {
		return
	}

	// register with consul
	err := s.consulcli.Agent().ServiceRegister(&consul.AgentServiceRegistration{
		ID:   address.sid,
		Name: service.service,
		Tags: []string{
			fmt.Sprintf("v%s", address.version.String()),
			//fmt.Sprintf("addr%s:%d", address, port),
		},
		Port: address.port,
		Check: &consul.AgentServiceCheck{
			TTL: fmt.Sprintf("%ds", s.opts.ttlSec),
			DeregisterCriticalServiceAfter: fmt.Sprintf("%ds", s.opts.ttlSec*3),
		},
	})
	if err != nil {
		s.log(fmt.Sprintf("[server.do_register][ERROR] error registering service %s:%s with consul: %v", reg.service, reg.sid, err))
		return
	}

	// force initial pass check
	s.consulcli.Agent().UpdateTTL(fmt.Sprintf("service:%s", address.sid), "", consul.HealthPassing)

	address.registered = true
	address.lastCheck = time.Now()
}

func (s *Server) log(msg string) {
	if s.opts.logfunc != nil {
		s.opts.logfunc(msg)
	}
}

//
// options
//

type ServerOption func(options *serverOptions)

func ServerTtlSec(ttlSec int) ServerOption {
	return func(o *serverOptions) {
		o.ttlSec = ttlSec
	}
}

func ServerLogFunc(logFunc LogFunc) ServerOption {
	return func(o *serverOptions) {
		o.logfunc = logFunc
	}
}

//
// service
//

type serverService struct {
	service     string
	addressList map[string]*serverServiceAddress
}

type serverServiceAddress struct {
	sid        string
	address    string
	port       int
	version    semver.Version
	registered bool
	lastCheck  time.Time
}

//
// other
//

type regdata struct {
	service string
	sid     string
}