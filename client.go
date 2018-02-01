package jjdiscovery

import (
	"fmt"
	"sync"

	"github.com/blang/semver"
	consul "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/watch"
)

type Client struct {
	consulcli *consul.Client
	m         sync.RWMutex

	opts clientOptions
}

type clientOptions struct {
	consulConfig *consul.Config
	tag          string
	logfunc      LogFunc
}

func NewClient(opts ...ClientOption) (*Client, error) {
	ret := &Client{
		opts: clientOptions{
			consulConfig: consul.DefaultConfig(),
			tag:          DefaultTag,
		},
	}

	for _, opt := range opts {
		opt(&ret.opts)
	}

	var err error
	ret.consulcli, err = consul.NewClient(ret.opts.consulConfig)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Client) Close() {

}

func (c *Client) Get(service string, opts ...ClientGetOption) (*ClientService, error) {
	c.m.RLock()
	defer c.m.RUnlock()

	var getOpts clientGetOptions
	for _, opt := range opts {
		opt(&getOpts)
	}

	servicelist, _, err := c.consulcli.Health().Service(service, c.opts.tag, !getOpts.returnInactive, nil)
	if err != nil {
		return nil, err
	}

	return c.parseResult(service, servicelist, getOpts), nil
}

func (c *Client) log(level LogLevel, msg string) {
	if c.opts.logfunc != nil {
		c.opts.logfunc(level, msg)
	}
}

func (c *Client) parseResult(service string, servicelist []*consul.ServiceEntry, getOpts clientGetOptions) *ClientService {
	ret := &ClientService{
		Service: service,
	}
	for _, svc := range servicelist {
		version, err := ConsulServiceVersion(svc)
		if err != nil {
			c.log(LEVEL_ERROR, fmt.Sprintf("[client.get] error parsing service %s:%s version: %v", service, svc.Service.ID, err))
			continue
		}

		// filter min version
		if getOpts.minVersion != nil && version.Compare(*getOpts.minVersion) == -1 {
			continue
		}

		// filter version range
		if getOpts.versionRange != nil && !getOpts.versionRange(version) {
			continue
		}

		ret.AddressList = append(ret.AddressList, &ClientServiceAddress{
			Sid:     svc.Service.ID,
			Address: svc.Node.Address,
			Port:    svc.Service.Port,
			Status:  ConsulServiceStatus(svc),
			Version: version,
		})
	}
	return ret
}

//
// ClientWatcher
//

type ClientWatcher struct {
	C chan *ClientService

	wp      *watch.Plan
	service string
	client  *Client
	getOpts clientGetOptions
}

func (c *Client) Watch(service string, opts ...ClientGetOption) (*ClientWatcher, error) {
	c.m.RLock()
	defer c.m.RUnlock()

	ret := &ClientWatcher{
		C:       make(chan *ClientService, 20),
		service: service,
		client:  c,
	}

	for _, opt := range opts {
		opt(&ret.getOpts)
	}

	wparams := map[string]interface{}{
		"type":    "service",
		"service": service,
		"tag":     c.opts.tag,
	}
	if !ret.getOpts.returnInactive {
		wparams["passingonly"] = false
	}

	var err error
	ret.wp, err = watch.Parse(wparams)
	if err != nil {
		return nil, err
	}

	ret.wp.Handler = ret.handle
	go ret.wp.Run(c.opts.consulConfig.Address)

	return ret, nil
}

func (cw *ClientWatcher) Stop() {
	cw.C <- nil
	cw.wp.Stop()
}

func (cw *ClientWatcher) handle(idx uint64, data interface{}) {
	servicelist, ok := data.([]*consul.ServiceEntry)
	if !ok {
		return
	}

	cw.C <- cw.client.parseResult(cw.service, servicelist, cw.getOpts)
}

//
// ClientService
//

type ClientService struct {
	Service     string
	AddressList []*ClientServiceAddress
}

type ClientServiceAddress struct {
	Sid     string
	Address string
	Port    int
	Version semver.Version
	Status  string
}

//
// options
//

type ClientOption func(options *clientOptions)

func ClientTag(tag string) ClientOption {
	return func(o *clientOptions) {
		o.tag = tag
	}
}

func ClientConsulConfig(consulConfig *consul.Config) ClientOption {
	return func(o *clientOptions) {
		o.consulConfig = consulConfig
	}
}

func ClientLogFunc(logFunc LogFunc) ClientOption {
	return func(o *clientOptions) {
		o.logfunc = logFunc
	}
}

//
// get options
//

type clientGetOptions struct {
	returnInactive bool
	minVersion     *semver.Version
	versionRange   semver.Range
}

type ClientGetOption func(options *clientGetOptions)

func ClientGetMinVersion(minVersion semver.Version) ClientGetOption {
	return func(o *clientGetOptions) {
		o.minVersion = &minVersion
	}
}

func ClientGetVersionRange(versionRange semver.Range) ClientGetOption {
	return func(o *clientGetOptions) {
		o.versionRange = versionRange
	}
}

func ClientGetReturnInactive(returnInactive bool) ClientGetOption {
	return func(o *clientGetOptions) {
		o.returnInactive = returnInactive
	}
}
