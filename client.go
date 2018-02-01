package jjdiscovery

import (
	"fmt"
	"sync"

	"github.com/blang/semver"
	consul "github.com/hashicorp/consul/api"
)

type Client struct {
	consulcli *consul.Client
	m         sync.Mutex

	opts clientOptions
}

type clientOptions struct {
	tag     string
	logfunc LogFunc
}

func NewClient(consulcli *consul.Client, opts ...ClientOption) *Client {
	ret := &Client{
		consulcli: consulcli,
		opts: clientOptions{
			tag: DefaultTag,
		},
	}

	for _, opt := range opts {
		opt(&ret.opts)
	}

	return ret
}

func (c *Client) Close() {

}

func (c *Client) Get(service string, opts ...ClientGetOption) (*ClientService, error) {
	var getOpts clientGetOptions
	for _, opt := range opts {
		opt(&getOpts)
	}

	servicelist, _, err := c.consulcli.Health().Service(service, c.opts.tag, !getOpts.returnInactive, nil)
	if err != nil {
		return nil, err
	}

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

	return ret, nil
}

func (c *Client) log(level LogLevel, msg string) {
	if c.opts.logfunc != nil {
		c.opts.logfunc(level, msg)
	}
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
