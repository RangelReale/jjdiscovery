package jjdiscovery

import (
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
}

func NewClient(consulcli *consul.Client, opts ...ClientOption) *Client {
	ret := &Client{}

	for _, opt := range opts {
		opt(&ret.opts)
	}

	return ret
}

func (c *Client) Close() {

}

func (c *Client) Get(service string, opts ...ClientGetOption) (*ClientService, error) {
	return nil, nil
}

//
// ClientService
//

type ClientService struct {
	Service string
}

//
// options
//

type ClientOption func(options *clientOptions)

//
// get options
//

type clientGetOptions struct {
	fixedVersion *semver.Version
	minVersion   *semver.Version
	versionRange *semver.Range
}

type ClientGetOption func(options *clientGetOptions)

func ClientGetFixedVersion(fixedVersion semver.Version) ClientGetOption {
	return func(o *clientGetOptions) {
		o.fixedVersion = &fixedVersion
	}
}

func ClientGetMinVersion(minVersion semver.Version) ClientGetOption {
	return func(o *clientGetOptions) {
		o.minVersion = &minVersion
	}
}

func ClientGetVersionRange(versionRange semver.Range) ClientGetOption {
	return func(o *clientGetOptions) {
		o.versionRange = &versionRange
	}
}
