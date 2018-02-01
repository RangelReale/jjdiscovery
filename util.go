package jjdiscovery

import (
	"errors"
	"strings"

	"github.com/blang/semver"
	consul "github.com/hashicorp/consul/api"
)

var (
	DefaultTag = "jjdiscovery"
)

func ConsulServiceStatus(s *consul.ServiceEntry) string {
	status := consul.HealthPassing
	for _, c := range s.Checks {
		if c.Status != consul.HealthPassing {
			status = c.Status
			break
		}
	}
	return status
}

func ConsulServiceVersion(s *consul.ServiceEntry) (semver.Version, error) {
	version := ""
	for _, t := range s.Service.Tags {
		if strings.HasPrefix(t, "v") {
			version = strings.TrimPrefix(t, "v")
			break
		}
	}
	if version == "" {
		return semver.Version{}, errors.New("Version tag not found")
	}
	return semver.Make(version)
}
