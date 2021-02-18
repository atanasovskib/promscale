// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"context"
	"github.com/timescale/promscale/pkg/ha/client"
	"sync"
	"time"
)

func MockNewHAService(clusterInfo []*client.LeaseDBState) *Service {
	lockClient := newMockLockClient()
	timeout, refresh, _ := lockClient.ReadLeaseSettings(context.Background())

	for _, c := range clusterInfo {
		lockClient.leadersPerCluster[c.Cluster] = c
	}

	service := &Service{
		state:             &sync.Map{},
		leaseClient:       lockClient,
		leaseTimeout:      timeout,
		leaseRefresh:      refresh,
		leaderChangeLocks: &sync.Map{},
	}
	return service
}

func SetLeaderInMockService(service *Service, cluster, leader string, minT, maxT time.Time) {
	service.leaseClient.(*mockLockClient).leadersPerCluster[cluster] = &client.LeaseDBState{
		Cluster:    cluster,
		Leader:     leader,
		LeaseStart: minT,
		LeaseUntil: maxT,
	}
}
