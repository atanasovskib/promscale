// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/ha/client"
	"time"

	"github.com/jackc/pgconn"
)

type mockLockClient struct {
	leadersPerCluster map[string]*client.LeaseDBState
}

func (m *mockLockClient) UpdateLease(_ context.Context, cluster, leader string, minTime, maxTime time.Time) (*client.LeaseDBState, error) {
	lock, exists := m.leadersPerCluster[cluster]
	if !exists {
		lock = &client.LeaseDBState{
			Cluster:    cluster,
			Leader:     leader,
			LeaseStart: minTime,
			LeaseUntil: maxTime,
		}
		m.leadersPerCluster[cluster] = lock
	}

	if cluster == "cluster4" || cluster == "cluster5" {
		p := &pgconn.PgError{}
		p.Code = "PS010"
		return lock, p
	}

	return lock, nil
}

func (m *mockLockClient) TryChangeLeader(_ context.Context, cluster, newLeader string, maxTime time.Time) (*client.LeaseDBState, error) {
	lock, exists := m.leadersPerCluster[cluster]
	if !exists {
		return nil, fmt.Errorf("no leader for %s, UpdateLease never called before TryChangeLeader", cluster)
	}
	lock = &client.LeaseDBState{
		Cluster:    cluster,
		Leader:     newLeader,
		LeaseStart: lock.LeaseUntil,
		LeaseUntil: maxTime,
	}
	m.leadersPerCluster[cluster] = lock
	return lock, nil
}

func newMockLockClient() *mockLockClient {
	return &mockLockClient{leadersPerCluster: make(map[string]*client.LeaseDBState)}
}

func (m *mockLockClient) ReadLeaseSettings(ctx context.Context) (timeout, refresh time.Duration, err error) {
	return time.Minute * 1, time.Second * 10, nil
}

var count int

func (m *mockLockClient) ReadLeaseState(ctx context.Context, cluster string) (*client.LeaseDBState, error) {
	count++

	// the below cases are added to simulate the scenario's the leader has updated by another
	// promscale now the current promscale should adopt to the change.
	if cluster == "cluster4" {
		m.leadersPerCluster[cluster].Leader = "replica2"
	}

	if cluster == "cluster5" && count == 1 {
		m.leadersPerCluster[cluster].Leader = "replica2"
		m.leadersPerCluster[cluster].LeaseUntil = time.Now().Add(-10 * time.Minute)
	}

	return m.leadersPerCluster[cluster], nil
}
