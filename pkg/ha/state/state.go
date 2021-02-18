package state

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/ha/client"
	"sync"
	"time"
)

type Lease struct {
	cluster               string
	leader                string
	leaseStart            time.Time
	leaseUntil            time.Time
	maxTimeSeen           time.Time // max data time seen by any instance
	maxTimeInstance       string    // the instance name that’s seen the maxtime
	maxTimeSeenLeader     time.Time
	recentLeaderWriteTime time.Time
	_mu                   sync.RWMutex
}

type LeaseView struct {
	Cluster               string
	Leader                string
	LeaseStart            time.Time
	LeaseUntil            time.Time
	MaxTimeSeen           time.Time
	MaxTimeInstance       string
	MaxTimeSeenLeader     time.Time
	RecentLeaderWriteTime time.Time
}

func NewLease(c client.LeaseClient, cluster, potentialLeader string, minT, maxT, currentTime time.Time) (*Lease, error) {
	dbState, err := c.UpdateLease(context.Background(), cluster, potentialLeader, minT, maxT)
	if err != nil {
		return nil, fmt.Errorf("could not create new lease: %#v", err)
	}
	return &Lease{
		cluster:               dbState.Cluster,
		leader:                dbState.Leader,
		leaseStart:            dbState.LeaseStart,
		leaseUntil:            dbState.LeaseUntil,
		maxTimeSeen:           dbState.LeaseUntil,
		maxTimeSeenLeader:     dbState.LeaseUntil,
		maxTimeInstance:       dbState.Leader,
		recentLeaderWriteTime: currentTime,
	}, nil
}

func (h *Lease) UpdateFromDB(c client.LeaseClient, potentialLeader string, minT, maxT time.Time) error {
	h._mu.RLock()
	stateFromDb, err := c.UpdateLease(context.Background(), h.cluster, potentialLeader, minT, maxT)
	h._mu.RUnlock()
	if err != nil {
		return fmt.Errorf("could not update lease from db: %#v", err)
	}
	h.SetUpdateFromDB(stateFromDb)
	return nil
}

func (h *Lease) SetUpdateFromDB(stateFromDB *client.LeaseDBState) {
	h._mu.Lock()
	defer h._mu.Unlock()
	h.leader = stateFromDB.Leader
	h.leaseStart = stateFromDB.LeaseStart
	h.leaseUntil = stateFromDB.LeaseUntil
}

func (h *Lease) UpdateMaxSeenTime(currentReplica string, currentMaxT, currentTime time.Time) {
	h._mu.Lock()
	defer h._mu.Unlock()
	if currentMaxT.After(h.maxTimeSeen) {
		h.maxTimeInstance = currentReplica
		h.maxTimeSeen = currentMaxT
	}

	if currentMaxT.After(h.maxTimeSeenLeader) && currentReplica == h.leader {
		h.maxTimeSeenLeader = currentMaxT
	}
	h.recentLeaderWriteTime = currentTime
}

func (h *Lease) SafeGetLeader() string {
	h._mu.RLock()
	defer h._mu.RUnlock()
	return h.leader
}

func (h *Lease) Clone() *LeaseView {
	h._mu.RLock()
	defer h._mu.RUnlock()
	return &LeaseView{
		Cluster:               h.cluster,
		Leader:                h.leader,
		LeaseStart:            h.leaseStart,
		LeaseUntil:            h.leaseUntil,
		MaxTimeSeen:           h.maxTimeSeen,
		MaxTimeInstance:       h.maxTimeInstance,
		MaxTimeSeenLeader:     h.maxTimeSeenLeader,
		RecentLeaderWriteTime: h.recentLeaderWriteTime,
	}
}
