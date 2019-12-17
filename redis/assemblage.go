package redis

import (
	"sync"
)

// an assemblage is a collection of pools, each of these pool connects to a redis
// node that has an exact copy of a "master" node, just like RAID-1 but for redis

const (
	ASSEMBLAGE_ROUND_ROBIN = 0
	ASSEMBLAGE_ADAPTIVE    = 1
)

type Assemblage struct {
	num_pools int // number of pools in an assemblage
	pools     []*Pool
	mu        *sync.Mutex
	i         int
}

func NewAssemblage() *Assemblage {
	return &Assemblage{
		num_pools: 0,
		i:         0,
		mu:        &sync.Mutex{},
	}
}

func (a *Assemblage) AddPool(p *Pool) {
	a.pools = append(a.pools, p)
	a.num_pools++
}

// no need to consistently return a pool, just pick the one with the most idle connections
func (a *Assemblage) Get(kind int) Conn {
	switch kind {
	case ASSEMBLAGE_ROUND_ROBIN:
		return a.getRoundRobin()
	case ASSEMBLAGE_ADAPTIVE:
		return a.getAdaptive()
	}
	// default case
	return a.getRoundRobin()
}

func (a *Assemblage) getRoundRobin() Conn {
	// no need to call mutex here since num_pools is a field
	// a.mu.Lock()
	a.i = (a.i + 1) % a.num_pools
	// a.mu.Unlock()
	return a.pools[a.i].Get()
}

func (a *Assemblage) getAdaptive() Conn {
	var max_idle_i = 0

	// not really sure about efficiency of this over round robin, since this calls
	// mutex lock and unlock exactly `num_pools` times, whereas RR is O(1)
	// regardless `num_pools`
	for i := 1; i < a.num_pools; i++ {
		if a.pools[i].IdleCount() >= max_idle_i {
			max_idle_i = i
		}
	}

	return a.pools[max_idle_i].Get()
}
