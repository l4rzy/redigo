package redis

import (
	"sync"
)

// an assemblage is a collection of pools, each of these pool connects to a redis
// node that has an exact copy of a "master" node, just like RAID-1 but for redis

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

func (a *Assemblage) Get() Conn {
	a.mu.Lock()
	a.i = (a.i + 1) % a.num_pools
	a.mu.Unlock()
	return a.pools[a.i].Get()
}
