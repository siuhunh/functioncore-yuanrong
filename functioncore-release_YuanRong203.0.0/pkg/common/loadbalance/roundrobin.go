/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package loadbalance provides roundrobin algorithm
package loadbalance

import (
	"math/rand"
	"time"
)

// WeightNginx weight nginx
type WeightNginx struct {
	Node            interface{}
	Weight          int
	CurrentWeight   int
	EffectiveWeight int
}

// WNGINX w nginx
type WNGINX struct {
	nodes []*WeightNginx
	n     int
}

// Add add node
func (w *WNGINX) Add(node interface{}, weight int) {
	weighted := &WeightNginx{
		Node:            node,
		Weight:          weight,
		EffectiveWeight: weight}
	w.nodes = append(w.nodes, weighted)
	w.n++
}

// Remove removes a node
func (w *WNGINX) Remove(node interface{}) {
	for i, weighted := range w.nodes {
		if weighted.Node == node {
			w.nodes = append(w.nodes[:i], w.nodes[i+1:]...)
			w.n--
			break
		}
	}
}

// RemoveAll remove all nodes
func (w *WNGINX) RemoveAll() {
	w.nodes = w.nodes[:0]
	w.n = 0
}

// Next get next node
func (w *WNGINX) Next(_ string, _ bool) interface{} {
	if w.n == 0 {
		return nil
	}
	if w.n == 1 {
		return w.nodes[0].Node
	}
	return nextWeightedNode(w.nodes).Node
}

// nextWeightedNode get best next node info
func nextWeightedNode(nodes []*WeightNginx) *WeightNginx {
	total := 0
	if len(nodes) == 0 {
		return nil
	}
	best := nodes[0]
	for _, w := range nodes {
		w.CurrentWeight += w.EffectiveWeight
		total += w.EffectiveWeight
		if w.CurrentWeight > best.CurrentWeight {
			best = w
		}
	}
	best.CurrentWeight -= total
	return best
}

// Reset reset all nodes
func (w *WNGINX) Reset() {
	for _, s := range w.nodes {
		s.EffectiveWeight = s.Weight
		s.CurrentWeight = 0
	}
}

// Done -
func (w *WNGINX) Done(node interface{}, cost time.Duration) {}

// NextWithRequest -
func (w *WNGINX) NextWithRequest(req *Request, move bool) interface{} {
	return w.Next(req.Name, move)
}

// SetConcurrency -
func (w *WNGINX) SetConcurrency(concurrency int) {}

// SetIsLongRun -
func (w *WNGINX) SetIsLongRun(isLongRun bool) {}

// SetIsCaeFunc -
func (w *WNGINX) SetIsCaeFunc(isCaeFunc bool) {}

// Start -
func (w *WNGINX) Start() {}

// Stop -
func (w *WNGINX) Stop() {}

// NoLock -
func (w *WNGINX) NoLock() bool {
	return false
}

// WeightLvs weight lv5
type WeightLvs struct {
	Node   interface{}
	Weight int
}

// WLVS w lv5
type WLVS struct {
	nodes []*WeightLvs
	n     int
	gcd   int // general weight divisor
	maxW  int // maximum weight
	i     int // number of times selected
	cw    int // current weight
}

// Next get next node
func (w *WLVS) Next(_ string, _ bool) interface{} {
	if w.n == 0 {
		return nil
	}
	if w.n == 1 {
		return w.nodes[0].Node
	}
	for {
		if w.updateCwAnsIsReturn() {
			return nil
		}

		if w.i < len(w.nodes) && w.nodes[w.i].Weight >= w.cw {
			return w.nodes[w.i].Node
		}
	}
}

// updateCwAnsIsReturn update current weight and return the value whether to return
func (w *WLVS) updateCwAnsIsReturn() bool {
	w.i = (w.i + 1) % w.n
	if w.i == 0 {
		if w.cw = w.cw - w.gcd; w.cw <= 0 {
			if w.cw = w.maxW; w.cw == 0 {
				return true
			}
		}
	}

	return false
}

// Add add a node
func (w *WLVS) Add(node interface{}, weight int) {
	weighted := &WeightLvs{Node: node, Weight: weight}
	if weight > 0 {
		w.gcd = gcd(w.gcd, weight)
		if w.maxW < weight {
			w.maxW = weight
		}
	}
	w.nodes = append(w.nodes, weighted)
	w.n++
}

// Returns the maximum divisor
func gcd(x, y int) int {
	for {
		if y == 0 {
			return x
		}
		x, y = y, x%y
	}
}

// Remove removes a node
func (w *WLVS) Remove(node interface{}) {
	for i, weighted := range w.nodes {
		if weighted.Node == node {
			w.nodes = append(w.nodes[:i], w.nodes[i+1:]...)
			w.n--
			break
		}
	}
}

// RemoveAll remove all nodes
func (w *WLVS) RemoveAll() {
	w.nodes = w.nodes[:0]
	w.n = 0
	w.gcd = 0
	w.maxW = 0
	w.i = -1
	w.cw = 0
}

// Reset reset all nodes
func (w *WLVS) Reset() {
	w.i = -1
	w.cw = 0
}

// Done -
func (w *WLVS) Done(node interface{}, cost time.Duration) {}

// NextWithRequest -
func (w *WLVS) NextWithRequest(req *Request, move bool) interface{} {
	return w.Next(req.Name, move)
}

// SetConcurrency -
func (w *WLVS) SetConcurrency(concurrency int) {}

// SetIsLongRun -
func (w *WLVS) SetIsLongRun(isLongRun bool) {}

// SetIsCaeFunc -
func (w *WLVS) SetIsCaeFunc(isCaeFunc bool) {}

// Start -
func (w *WLVS) Start() {}

// Stop -
func (w *WLVS) Stop() {}

// NoLock -
func (w *WLVS) NoLock() bool {
	return false
}

// NewRandomRR init RandomRR
func NewRandomRR() *RandomRR {
	return &RandomRR{
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// RandomRR is random version of WLVS
// it will shuffle all nodes randomly when reset
type RandomRR struct {
	rnd *rand.Rand
	WLVS
}

// Reset reset RR and shuffle all nodes
func (r *RandomRR) Reset() {
	r.WLVS.Reset()
	r.rnd.Shuffle(len(r.WLVS.nodes), func(i, j int) {
		r.WLVS.nodes[i], r.WLVS.nodes[j] = r.WLVS.nodes[j], r.WLVS.nodes[i]
	})
}
