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

package loadbalance

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNext(t *testing.T) {
	weightNginx := &WeightNginx{"Node1", 30, 10, 20}
	var node []*WeightNginx
	node = append(node, weightNginx)
	wnginx := WNGINX{node, 0}
	if wnginx.Next("", true) != nil {
		err := errors.New("Test_Next error")
		t.Fatal(err)
	}
}

func TestReset(t *testing.T) {
	weightNginx := &WeightNginx{"Node1", 30, 10, 20}
	var node []*WeightNginx
	node = append(node, weightNginx)
	wnginx := WNGINX{node, 0}
	wnginx.Reset()
	if weightNginx.EffectiveWeight != weightNginx.Weight {
		err := errors.New("Test_Reset error")
		t.Fatal(err)
	}
}

func TestAdd(t *testing.T) {
	weightLvs := &WeightLvs{"Node1", 10}
	var node []*WeightLvs
	node = append(node, weightLvs)
	wlvs := &WLVS{node, 10, 0, 10, 10, 10}
	wlvs.Add("Node2", 20)
	if wlvs.gcd != 20 || wlvs.maxW != 20 {
		err := errors.New("Test_Add error")
		t.Fatal(err)
	}
	wlvs.gcd = 0
	wlvs.Add("Node2", 20)
	if wlvs.gcd != 20 || wlvs.maxW != 20 {
		err := errors.New("Test_Add error")
		t.Fatal(err)
	}
}

func TestRemoveAll(t *testing.T) {
	weightLvs := &WeightLvs{"Node1", 10}
	var node []*WeightLvs
	node = append(node, weightLvs)
	wlvs := &WLVS{node, 10, 0, 10, 10, 10}
	wlvs.RemoveAll()
	if wlvs.maxW != 0 || wlvs.gcd != 0 || wlvs.cw != 0 || wlvs.i != -1 {
		err := errors.New("Test_RemoveAll error")
		t.Fatal(err)
	}
}

func TestWLVSReset(t *testing.T) {
	weightLvs := &WeightLvs{"Node1", 10}
	var node []*WeightLvs
	node = append(node, weightLvs)
	wlvs := &WLVS{node, 10, 0, 10, 10, 10}
	wlvs.Reset()
	if wlvs.cw != 0 || wlvs.i != -1 {
		err := errors.New("Test_WLVSReset error")
		t.Fatal(err)
	}

}

func TestWLVSNext(t *testing.T) {
	weightLvs := &WeightLvs{"Node1", 10}
	var node []*WeightLvs
	node = append(node, weightLvs)
	wlvs := &WLVS{node, 0, 0, 10, 10, 10}
	if wlvs.Next("", true) != nil {
		err := errors.New("Test_WLVSNext error")
		t.Fatal(err)
	}

	wlvss := &WLVS{node, 1, 0, 10, 10, 10}
	if wlvss.Next("", true) != "Node1" {
		err := errors.New("Test_WLVSNext error")
		t.Fatal(err)
	}
}

func Test_gcd(t *testing.T) {
	cases := []struct {
		x    int
		y    int
		want int
	}{
		{9, 0, 9},
		{0, 8, 8},
		{1, 1, 1},
		{2, 9, 1},
		{12, 18, 6},
		{60, 140, 20},
		{3, 5, 1},
		{111, 111, 111},
	}
	for _, tt := range cases {
		assert.Equal(t, tt.want, gcd(tt.x, tt.y))
	}
}
