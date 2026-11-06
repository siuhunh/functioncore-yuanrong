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

// Package colorprint Print with color
// ac Aho-Corasick automation Multi-string matching single-string algorithm
package colorprint

import (
	"strings"
)

// two results are added for each match during AC automaton cutting,
// and two results need to be rolled back during rollback
const acEachAppend = 2

// acNode ac Automaton Node
type acNode struct {
	fail      *acNode
	next      map[rune]*acNode
	isPattern bool
}

func newAcNode() *acNode {
	return &acNode{
		fail:      nil,
		isPattern: false,
		next:      map[rune]*acNode{},
	}
}

// AcAutoMachine trie tree of ac automaton
type AcAutoMachine struct {
	root *acNode
}

func newAcAutoMachine() *AcAutoMachine {
	return &AcAutoMachine{
		root: newAcNode(),
	}
}

func (ac *AcAutoMachine) addPattern(pattern string) {
	chars := []rune(pattern)
	iter := ac.root
	for _, c := range chars {
		if _, ok := iter.next[c]; !ok {
			iter.next[c] = newAcNode()
		}
		iter = iter.next[c]
	}
	iter.isPattern = true
}

func (ac *AcAutoMachine) buildTrie() {
	var trie []*acNode
	trie = append(trie, ac.root)
	for len(trie) != 0 {
		parent := trie[0]
		trie = trie[1:]
		for char, child := range parent.next {
			trie = append(trie, ac.getTrieChild(parent, child, char))
		}
	}
}

func (ac *AcAutoMachine) getTrieChild(parent, child *acNode, char rune) *acNode {
	if parent == ac.root {
		child.fail = ac.root
	} else {
		failAcNode := parent.fail
		for failAcNode != nil {
			if _, ok := failAcNode.next[char]; ok {
				child.fail = failAcNode.next[char]
				break
			}
			failAcNode = failAcNode.fail
		}
		if failAcNode == nil {
			child.fail = ac.root
		}
	}
	return child
}

func (ac *AcAutoMachine) splitWithAC(content string) []string {
	chars := []rune(content)
	iter := ac.root
	var start, end int
	var results []string
	for i, c := range chars {
		_, ok := iter.next[c]
		for !ok && iter != ac.root {
			iter = iter.fail
		}
		if _, ok = iter.next[c]; ok {
			if iter == ac.root { // this is the first match, record the start position
				start = i
			}
			iter = iter.next[c]
			if iter.isPattern {
				results, end = ac.matchString(content, start, i, end, results)
			}
		}
	}
	if len(results) == 0 {
		return []string{content}
	}
	if end != len(chars)-1 && len(chars) != 0 {
		results = append(results, string([]rune(content)[end+1:]))
	}
	return results
}

func (ac *AcAutoMachine) matchString(content string, start, current, end int, results []string) ([]string, int) {
	currentStr := string([]rune(content)[start : current+1])
	if len(results) > 0 && strings.Contains(currentStr, results[len(results)-1]) &&
		currentStr != results[len(results)-1] {
		end -= len([]rune(results[len(results)-acEachAppend])) + 1
		results = results[:len(results)-acEachAppend]
	}
	if end == 0 {
		results = append(results, string([]rune(content)[end:start]))
	} else {
		results = append(results, string([]rune(content)[end+1:start]))
	}
	results = append(results, currentStr)
	return results, current
}
