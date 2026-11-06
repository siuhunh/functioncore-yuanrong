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

package diskusage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const folderSize = 4096

type cache struct {
	directory
	rootDir string
}

func (c *cache) add(name string, fi os.FileInfo) {
	name = strings.TrimPrefix(name, c.rootDir)
	if name != "" {
		c.directory.add(name, fi)
	}
}

func (c *cache) remove(name string, walkFunc walkFunc) error {
	name = strings.TrimPrefix(name, c.rootDir)
	if name != "" {
		return c.directory.remove(name, walkFunc)
	}
	return nil
}

type walkFunc func(path string, f file) error

type file interface {
	size() int64
}

type normalFile struct {
	si int64
}

func (n *normalFile) size() int64 {
	return n.si
}

// trie-like structure
type directory struct {
	cachedSize *int64
	children   map[string]file
}

func (d *directory) size() int64 {
	if d.cachedSize != nil {
		return *d.cachedSize
	}
	var total int64 = folderSize
	for _, child := range d.children {
		total += child.size()
	}
	d.cachedSize = &total
	return total
}

func (d *directory) add(name string, fi os.FileInfo) {
	dirname, filename := filepath.Split(name)
	node := d.addParent(dirname)
	if node.children == nil {
		node.children = map[string]file{}
	}
	if fi.IsDir() {
		if dirIf, ok := node.children[filename]; !ok {
			node.children[filename] = &directory{}
		} else {
			_, ok := dirIf.(*directory)
			if !ok {
				node.children[filename] = &directory{}
			}
		}
	} else {
		size := fi.Size()
		if normalFileIf, ok := node.children[filename]; !ok {
			node.children[filename] = &normalFile{si: size}
		} else {
			f, ok := normalFileIf.(*normalFile)
			if !ok {
				node.children[filename] = &normalFile{si: size}
			} else {
				f.si = size
			}
		}
	}
}

func (d *directory) addParent(dirname string) *directory {
	node := d
	node.cachedSize = nil
	part, i := segmenter(dirname, 0)
	for ; i != -1; part, i = segmenter(dirname, i) {
		if part == "" {
			continue
		}
		if node.children == nil {
			node.children = map[string]file{}
		}

		var dir *directory
		if dirIf, ok := node.children[part]; !ok {
			dir = &directory{}
			node.children[part] = dir
		} else {
			var ok bool
			dir, ok = dirIf.(*directory)
			if !ok {
				dir = &directory{}
				node.children[part] = dir
			}
		}
		node = dir
		node.cachedSize = nil
	}
	return node
}

func (d *directory) remove(name string, walkFunc walkFunc) error {
	dirname, filename := filepath.Split(name)
	node := d
	node.cachedSize = nil
	part, i := segmenter(dirname, 0)
	for ; i != -1; part, i = segmenter(dirname, i) {
		if part == "" {
			continue
		}
		if node.children == nil {
			return nil
		}

		dirIf, ok := node.children[part]
		if !ok {
			return nil
		}
		dir, ok := dirIf.(*directory)
		if !ok {
			return nil
		}

		node = dir
		node.cachedSize = nil
	}

	if node.children == nil {
		return nil
	}
	ff, ok := node.children[filename]
	if !ok {
		return nil
	}
	if err := walk(ff, name, walkFunc); err != nil {
		return err
	}

	delete(node.children, filename)
	return nil
}

func walk(ff file, path string, walkFunc walkFunc) error {
	if walkFunc != nil {
		if err := walkFunc(path, ff); err != nil {
			return err
		}
	}
	switch f := ff.(type) {
	case *normalFile:
		// nothing
	case *directory:
		for part, child := range f.children {
			if err := walk(child, path+string(filepath.Separator)+part, walkFunc); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("failed to assert")
	}
	return nil
}

func segmenter(path string, start int) (string, int) {
	if len(path) == 0 || start < 0 || start >= len(path) {
		return "", -1
	}
	end := strings.IndexRune(path[start:], filepath.Separator)
	if end == -1 {
		return path[start:], end
	}
	return path[start : start+end], start + end + 1
}
