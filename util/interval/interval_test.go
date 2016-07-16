// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Jingguo Yao (yaojingguo@gmail.com)

package interval

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
)

var degree = flag.Int("degree", 32, "B-Tree degree")

type Interval struct {
	r  Range
	id uintptr
}

func (iv *Interval) Range() Range {
	return iv.r
}

func (iv *Interval) ID() uintptr {
	return iv.id
}

func (iv *Interval) String() string {
	return fmt.Sprintf("%v-%d", iv.Range(), iv.ID())
}

func (interfaces interfaces) Len() int {
	return len(interfaces)
}

func (interfaces interfaces) Less(i, j int) bool {
	return Compare(interfaces[i], interfaces[j]) <= 0
}

func (interfaces interfaces) Swap(i, j int) {
	interfaces[i], interfaces[j] = interfaces[j], interfaces[i]
}

func (children children) Len() int {
	return len(children)
}

func (children children) Less(i, j int) bool {
	return children[i].Range.Start.Compare(children[j].Range.Start) <= 0
}

func (children children) Swap(i, j int) {
	children[i], children[j] = children[j], children[i]
}

// describe returns a string description of the tree. The format is similar to
// https://en.wikipedia.org/wiki/Newick_format
func (tree *Tree) describe() string {
	if tree.isEmpty() {
		return ";"
	}
	return tree.root.String()
}

func (n node) String() string {
	var buf bytes.Buffer
	n.describe(&buf)
	return buf.String()
}

func (n *node) describe(buf *bytes.Buffer) {
	if len(n.children) == 0 {
		for idx, i := range n.interfaces {
			if idx != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(i.(*Interval).String())
		}
	}
	for i, c := range n.children {
		buf.WriteString("(")
		c.describe(buf)
		buf.WriteString(fmt.Sprintf(":%s", c.Range))
		buf.WriteString(")")
		if i < len(n.children)-1 {
			buf.WriteString(n.interfaces[i].(*Interval).String())
		}
	}
}

func (tree *Tree) isKeyInRange(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	return tree.root.isKeyInRange(t, nil, nil)
}

func (n *node) isKeyInRange(t *testing.T, min, max Comparable) bool {
	t.Logf("%v, min: %v, max: %v", n, min, max)
	for _, i := range n.interfaces {
		start := i.Range().Start
		t.Log(i.Range())
		if min != nil && start.Compare(min) < 0 {
			return false
		}
		if max != nil && start.Compare(max) > 0 {
			return false
		}
	}
	oldMin, oldMax := min, max
	for i, c := range n.children {
		min, max := oldMin, oldMax
		if i != 0 {
			min = n.interfaces[i-1].Range().Start
		}
		if i != len(n.children)-1 {
			max = n.interfaces[i].Range().Start
		}
		if !c.isKeyInRange(t, min, max) {
			return false
		}
	}
	return true
}

func (tree *Tree) isSorted(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	return tree.root.isSorted(t)
}

func (n *node) isSorted(t *testing.T) bool {
	for _, c := range n.children {
		if !c.isSorted(t) {
			return false
		}
	}
	if !sort.IsSorted(n.interfaces) {
		return false
	}
	if !sort.IsSorted(n.children) {
		return false
	}
	return true
}

func (tree *Tree) isLeafSameDepth(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	height := -1
	for node := tree.root; ; {
		height++
		if len(node.children) == 0 {
			break
		}
		node = node.children[0]
	}
	t.Logf("tree height: %d", height)
	return tree.root.isDepthEqualToHeight(t, 0, height)
}

func (n *node) isDepthEqualToHeight(t *testing.T, depth, height int) bool {
	if len(n.children) == 0 {
		return depth == height
	}
	for _, c := range n.children {
		if !c.isDepthEqualToHeight(t, depth+1, height) {
			return false
		}
	}
	return true
}

func (tree *Tree) isCountAllowed(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	return tree.root.isCountAllowed(t, tree.minInterfaces(), tree.maxInterfaces(), true)
}

func (n *node) isCountAllowed(t *testing.T, minInterfaces, maxInterfaces int, root bool) bool {
	iLen := len(n.interfaces)
	cLen := len(n.children)
	if !root {
		iAllowed := minInterfaces <= iLen && iLen <= maxInterfaces
		if !iAllowed {
			return false
		}
	}
	if cLen > 0 {
		cAllowed := cLen == iLen+1
		if !cAllowed {
			return false
		}
		for _, c := range n.children {
			allowed := c.isCountAllowed(t, minInterfaces, maxInterfaces, false)
			if !allowed {
				return false
			}
		}
	}
	return true
}

// Does every node correctly annotate the range of its children.
func (tree *Tree) isIntervalInRange(t *testing.T) bool {
	if tree.isEmpty() {
		return true
	}
	return tree.root.isIntervalInRange(t)
}

func (n *node) isIntervalInRange(t *testing.T) bool {
	for _, c := range n.children {
		if !c.isIntervalInRange(t) {
			return false
		}
	}
	r := n.bound()
	if !n.Range.Equal(r) {
		t.Errorf("%v expected range %v, got %v", n, r, n.Range)
		return false
	}
	return true
}

func (r *Range) combine(other Range) {
	if r.Start.Compare(other.Start) > 0 {
		r.Start = other.Start
	}
	if r.End.Compare(other.End) < 0 {
		r.End = other.End
	}
}

func (n *node) bound() Range {
	r := n.interfaces[0].Range()
	ptr := &r
	for _, e := range n.interfaces[1:] {
		ptr.combine(e.Range())
	}
	for _, c := range n.children {
		ptr.combine(c.Range)
	}
	return r
}

func check(t *testing.T, tree *Tree) {
	t.Logf("tree: %s", tree.describe())
	if !tree.isLeafSameDepth(t) {
		t.Error("Not all the leaves have the same depth as the tree height")
	}
	if !tree.isCountAllowed(t) {
		t.Error("Not all the nodes have allowed key count and child node count")
	}
	if !tree.isIntervalInRange(t) {
		t.Error("Not all the nodes bound all the intervals in its subtree with its Range field")
	}
	if !tree.isSorted(t) {
		t.Error("Not all the nodes have its interfaces and children fields sorted")
	}
	if !tree.isKeyInRange(t) {
		t.Error("not all the nodes keep node keys (range.start) in range")
	}
}

func checkFastDelete(t *testing.T, tree *Tree, ivs interfaces, count int) {
	for _, iv := range ivs[:count] {
		if err := tree.Delete(iv, true); err != nil {
			t.Errorf("delete error: %s", err)
		}
		// Unlike fast insert, AdjustRanges must be called after each fast delete. Otherwise, the
		// following fast deletes may go wrong.
		tree.AdjustRanges()
	}
	checkWithLen(t, tree, len(ivs)-count)
}

func checkWithLen(t *testing.T, tree *Tree, l int) {
	if tree.Len() != l {
		t.Errorf("expected tree length %d, got %d\n", l, tree.Len())
	}
	check(t, tree)
}

func checkEqualIntervals(t *testing.T, actual, expected interfaces) {
	sort.Sort(actual)
	sort.Sort(expected)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("expected intervals %v, got %v", expected, actual)
	}
}

func checkTraversal(t *testing.T, tree *Tree, ivs interfaces) {
	// Get, GetWithOverlapper
	r := Range{Comparable{0x0}, Comparable{0x1}}
	expectedIntervals := interfaces{ivs[0], ivs[2], ivs[4]}
	checkEqualIntervals(t, tree.Get(r), expectedIntervals)
	checkEqualIntervals(t, tree.GetWithOverlapper(r, ExclusiveOverlapper), interfaces{ivs[0]})

	// DoMatching
	var overlapped interfaces
	tree.DoMatching(func(e Interface) bool {
		overlapped = append(overlapped, e)
		return false
	}, r)
	checkEqualIntervals(t, overlapped, expectedIntervals)

	// Do
	var all interfaces
	tree.Do(func(e Interface) bool {
		all = append(all, e)
		return false
	})
	checkEqualIntervals(t, all, ivs)
}

func intervals() interfaces {
	ivs := interfaces{
		&Interval{Range{Comparable{0}, Comparable{2}}, 0},
		&Interval{Range{Comparable{2}, Comparable{4}}, 0},
		&Interval{Range{Comparable{1}, Comparable{6}}, 0},
		&Interval{Range{Comparable{3}, Comparable{4}}, 0},
		&Interval{Range{Comparable{1}, Comparable{3}}, 0},
		&Interval{Range{Comparable{4}, Comparable{6}}, 0},
		&Interval{Range{Comparable{5}, Comparable{8}}, 0},
		&Interval{Range{Comparable{6}, Comparable{8}}, 0},
		&Interval{Range{Comparable{5}, Comparable{9}}, 0},
		&Interval{Range{Comparable{0x11}, Comparable{0x13}}, 0},
		&Interval{Range{Comparable{0x14}, Comparable{0x16}}, 0},
		&Interval{Range{Comparable{0x15}, Comparable{0x18}}, 0},
		&Interval{Range{Comparable{0x10}, Comparable{0x12}}, 0},
		&Interval{Range{Comparable{0x20}, Comparable{0x62}}, 0},
		&Interval{Range{Comparable{0x24}, Comparable{0xA0}}, 0},
		&Interval{Range{Comparable{0x31}, Comparable{0x63}}, 0},
		&Interval{Range{Comparable{0x44}, Comparable{0x56}}, 0},
		&Interval{Range{Comparable{0x45}, Comparable{0x68}}, 0},
		&Interval{Range{Comparable{0x30}, Comparable{0x72}}, 0},
		&Interval{Range{Comparable{0x30}, Comparable{0x52}}, 0},
		&Interval{Range{Comparable{0x44}, Comparable{0xB0}}, 0},
	}
	for i, iv := range ivs {
		iv.(*Interval).id = uintptr(i)
	}
	return ivs
}

func TestSmallTree(t *testing.T) {
	tree := NewWithDegree(InclusiveOverlapper, 2)
	ivs := intervals()

	// Insert
	for i, iv := range ivs {
		if err := tree.Insert(iv, false); err != nil {
			t.Errorf("insert error: %s", err)
		}
		checkWithLen(t, tree, i+1)
	}

	checkTraversal(t, tree, ivs)

	// Delete
	l := tree.Len()
	for i, iv := range ivs {
		if err := tree.Delete(iv, false); err != nil {
			t.Errorf("delete error: %s", err)
		}
		checkWithLen(t, tree, l-i-1)
	}
}

func TestSmallTreeWithFastOperations(t *testing.T) {
	tree := NewWithDegree(InclusiveOverlapper, 2)
	ivs := intervals()

	// Fast insert
	for _, iv := range ivs {
		if err := tree.Insert(iv, true); err != nil {
			t.Errorf("insert error: %s", err)
		}
	}
	tree.AdjustRanges()
	checkWithLen(t, tree, len(ivs))

	checkTraversal(t, tree, ivs)
	checkFastDelete(t, tree, ivs, tree.Len()/2)
}

func TestLargeTree(t *testing.T) {
	no := 0
	var i, j byte
	var ivs interfaces
	var maxByte byte = math.MaxUint8 - 1
	for i = 0; i <= maxByte; i++ {
		for j = i + 1; j <= maxByte; j++ {
			start := make(Comparable, 2, 2)
			start[0], start[1] = i, j
			end := make(Comparable, 2, 2)
			end[0], end[1] = i+1, j+1
			ivs = append(ivs, &Interval{Range{start, end}, uintptr(no)})
			no++
		}
	}

	tree := NewWithDegree(ExclusiveOverlapper, *degree)
	for _, iv := range ivs {
		if err := tree.Insert(iv, true); err != nil {
			t.Errorf("fast insert error: %s\n", err)
		}
	}
	tree.AdjustRanges()
	checkWithLen(t, tree, no)
	checkFastDelete(t, tree, ivs, 100)
}

func toBytes(b *testing.B, n uint32) Comparable {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, n); err != nil {
		b.Fatalf("binary.Write error: %s", err)
	}
	return Comparable(buf.Bytes())
}

func interval(b *testing.B, start, end uint32) Interface {
	iv := &Interval{Range{toBytes(b, start), toBytes(b, end)}, uintptr(start)}
	return iv
}

const (
	intervalLen = 10
)

func generateIntervals(b *testing.B) interfaces {
	var ivs interfaces
	for i := 0; i < b.N; i++ {
		n := uint32(i)
		ivs = append(ivs, interval(b, n, n+intervalLen))
	}
	return ivs
}

func initBenchmark(b *testing.B) interfaces {
	ivs := generateIntervals(b)
	return ivs
}

func BenchmarkInsert(b *testing.B) {
	b.StopTimer()
	ivs := initBenchmark(b)
	tree := NewWithDegree(InclusiveOverlapper, *degree)
	b.StartTimer()
	for _, e := range ivs {
		if err := tree.Insert(e, false); err != nil {
			b.Fatalf("insert error: %s", err)
		}
	}
}

func BenchmarkFastInsert(b *testing.B) {
	b.StopTimer()
	ivs := initBenchmark(b)
	tree := NewWithDegree(InclusiveOverlapper, *degree)
	b.StartTimer()
	for _, iv := range ivs {
		if err := tree.Insert(iv, true); err != nil {
			b.Fatalf("fast insert error: %s", err)
		}
	}
	tree.AdjustRanges()
}

func loadTree(b *testing.B) (ivs interfaces, tree *Tree) {
	ivs = initBenchmark(b)
	tree = NewWithDegree(InclusiveOverlapper, *degree)
	for _, iv := range ivs {
		if err := tree.Insert(iv, false); err != nil {
			b.Fatalf("fast insert error: %s", err)
		}
	}
	tree.AdjustRanges()
	return
}

func BenchmarkDelete(b *testing.B) {
	b.StopTimer()
	ivs, tree := loadTree(b)
	b.StartTimer()
	for _, iv := range ivs {
		if err := tree.Delete(iv, false); err != nil {
			b.Fatalf("delete error: %s", err)
		}
	}
	if tree.Len() != 0 {
		b.Errorf("expectecd tree length %d, got %d", 0, tree.Len())
	}
}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()
	ivs, tree := loadTree(b)
	b.StartTimer()
	for _, iv := range ivs {
		ptr := iv.(*Interval)
		tree.Get(Range{ptr.r.Start, ptr.r.End})
	}
}
