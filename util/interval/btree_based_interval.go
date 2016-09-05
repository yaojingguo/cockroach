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
// This code is based on: https://github.com/google/btree
//
// Author: Jingguo Yao (yaojingguo@gmail.com)

package interval

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

const (
	// DefaultFreeListSize is the defalut free list size
	DefaultFreeListSize = 32
	// DefaultBTreeDegree is the default B-tree degree. A benchmarks shows the interval tree perform
	// best with this degree.
	DefaultBTreeDegree = 32
)

// toRemove details what interface to remove in a node.remove call.
type toRemove int

const (
	removeItem toRemove = iota // removes the given interface
	removeMin                  // removes smallest interface in the subtree
	removeMax                  // removes largest interface in the subtree
)

// FreeList represents a free list of btree nodes. By default each
// B-tree has its own FreeList, but multiple B-trees can share the same
// FreeList.
// Two B-trees using the same freelist are not safe for concurrent write access.
type FreeList struct {
	freelist []*node
}

// NewFreeList creates a new free list.
// size is the maximum size of the returned free list.
func NewFreeList(size int) *FreeList {
	return &FreeList{freelist: make([]*node, 0, size)}
}

func (f *FreeList) newNode() (n *node) {
	index := len(f.freelist) - 1
	if index < 0 {
		n = new(node)
		return
	}
	f.freelist, n = f.freelist[:index], f.freelist[index]
	return
}

func (f *FreeList) freeNode(n *node) {
	if len(f.freelist) < cap(f.freelist) {
		f.freelist = append(f.freelist, n)
	}
}

// NewBTree creates a new interval tree with the given overlapper function and the default B-tree degree.
func NewBTree(overlapper Overlapper) *BTree {
	return NewBTreeWithDegree(overlapper, DefaultBTreeDegree)
}

// NewBTreeWithDegree creates a new interval tree with the given overlapper function and the given
// degree.
//
// NewBTreeWithDegree(overlapper, 2), for example, will create a 2-3-4 tree (each node contains 1-3 Interfaces and
// 2-4 children).
func NewBTreeWithDegree(overlapper Overlapper, degree int) *BTree {
	return NewBTreeWithDegreeAndFreeList(overlapper, degree, NewFreeList(DefaultFreeListSize))
}

// NewBTreeWithDegreeAndFreeList creates a new interval tree with the given overlapper function, the
// given degree and the given node free list.
func NewBTreeWithDegreeAndFreeList(overlapper Overlapper, degree int, f *FreeList) *BTree {
	if degree <= 1 {
		panic("bad degree")
	}
	return &BTree{
		degree:     degree,
		freelist:   f,
		overlapper: overlapper,
	}
}

// interfaces stores Interfaces sorted by Range().End in a node.
type interfaces []Interface

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *interfaces) insertAt(index int, e Interface) {
	oldLen := len(*s)
	*s = append(*s, nil)
	if index < oldLen {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = e
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *interfaces) removeAt(index int) Interface {
	e := (*s)[index]
	(*s)[index] = nil
	copy((*s)[index:], (*s)[index+1:])
	*s = (*s)[:len(*s)-1]
	return e
}

// pop removes and returns the last element in the list.
func (s *interfaces) pop() (out Interface) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// find returns the index where the given Interface should be inserted into this
// list. 'found' is true if the interface already exists in the list at the given
// index.
func (s interfaces) find(e Interface) (index int, found bool) {
	i := sort.Search(len(s), func(i int) bool {
		return Compare(e, s[i]) < 0
	})
	if i > 0 && Equal(s[i-1], e) {
		return i - 1, true
	}
	return i, false
}

// children stores child nodes sorted by Range.End in a node.
type children []*node

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *children) insertAt(index int, n *node) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = n
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *children) removeAt(index int) *node {
	n := (*s)[index]
	(*s)[index] = nil
	copy((*s)[index:], (*s)[index+1:])
	*s = (*s)[:len(*s)-1]
	return n
}

// pop removes and returns the last element in the list.
func (s *children) pop() (out *node) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// node is an internal node in a tree.
//
// It must at all times maintain the invariant that either
//   * len(children) == 0, len(interfaces) unconstrained
//   * len(children) == len(interfaces) + 1
type node struct {
	// Range is the node range which covers all the ranges in the subtree rooted at the node.
	// Range.Start is the leftmost position. Range.End is the rightmost position. Here we follow the
	// approach employed by https://github.com/biogo/store/tree/master/interval since it make it easy
	// to analyze the traversal of intervals which overlaps with a given interval. CRLS only uses
	// Range.End.
	Range      Range
	interfaces interfaces
	children   children
	t          *BTree
}

// split splits the given node at the given index. The current node shrinks,
// and this function returns the Interface that existed at that index and a new node
// containing all interfaces/children after it. Before splitting:
//
//          +-----------+
//          |   x y z   |
//          ---/-/-\-\--+
//
// After splitting:
//
//          +-----------+
//          |     y     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         x |     | z         |
// +-----------+     +-----------+
//
func (n *node) split(i int, fast bool) (Interface, *node) {
	e := n.interfaces[i]
	second := n.t.newNode()
	second.interfaces = append(second.interfaces, n.interfaces[i+1:]...)
	n.interfaces = n.interfaces[:i]
	if len(n.children) > 0 {
		second.children = append(second.children, n.children[i+1:]...)
		n.children = n.children[:i+1]
	}
	if !fast {
		// adjust range for the first split part
		oldRangeEnd := n.Range.End
		n.Range.End = n.rangeEnd()

		// adjust ragne for the second split part
		second.Range.Start = second.rangeStart()
		if n.Range.End.Equal(oldRangeEnd) || e.Range().End.Equal(oldRangeEnd) {
			second.Range.End = second.rangeEnd()
		} else {
			second.Range.End = oldRangeEnd
		}
	}
	return e, second
}

// maybeSplitChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred.
func (n *node) maybeSplitChild(i int, fast bool) bool {
	maxInterfaces := n.t.maxInterfaces()
	if len(n.children[i].interfaces) < maxInterfaces {
		return false
	}
	first := n.children[i]
	e, second := first.split(maxInterfaces/2, fast)
	n.interfaces.insertAt(i, e)
	n.children.insertAt(i+1, second)
	return true
}

// insert inserts an Interface into the subtree rooted at this node, making sure
// no nodes in the subtree exceed maxInterfaces Interfaces.
func (n *node) insert(e Interface, fast bool) (out Interface, extended bool) {
	i, found := n.interfaces.find(e)
	if found {
		out = n.interfaces[i]
		n.interfaces[i] = e
		return
	}
	if len(n.children) == 0 {
		n.interfaces.insertAt(i, e)
		out = nil
		if !fast {
			if i == 0 {
				extended = true
				n.Range.Start = n.interfaces[0].Range().Start
			}
			if n.interfaces[i].Range().End.Compare(n.Range.End) > 0 {
				extended = true
				n.Range.End = n.interfaces[i].Range().End
			}
		}
		return
	}
	if n.maybeSplitChild(i, fast) {
		inTree := n.interfaces[i]
		switch Compare(e, inTree) {
		case -1:
			// no change, we want first split node
		case 1:
			i++ // we want second split node
		default:
			out = n.interfaces[i]
			n.interfaces[i] = e
			return
		}
	}
	out, extended = n.children[i].insert(e, fast)
	if !fast && extended {
		extended = false
		if i == 0 && n.children[0].Range.Start.Compare(n.Range.Start) < 0 {
			extended = true
			n.Range.Start = n.children[0].Range.Start
		}
		if n.children[i].Range.End.Compare(n.Range.End) > 0 {
			extended = true
			n.Range.End = n.children[i].Range.End
		}
	}
	return
}

func (t *BTree) isEmpty() bool {
	return t.root == nil || len(t.root.interfaces) == 0
}

// Get implements the Tree interface.
func (t *BTree) Get(r Range) (o []Interface) {
	return t.GetWithOverlapper(r, t.overlapper)
}

// GetWithOverlapper implements the Tree interface.
func (t *BTree) GetWithOverlapper(r Range, overlapper Overlapper) (o []Interface) {
	if err := rangeError(r); err != nil {
		return
	}
	if !t.overlappable(r) {
		return
	}
	t.root.doMatch(func(e Interface) (done bool) { o = append(o, e); return }, r, overlapper)
	return
}

// DoMatching implements the Tree interface.
func (t *BTree) DoMatching(fn Operation, r Range) bool {
	if err := rangeError(r); err != nil {
		return false
	}
	if !t.overlappable(r) {
		return false
	}
	return t.root.doMatch(fn, r, t.overlapper)
}

func (t *BTree) overlappable(r Range) bool {
	if t.isEmpty() || !t.overlapper.Overlap(r, t.root.Range) {
		return false
	}
	return true
}

// doMatch for InclusiveOverlapper.
func (n *node) inclusiveDoMatch(fn Operation, r Range, overlapper Overlapper) (done bool) {
	length := sort.Search(len(n.interfaces), func(i int) bool {
		return n.interfaces[i].Range().Start.Compare(r.End) > 0
	})

	if len(n.children) == 0 {
		for _, e := range n.interfaces[:length] {
			if r.Start.Compare(e.Range().End) <= 0 {
				if done = fn(e); done {
					return
				}
			}
		}
		return
	}

	for i := 0; i < length; i++ {
		c := n.children[i]
		if r.Start.Compare(c.Range.End) <= 0 {
			if done = c.inclusiveDoMatch(fn, r, overlapper); done {
				return
			}
		}
		e := n.interfaces[i]
		if r.Start.Compare(e.Range().End) <= 0 {
			if done = fn(e); done {
				return
			}
		}
	}

	if overlapper.Overlap(r, n.children[length].Range) {
		done = n.children[length].inclusiveDoMatch(fn, r, overlapper)
	}
	return
}

// doMatch for ExclusiveOverlapper.
func (n *node) exclusiveDoMatch(fn Operation, r Range, overlapper Overlapper) (done bool) {
	length := sort.Search(len(n.interfaces), func(i int) bool {
		return n.interfaces[i].Range().Start.Compare(r.End) >= 0
	})

	if len(n.children) == 0 {
		for _, e := range n.interfaces[:length] {
			if r.Start.Compare(e.Range().End) < 0 {
				if done = fn(e); done {
					return
				}
			}
		}
		return
	}

	for i := 0; i < length; i++ {
		c := n.children[i]
		if r.Start.Compare(c.Range.End) < 0 {
			if done = c.exclusiveDoMatch(fn, r, overlapper); done {
				return
			}
		}
		e := n.interfaces[i]
		if r.Start.Compare(e.Range().End) < 0 {
			if done = fn(e); done {
				return
			}
		}
	}

	if overlapper.Overlap(r, n.children[length].Range) {
		done = n.children[length].exclusiveDoMatch(fn, r, overlapper)
	}
	return
}

// benchmarks show that if Comparable.Compare is invoked directly instead of through an indirection
// with Overlapper, Insert, Delete and a traversal to visit overlapped intervals have a noticeable
// speed-up. So two versions of doMatch are created. One is for InclusiveOverlapper. The other is
// for ExclusiveOverlapper.
func (n *node) doMatch(fn Operation, r Range, overlapper Overlapper) (done bool) {
	if overlapper == InclusiveOverlapper {
		return n.inclusiveDoMatch(fn, r, overlapper)
	}
	return n.exclusiveDoMatch(fn, r, overlapper)
}

// Do implements the Tree interface.
func (t *BTree) Do(fn Operation) bool {
	if t.root == nil {
		return false
	}
	return t.root.do(fn)
}

func (n *node) do(fn Operation) (done bool) {
	cLen := len(n.children)
	if cLen == 0 {
		for _, e := range n.interfaces {
			if done = fn(e); done {
				return
			}
		}
		return
	}

	for i := 0; i < cLen-1; i++ {
		c := n.children[i]
		if done = c.do(fn); done {
			return
		}
		e := n.interfaces[i]
		if done = fn(e); done {
			return
		}
	}
	done = n.children[cLen-1].do(fn)
	return
}

// remove removes an interface from the subtree rooted at this node.
func (n *node) remove(e Interface, minInterfaces int, typ toRemove, fast bool) (out Interface, shrunk bool) {
	var i int
	var found bool
	switch typ {
	case removeMax:
		if len(n.children) == 0 {
			return n.removeFromLeaf(len(n.interfaces)-1, fast)
		}
		i = len(n.interfaces)
	case removeMin:
		if len(n.children) == 0 {
			return n.removeFromLeaf(0, fast)
		}
		i = 0
	case removeItem:
		i, found = n.interfaces.find(e)
		if len(n.children) == 0 {
			if found {
				return n.removeFromLeaf(i, fast)
			}
			return
		}
	default:
		panic("invalid remove type")
	}
	// If we get to here, we have children.
	child := n.children[i]
	if len(child.interfaces) <= minInterfaces {
		out, shrunk = n.growChildAndRemove(i, e, minInterfaces, typ, fast)
		return
	}
	// Either we had enough interfaces to begin with, or we've done some
	// merging/stealing, because we've got enough now and we're ready to return
	// stuff.
	if found {
		// The interface exists at index 'i', and the child we've selected can give us a
		// predecessor, since if we've gotten here it's got > minInterfaces interfaces in it.
		out = n.interfaces[i]
		// We use our special-case 'remove' call with typ=removeMax to pull the
		// predecessor of interface i (the rightmost leaf of our immediate left child)
		// and set it into where we pulled the interface from.
		n.interfaces[i], _ = child.remove(nil, minInterfaces, removeMax, fast)
		if !fast {
			shrunk = n.adjustRangeEndForRemoval(out, nil)
		}
		return
	}
	// Final recursive call. Once we're here, we know that the interface isn't in this
	// node and that the child is big enough to remove from.
	out, shrunk = child.remove(e, minInterfaces, typ, fast)
	if !fast && shrunk {
		shrunkOnStart := false
		if i == 0 {
			if n.Range.Start.Compare(child.Range.Start) < 0 {
				shrunkOnStart = true
				n.Range.Start = child.Range.Start
			}
		}
		shrunkOnEnd := n.adjustRangeEndForRemoval(out, nil)
		shrunk = shrunkOnStart || shrunkOnEnd
	}
	return
}

// adjustRangeEndForRemoval adjusts Range.End for the node after an interface and/or a child is
// removed.
func (n *node) adjustRangeEndForRemoval(e Interface, c *node) (decreased bool) {
	if (e != nil && e.Range().End.Equal(n.Range.End)) || (c != nil && c.Range.End.Equal(n.Range.End)) {
		newEnd := n.rangeEnd()
		if n.Range.End.Compare(newEnd) > 0 {
			decreased = true
			n.Range.End = newEnd
		}
	}
	return
}

// removeFromLeaf removes children[i] from the leaf node.
func (n *node) removeFromLeaf(i int, fast bool) (out Interface, shrunk bool) {
	if i == len(n.interfaces)-1 {
		out = n.interfaces.pop()
	} else {
		out = n.interfaces.removeAt(i)
	}
	if !fast && len(n.interfaces) > 0 {
		shrunkOnStart := false
		if i == 0 {
			oldStart := n.Range.Start
			n.Range.Start = n.interfaces[0].Range().Start
			if !n.Range.Start.Equal(oldStart) {
				shrunkOnStart = true
			}
		}
		shrunkOnEnd := n.adjustRangeEndForRemoval(out, nil)
		shrunk = shrunkOnStart || shrunkOnEnd
	}
	return
}

// growChildAndRemove grows child 'i' to make sure it's possible to remove an
// Interface from it while keeping it at minInterfaces, then calls remove to actually
// remove it.
//
// Most documentation says we have to do two sets of special casing:
//   1) interface is in this node
//   2) interface is in child
// In both cases, we need to handle the two subcases:
//   A) node has enough values that it can spare one
//   B) node doesn't have enough values
// For the latter, we have to check:
//   a) left sibling has node to spare
//   b) right sibling has node to spare
//   c) we must merge
// To simplify our code here, we handle cases #1 and #2 the same:
// If a node doesn't have enough Interfaces, we make sure it does (using a,b,c).
// We then simply redo our remove call, and the second time (regardless of
// whether we're in case 1 or 2), we'll have enough Interfaces and can guarantee
// that we hit case A.
func (n *node) growChildAndRemove(i int, e Interface, minInterfaces int, typ toRemove, fast bool) (out Interface, shrunk bool) {
	if i > 0 && len(n.children[i-1].interfaces) > minInterfaces {
		n.stealFromLeftChild(i, fast)
	} else if i < len(n.interfaces) && len(n.children[i+1].interfaces) > minInterfaces {
		n.stealFromRightChild(i, fast)
	} else {
		if i >= len(n.interfaces) {
			i--
		}
		n.mergeWithRightChild(i, fast)
	}
	return n.remove(e, minInterfaces, typ, fast)
}

// Steal from left child. Before stealing:
//
//          +-----------+
//          |     y     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         x |     |           |
// +----------\+     +-----------+
//             \
//              v
//              a
//
// After stealing:
//
//          +-----------+
//          |     x     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |           |     | y         |
// +-----------+     +/----------+
//                   /
//                  v
//                  a
//
func (n *node) stealFromLeftChild(i int, fast bool) {
	// steal
	stealTo := n.children[i]
	stealFrom := n.children[i-1]
	x := stealFrom.interfaces.pop()
	y := n.interfaces[i-1]
	stealTo.interfaces.insertAt(0, y)
	n.interfaces[i-1] = x
	var a *node
	if len(stealFrom.children) > 0 {
		a = stealFrom.children.pop()
		stealTo.children.insertAt(0, a)
	}

	if !fast {
		// adjust range for stealFrom
		stealFrom.adjustRangeEndForRemoval(x, a)

		// adjust range for stealTo
		stealTo.Range.Start = stealTo.rangeStart()
		if y.Range().End.Compare(stealTo.Range.End) > 0 {
			stealTo.Range.End = y.Range().End
		}
		if a != nil && a.Range.End.Compare(stealTo.Range.End) > 0 {
			stealTo.Range.End = a.Range.End
		}
	}
}

// Steal from right child. Before stealing:
//
//          +-----------+
//          |     y     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |           |     | x         |
// +---------- +     +/----------+
//                   /
//                  v
//                  a
//
// After stealing:
//
//          +-----------+
//          |     x     |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         y |     |           |
// +----------\+     +-----------+
//             \
//              v
//              a
//
func (n *node) stealFromRightChild(i int, fast bool) {
	// steal
	stealTo := n.children[i]
	stealFrom := n.children[i+1]
	x := stealFrom.interfaces.removeAt(0)
	y := n.interfaces[i]
	stealTo.interfaces = append(stealTo.interfaces, y)
	n.interfaces[i] = x
	var a *node
	if len(stealFrom.children) > 0 {
		a = stealFrom.children.removeAt(0)
		stealTo.children = append(stealTo.children, a)
	}

	if !fast {
		// adjust range for stealFrom
		stealFrom.Range.Start = stealFrom.rangeStart()
		stealFrom.adjustRangeEndForRemoval(x, a)

		// adjust range for stealTo
		if y.Range().End.Compare(stealTo.Range.End) > 0 {
			stealTo.Range.End = y.Range().End
		}
		if a != nil && a.Range.End.Compare(stealTo.Range.End) > 0 {
			stealTo.Range.End = a.Range.End
		}
	}
}

// Merge with right child. Before merging:
//
//          +-----------+
//          |   u y v   |
//          -----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         x |     | z         |
// +---------- +     +-----------+
//
// After merging:
//
//          +-----------+
//          |    u v    |
//          ------|-----+
//                |
//                v
//          +-----------+
//          |   x y z   |
//          +---------- +
//
func (n *node) mergeWithRightChild(i int, fast bool) {
	// merge
	y := n.interfaces.removeAt(i)
	child := n.children[i]
	mergeChild := n.children.removeAt(i + 1)
	child.interfaces = append(child.interfaces, y)
	child.interfaces = append(child.interfaces, mergeChild.interfaces...)
	child.children = append(child.children, mergeChild.children...)

	if !fast {
		if y.Range().End.Compare(child.Range.End) > 0 {
			child.Range.End = y.Range().End
		}
		if mergeChild.Range.End.Compare(child.Range.End) > 0 {
			child.Range.End = mergeChild.Range.End
		}
		n.t.freeNode(mergeChild)
	}
}

// Used for testing/debugging purposes.
func (n *node) print(w io.Writer, level int) {
	fmt.Fprintf(w, "%sNODE:%v\n", strings.Repeat("  ", level), n.interfaces)
	for _, c := range n.children {
		c.print(w, level+1)
	}
}

// BTree is a B-tree based interval tree.
//
// BTree stores Instances in an ordered structure, allowing easy insertion,
// removal, and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTree struct {
	degree     int
	length     int
	root       *node
	freelist   *FreeList
	overlapper Overlapper
}

// adjustRange sets the Range to the maximum extent of the childrens' Range
// spans and its range spans.
func (n *node) adjustRange() {
	n.Range.Start = n.rangeStart()
	n.Range.End = n.rangeEnd()
}

// rangeStart returns the leftmost position for the node range, assuming that its children have
// correct range extents.
func (n *node) rangeStart() Comparable {
	minStart := n.interfaces[0].Range().Start
	if len(n.children) > 0 {
		minStart = n.children[0].Range.Start
	}
	return minStart
}

// rangeEnd returns the rightmost position for the node range, assuming that its children have
// correct range extents.
func (n *node) rangeEnd() Comparable {
	if len(n.interfaces) == 0 {
		maxEnd := n.children[0].Range.End
		for _, c := range n.children[1:] {
			if end := c.Range.End; maxEnd.Compare(end) < 0 {
				maxEnd = end
			}
		}
		return maxEnd
	}
	maxEnd := n.interfaces[0].Range().End
	for _, e := range n.interfaces[1:] {
		if end := e.Range().End; maxEnd.Compare(end) < 0 {
			maxEnd = end
		}
	}
	for _, c := range n.children {
		if end := c.Range.End; maxEnd.Compare(end) < 0 {
			maxEnd = end
		}
	}
	return maxEnd
}

// AdjustRanges implements the Tree interface.
func (t *BTree) AdjustRanges() {
	if t.isEmpty() {
		return
	}
	t.root.adjustRanges()
}

func (n *node) adjustRanges() {
	for _, c := range n.children {
		c.adjustRanges()
	}
	n.adjustRange()
}

// maxInterfaces returns the max number of Interfaces to allow per node.
func (t *BTree) maxInterfaces() int {
	return t.degree*2 - 1
}

// minInterfaces returns the min number of Interfaces to allow per node (ignored for the
// root node).
func (t *BTree) minInterfaces() int {
	return t.degree - 1
}

func (t *BTree) newNode() (n *node) {
	n = t.freelist.newNode()
	n.t = t
	return
}

func (t *BTree) freeNode(n *node) {
	for i := range n.interfaces {
		n.interfaces[i] = nil // clear to allow GC
	}
	n.interfaces = n.interfaces[:0]
	for i := range n.children {
		n.children[i] = nil // clear to allow GC
	}
	n.children = n.children[:0]
	n.t = nil // clear to allow GC
	t.freelist.freeNode(n)
}

// Insert implements the Tree interface.
func (t *BTree) Insert(e Interface, fast bool) (err error) {
	if err = isValidInterface(e); err != nil {
		return
	}

	if t.root == nil {
		t.root = t.newNode()
		t.root.interfaces = append(t.root.interfaces, e)
		t.length++
		if !fast {
			t.root.Range.Start = e.Range().Start
			t.root.Range.End = e.Range().End
		}
		return nil
	} else if len(t.root.interfaces) >= t.maxInterfaces() {
		oldroot := t.root
		t.root = t.newNode()
		if !fast {
			t.root.Range.Start = oldroot.Range.Start
			t.root.Range.End = oldroot.Range.End
		}
		e2, second := oldroot.split(t.maxInterfaces()/2, fast)
		t.root.interfaces = append(t.root.interfaces, e2)
		t.root.children = append(t.root.children, oldroot, second)
	}
	out, _ := t.root.insert(e, fast)
	if out == nil {
		t.length++
	}
	return
}

// Delete implements the Tree interface.
func (t *BTree) Delete(e Interface, fast bool) (err error) {
	if err = isValidInterface(e); err != nil {
		return
	}
	if !t.overlappable(e.Range()) {
		return
	}
	t.delete(e, removeItem, fast)
	return
}

func (t *BTree) delete(e Interface, typ toRemove, fast bool) Interface {
	out, _ := t.root.remove(e, t.minInterfaces(), typ, fast)
	if len(t.root.interfaces) == 0 && len(t.root.children) > 0 {
		oldroot := t.root
		t.root = t.root.children[0]
		t.freeNode(oldroot)
	}
	if out != nil {
		t.length--
	}
	return out
}

// Len implements the Tree interface.
func (t *BTree) Len() int {
	return t.length
}
