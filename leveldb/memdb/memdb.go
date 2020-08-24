// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package memdb provides in-memory key/value database implementation.
// 内存kv数据库存储实现, 底层利用跳表来实现, 读写操作时间复杂度为O(log n), 操作效率和平衡树相当, 实现起来更简单
package memdb

import (
	"math/rand"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Common errors.
var (
	ErrNotFound     = errors.ErrNotFound
	ErrIterReleased = errors.New("leveldb/memdb: iterator released")
)

// 最大树高
const tMaxHeight = 12

// 跳表的目的是借助概率平衡来构建一个快速且简单的数据结构取代平衡树, 避免红黑树/avl树复杂的节点旋转操作
//
// header |  data 自下而上增加索引
// head3  -> r30 ----------------------------------------------------------------> nil
// head2  -> r20 ---------------> r21 --------> r22 -----------------------------> nil
// head1  -> r10 --------> r11 -> r12 --------> r13 ---------------> r14 --------> nil
// head0  -> r00 -> r01 -> r02 -> r03 -> r04 -> r05 -> r06 -> r07 -> r08 -> r09 -> nil
//            1      2      3      4      5      6      7      8      9      10
type dbIter struct {
	util.BasicReleaser
	p          *DB
	slice      *util.Range
	node       int // 节点编号
	forward    bool
	key, value []byte
	err        error
}

func (i *dbIter) fill(checkStart, checkLimit bool) bool {
	if i.node != 0 {
		n := i.p.nodeData[i.node] // 偏移
		// [0]         : KV offset
		// [1]         : Key length
		// [2]         : Value length
		// [3]         : Height
		// [3..height] : Next nodes
		m := n + i.p.nodeData[i.node+nKey] // key长度
		i.key = i.p.kvData[n:m]
		if i.slice != nil {
			switch {
			case checkLimit && i.slice.Limit != nil && i.p.cmp.Compare(i.key, i.slice.Limit) >= 0:
				fallthrough
			case checkStart && i.slice.Start != nil && i.p.cmp.Compare(i.key, i.slice.Start) < 0:
				i.node = 0
				goto bail
			}
		}
		i.value = i.p.kvData[m : m+i.p.nodeData[i.node+nVal]] // 获取值
		return true
	}
bail: // 保释???
	i.key = nil
	i.value = nil
	return false
}

// 验证是否为空?
func (i *dbIter) Valid() bool {
	return i.node != 0
}

// 移动到第一个键值对
func (i *dbIter) First() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil {
		i.node, _ = i.p.findGE(i.slice.Start, false)
	} else {
		i.node = i.p.nodeData[nNext]
	}
	return i.fill(false, true)
}

// 移动到最后一个键值对
func (i *dbIter) Last() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Limit != nil {
		i.node = i.p.findLT(i.slice.Limit)
	} else {
		i.node = i.p.findLast()
	}
	return i.fill(true, false)
}

// 定位到某一个key
func (i *dbIter) Seek(key []byte) bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil && i.p.cmp.Compare(key, i.slice.Start) < 0 {
		key = i.slice.Start
	}
	i.node, _ = i.p.findGE(key, false)
	return i.fill(false, true)
}

// 移动到下一个键值对
func (i *dbIter) Next() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if !i.forward {
			return i.First()
		}
		return false
	}
	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.nodeData[i.node+nNext] // 第0层的下一个节点
	return i.fill(false, true)
}

// 移动到前一个键值对
func (i *dbIter) Prev() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if i.forward {
			return i.Last()
		}
		return false
	}
	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.findLT(i.key)
	return i.fill(true, false)
}

func (i *dbIter) Key() []byte {
	return i.key
}

func (i *dbIter) Value() []byte {
	return i.value
}

func (i *dbIter) Error() error { return i.err }

// 释放迭代器
func (i *dbIter) Release() {
	if !i.Released() {
		i.p = nil
		i.node = 0
		i.key = nil
		i.value = nil
		i.BasicReleaser.Release()
	}
}

const (
	nKV = iota
	nKey
	nVal
	nHeight
	nNext
)

// DB is an in-memory key/value database.
type DB struct {
	cmp comparer.BasicComparer
	rnd *rand.Rand

	mu     sync.RWMutex
	kvData []byte
	// Node data:
	// [0]         : KV offset
	// [1]         : Key length
	// [2]         : Value length
	// [3]         : Height
	// [3..height] : Next nodes
	nodeData  []int   // 看意思应该是树状数组, 每一个节点占用4, 记录元信息
	prevNode  [tMaxHeight]int
	maxHeight int
	n         int
	kvSize    int
}

func (p *DB) randHeight() (h int) {
	const branching = 4
	h = 1
	for h < tMaxHeight && p.rnd.Int()%branching == 0 {
		h++
	}
	return
}

// Must hold RW-lock if prev == true, as it use shared prevNode slice.
func (p *DB) findGE(key []byte, prev bool) (int, bool) {

	// header |  data 自下而上增加索引
	// head3  -> r30 ----------------------------------------------------------------> nil
	// head2  -> r20 ---------------> r21 --------> r22 -----------------------------> nil
	// head1  -> r10 --------> r11 -> r12 --------> r13 ---------------> r14 --------> nil
	// head0  -> r00 -> r01 -> r02 -> r03 -> r04 -> r05 -> r06 -> r07 -> r08 -> r09 -> nil
	//            1      2      3      4      5      6      7      8      9      10
	// 如上图, 找到10的路径获得的prevNode是[1,6,9,10]
	node := 0
	h := p.maxHeight - 1
	// 根据跳表高度选取最高层头结点
	for {
		next := p.nodeData[node+nNext+h]
		cmp := 1
		if next != 0 {
			o := p.nodeData[next]
			cmp = p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key)
		}
		if cmp < 0 {// 搜索该层的下一个节点
			// Keep searching in this list
			node = next
		} else {// 当前层已经匹配

			// 这里有一个奇怪的trade off
			// 如果prev == true 且 cmp == 0, 那么会进行下一次轮次循环读取, 需要额外进行一次Compare
			// 这里这么写的必要性是由于prev置为true时需要找到每一层的前置节点
			// 所以不能写成如下的样子, (会造成低层的前前置节点没有填上, 后续进行插入操作就会错误)
			// if prev {...}
			// if cmp == 0 {return ...}
			if prev { // 更新前置路径
				p.prevNode[h] = node
			} else if cmp == 0 { // 命中
				return next, true
			}
			if h == 0 {
				return next, cmp == 0
			}
			h-- // 保持当前node, 降低层高
		}
	}
}

func (p *DB) findLT(key []byte) int {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]
		o := p.nodeData[next]
		if next == 0 || p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key) >= 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

func (p *DB) findLast() int {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]
		if next == 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Put returns.
func (p *DB) Put(key []byte, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 找到一个k大于给定key的节点
	if node, exact := p.findGE(key, true); exact {
		// 找到了, 那就覆盖掉元信息
		// 注意之前放在kvData里面的信息不会被修改, 仅仅是append, 那么前一次put进来的值并不会被删掉
		kvOffset := len(p.kvData)
		p.kvData = append(p.kvData, key...)
		p.kvData = append(p.kvData, value...)

		// [0]         : KV offset
		// [1]         : Key length
		// [2]         : Value length
		// [3]         : Height
		// [3..height] : Next nodes
		p.nodeData[node] = kvOffset // 更新偏移
		// 更新value长度
		m := p.nodeData[node+nVal]
		p.nodeData[node+nVal] = len(value)

		// 更新总大小
		p.kvSize += len(value) - m
		return nil
	}

	// 如果没找到就要新创建一个节点了
	h := p.randHeight()
	if h > p.maxHeight { // 更新最大树高
		for i := p.maxHeight; i < h; i++ {
			p.prevNode[i] = 0
		}
		p.maxHeight = h
	}

	// 存入kv数据
	kvOffset := len(p.kvData)
	p.kvData = append(p.kvData, key...)
	p.kvData = append(p.kvData, value...)

	// Node
	node := len(p.nodeData)
	p.nodeData = append(p.nodeData, kvOffset, len(key), len(value), h) // 存入节点元数据

	// header |  data 注意构建的方向是自下而上
	// head0  -> r00 ----------------------------------------------------------------> nil
	// head1  -> r10 ---------------> r11 --------> r12 -----------------------------> nil
	// head2  -> r20 --------> r21 -> r22 --------> r23 ---------------> r24 --------> nil
	// head3  -> r30 -> r31 -> r32 -> r33 -> r34 -> r35 -> r36 -> r37 -> r38 -> r39 -> nil
	//            1      2      3      4      5      6      7      8      9      10
	for i, n := range p.prevNode[:h] { // 更新跳表, 更新节点在跳表每一层的下一个节点, prevNode表示查找过程中的前任节点
		m := n + nNext + i // 前置节点的第i个next节点
		p.nodeData = append(p.nodeData, p.nodeData[m]) // 在跳表中将新生成的节点插入到前置节点与前置节点的第i个后置节点之间
		p.nodeData[m] = node
	}

	p.kvSize += len(key) + len(value)
	p.n++
	return nil
}

// Delete deletes the value for the given key. It returns ErrNotFound if
// the DB does not contain the key.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (p *DB) Delete(key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	node, exact := p.findGE(key, true)
	if !exact { // 无效删除操作
		return ErrNotFound
	}

	// 更新前置节点
	h := p.nodeData[node+nHeight]
	for i, n := range p.prevNode[:h] {
		m := n + nNext + i
		p.nodeData[m] = p.nodeData[p.nodeData[m]+nNext+i]
	}

	// 尺寸变更
	p.kvSize -= p.nodeData[node+nKey] + p.nodeData[node+nVal]
	p.n--
	return nil
}

// Contains returns true if the given key are in the DB.
//
// It is safe to modify the contents of the arguments after Contains returns.
func (p *DB) Contains(key []byte) bool {
	p.mu.RLock()
	_, exact := p.findGE(key, false)
	p.mu.RUnlock()
	return exact
}

// Get gets the value for the given key. It returns error.ErrNotFound if the
// DB does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (p *DB) Get(key []byte) (value []byte, err error) {
	// Get找到的key是等于给定key
	p.mu.RLock()
	if node, exact := p.findGE(key, false); exact {
		o := p.nodeData[node] + p.nodeData[node+nKey]
		value = p.kvData[o : o+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// Find finds key/value pair whose key is greater than or equal to the
// given key. It returns ErrNotFound if the table doesn't contain
// such pair.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Find returns.
func (p *DB) Find(key []byte) (rkey, value []byte, err error) {
	// Find找到的是key大于等于的给定key的
	p.mu.RLock()
	if node, _ := p.findGE(key, false); node != 0 {
		n := p.nodeData[node]
		m := n + p.nodeData[node+nKey]
		rkey = p.kvData[n:m]
		value = p.kvData[m : m+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// NewIterator returns an iterator of the DB.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. However, the resultant key/value pairs are not guaranteed
// to be a consistent snapshot of the DB at a particular point in time.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Key() methods), its content should not be modified
// unless noted otherwise.
//
// The iterator must be released after use, by calling Release method.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (p *DB) NewIterator(slice *util.Range) iterator.Iterator {
	return &dbIter{p: p, slice: slice}
}

// Capacity returns keys/values buffer capacity.
func (p *DB) Capacity() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData)
}

// Size returns sum of keys and values length. Note that deleted
// key/value will not be accounted for, but it will still consume
// the buffer, since the buffer is append only.
func (p *DB) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.kvSize
}

// Free returns keys/values free buffer before need to grow.
func (p *DB) Free() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData) - len(p.kvData)
}

// Len returns the number of entries in the DB.
func (p *DB) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.n
}

// Reset resets the DB to initial empty state. Allows reuse the buffer.
func (p *DB) Reset() {
	p.mu.Lock()
	p.rnd = rand.New(rand.NewSource(0xdeadbeef))
	p.maxHeight = 1
	p.n = 0
	p.kvSize = 0
	p.kvData = p.kvData[:0]
	p.nodeData = p.nodeData[:nNext+tMaxHeight]
	p.nodeData[nKV] = 0
	p.nodeData[nKey] = 0
	p.nodeData[nVal] = 0
	p.nodeData[nHeight] = tMaxHeight
	for n := 0; n < tMaxHeight; n++ {
		p.nodeData[nNext+n] = 0
		p.prevNode[n] = 0
	}
	p.mu.Unlock()
}

// New creates a new initialized in-memory key/value DB. The capacity
// is the initial key/value buffer capacity. The capacity is advisory,
// not enforced.
//
// This DB is append-only, deleting an entry would remove entry node but not
// reclaim KV buffer.
//
// The returned DB instance is safe for concurrent use.
func New(cmp comparer.BasicComparer, capacity int) *DB {
	p := &DB{
		cmp:       cmp,
		rnd:       rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
		kvData:    make([]byte, 0, capacity),
		nodeData:  make([]int, 4+tMaxHeight),
	}
	p.nodeData[nHeight] = tMaxHeight
	return p
}
