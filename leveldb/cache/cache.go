// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package cache provides interface and implementation of a cache algorithms.
package cache

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/util"
)

// 缓存对于一个数据库读性能的影响十分巨大，倘若leveldb的每一次读取都会发生一次磁盘的IO，那么其整体效率将会非常低下
// 在leveldb中需要通过cache达到的目的使得在发生读取热数据时，尽量在cache中命中，避免IO读取
// 1. 已打开的sstable文件对象和相关元数据；
// 2. sstable中的dataBlock的内容；

// Cacher provides interface to implements a caching functionality.
// An implementation must be safe for concurrent use.
type Cacher interface {
	// Capacity returns cache capacity.
	Capacity() int

	// SetCapacity sets cache capacity.
	SetCapacity(capacity int)

	// Promote promotes the 'cache node'.
	Promote(n *Node)

	// Ban evicts the 'cache node' and prevent subsequent 'promote'.
	Ban(n *Node)

	// Evict evicts the 'cache node'.
	Evict(n *Node)

	// EvictNS evicts 'cache node' with the given namespace.
	EvictNS(ns uint64)

	// EvictAll evicts all 'cache node'.
	EvictAll()

	// Close closes the 'cache tree'
	Close() error
}

// Value is a 'cacheable object'. It may implements util.Releaser, if
// so the the Release method will be called once object is released.
// 可缓存对象
type Value interface{}

// NamespaceGetter provides convenient wrapper for namespace.
type NamespaceGetter struct {
	Cache *Cache
	NS    uint64
}

// Get simply calls Cache.Get() method.
// 最终获取Handle, 实际拿到的是Node指针
func (g *NamespaceGetter) Get(key uint64, setFunc func() (size int, value Value)) *Handle {
	return g.Cache.Get(g.NS, key, setFunc)
}

// The hash tables implementation is based on:
// "Dynamic-Sized Nonblocking Hash Tables", by Yujie Liu,
// Kunlong Zhang, and Michael Spear.
// ACM Symposium on Principles of Distributed Computing, Jul 2014.
// 这里实现的是基于桶的动态大小hashtable用来存储数据, 特点是resize期间不会阻塞其他的并发读写操作
// 论文的核心设想是
// 1. hash表通常由若干个bucket组成, 每一个bucket中会存储若干条被散列至此的数据项(golang map其实就是这么实现的)
// 2. 当hash表进行resize的时候, 需要将旧bucket中的数据重新散列到新bucket, 且这个操作并不是原子操作, 如果不进行一定并发安全设计的话, 会出现错误
// 3. 论文提出的概念是给bucket增加状态frozen状态, 简化数据转移的复杂度
const (
	mInitialSize           = 1 << 4
	mOverflowThreshold     = 1 << 5
	mOverflowGrowThreshold = 1 << 7
)

type mBucket struct {
	mu     sync.Mutex
	node   []*Node
	frozen bool
}

func (b *mBucket) freeze() []*Node {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.frozen {
		b.frozen = true
	}
	return b.node
}

// get/set一体的方法, 如果noset为false的话会返回一个用于缓存Value存放的槽
func (b *mBucket) get(r *Cache, h *mNode, hash uint32, ns, key uint64, noset bool) (done, added bool, n *Node) {
	b.mu.Lock()

	if b.frozen {
		b.mu.Unlock()
		return
	}

	// Scan the node.
	// 在桶里面寻找指定节点, get流程
	for _, n := range b.node {
		if n.hash == hash && n.ns == ns && n.key == key {
			// 增加引用计数
			atomic.AddInt32(&n.ref, 1)
			b.mu.Unlock()
			return true, false, n
		}
	}

	// Get only.
	if noset {
		b.mu.Unlock()
		return true, false, nil
	}
	// Set流程
	// Create node.
	n = &Node{
		r:    r,
		hash: hash,
		ns:   ns,
		key:  key,
		ref:  1,
	}
	// Add node to bucket.
	// 追加节点(也就是冲突解决通过类似链表的形式进行)?
	b.node = append(b.node, n)
	bLen := len(b.node)
	b.mu.Unlock()

	// Update counter.
	// 桶溢出的话进行扩张流程
	grow := atomic.AddInt32(&r.nodes, 1) >= h.growThreshold // cache里面的node数量达到阈值就进行扩张
	if bLen > mOverflowThreshold {
		grow = grow ||
			atomic.AddInt32(&h.overflow, 1) >= mOverflowGrowThreshold // 单个桶的节点数量超过32+128以后, 会强制触发扩张
	}

	// Grow.
	// 扩张
	if grow && atomic.CompareAndSwapInt32(&h.resizeInProgess, 0, 1) {
		nhLen := len(h.buckets) << 1 // 桶数量翻倍
		nh := &mNode{
			buckets:         make([]unsafe.Pointer, nhLen),
			mask:            uint32(nhLen) - 1,
			pred:            unsafe.Pointer(h),
			growThreshold:   int32(nhLen * mOverflowThreshold),
			shrinkThreshold: int32(nhLen >> 1),
		}
		// cache的桶指向新的桶表
		ok := atomic.CompareAndSwapPointer(&r.mHead, unsafe.Pointer(h), unsafe.Pointer(nh))
		if !ok {
			panic("BUG: failed swapping head")
		}
		go nh.initBuckets() // 后台更新桶
	}

	return true, true, n
}

func (b *mBucket) delete(r *Cache, h *mNode, hash uint32, ns, key uint64) (done, deleted bool) {
	b.mu.Lock()

	if b.frozen {
		b.mu.Unlock()
		return
	}

	// Scan the node.
	var (
		n    *Node
		bLen int
	)

	// 找到节点并且删除
	for i := range b.node {
		n = b.node[i]
		if n.ns == ns && n.key == key {
			if atomic.LoadInt32(&n.ref) == 0 {
				deleted = true

				// Call releaser.
				if n.value != nil {
					if r, ok := n.value.(util.Releaser); ok {
						r.Release()
					}
					n.value = nil
				}

				// Remove node from bucket.
				b.node = append(b.node[:i], b.node[i+1:]...)
				bLen = len(b.node)
			}
			break
		}
	}
	b.mu.Unlock()

	if deleted {
		// Call OnDel.
		// 顺序调用注册的删除时的回调函数
		for _, f := range n.onDel {
			f()
		}

		// Update counter.
		// 更新节点计数
		atomic.AddInt32(&r.size, int32(n.size)*-1)
		shrink := atomic.AddInt32(&r.nodes, -1) < h.shrinkThreshold
		if bLen >= mOverflowThreshold {
			atomic.AddInt32(&h.overflow, -1)
		}

		// Shrink.
		// 收缩桶
		if shrink && len(h.buckets) > mInitialSize && atomic.CompareAndSwapInt32(&h.resizeInProgess, 0, 1) {
			nhLen := len(h.buckets) >> 1
			nh := &mNode{
				buckets:         make([]unsafe.Pointer, nhLen),
				mask:            uint32(nhLen) - 1,
				pred:            unsafe.Pointer(h),
				growThreshold:   int32(nhLen * mOverflowThreshold),
				shrinkThreshold: int32(nhLen >> 1),
			}
			ok := atomic.CompareAndSwapPointer(&r.mHead, unsafe.Pointer(h), unsafe.Pointer(nh))
			if !ok {
				panic("BUG: failed swapping head")
			}
			go nh.initBuckets() // 后台更新桶
		}
	}

	return true, deleted
}

type mNode struct {
	buckets         []unsafe.Pointer // []*mBucket
	mask            uint32
	pred            unsafe.Pointer // *mNode
	resizeInProgess int32 // resize标志

	overflow        int32 // overflow次数
	growThreshold   int32 // 溢出阈值
	shrinkThreshold int32 // 收缩阈值
}

func (n *mNode) initBucket(i uint32) *mBucket {
	if b := (*mBucket)(atomic.LoadPointer(&n.buckets[i])); b != nil {
		return b
	}

	p := (*mNode)(atomic.LoadPointer(&n.pred))
	if p != nil {
		var node []*Node
		if n.mask > p.mask { // 通过比较resize前后的掩码来确定是收缩还是扩张
			// Grow.
			pb := (*mBucket)(atomic.LoadPointer(&p.buckets[i&p.mask])) // 获取扩张前对应的桶
			if pb == nil {
				pb = p.initBucket(i & p.mask)
			}
			m := pb.freeze()
			// Split nodes.
			for _, x := range m { // 使用扩张前对应的桶里的数据, 初始化第i个桶里面的数据
				if x.hash&n.mask == i {
					node = append(node, x)
				}
			}
		} else {
			// Shrink.
			pb0 := (*mBucket)(atomic.LoadPointer(&p.buckets[i])) // 同样是获取扩张前对应的两个桶
			if pb0 == nil {
				pb0 = p.initBucket(i)
			}
			pb1 := (*mBucket)(atomic.LoadPointer(&p.buckets[i+uint32(len(n.buckets))]))
			if pb1 == nil {
				pb1 = p.initBucket(i + uint32(len(n.buckets)))
			}
			m0 := pb0.freeze()
			m1 := pb1.freeze()
			// Merge nodes.
			node = make([]*Node, 0, len(m0)+len(m1)) // 进行收缩
			node = append(node, m0...)
			node = append(node, m1...)
		}
		// 替换桶
		b := &mBucket{node: node}
		if atomic.CompareAndSwapPointer(&n.buckets[i], nil, unsafe.Pointer(b)) {
			if len(node) > mOverflowThreshold {
				atomic.AddInt32(&n.overflow, int32(len(node)-mOverflowThreshold))
			}
			return b
		}
	}

	return (*mBucket)(atomic.LoadPointer(&n.buckets[i]))
}

func (n *mNode) initBuckets() {
	for i := range n.buckets {
		n.initBucket(uint32(i))
	}
	atomic.StorePointer(&n.pred, nil)
}

// Cache is a 'cache map'.
type Cache struct {
	mu     sync.RWMutex
	mHead  unsafe.Pointer // *mNode 哈希表
	nodes  int32          // 节点数量计数器
	size   int32          // 大小
	cacher Cacher         // 缓存算法 只有lru
	closed bool
}

// NewCache creates a new 'cache map'. The cacher is optional and
// may be nil.
func NewCache(cacher Cacher) *Cache {
	h := &mNode{
		buckets:         make([]unsafe.Pointer, mInitialSize),
		mask:            mInitialSize - 1,
		growThreshold:   int32(mInitialSize * mOverflowThreshold),
		shrinkThreshold: 0,
	}
	for i := range h.buckets {
		h.buckets[i] = unsafe.Pointer(&mBucket{})
	}
	r := &Cache{
		mHead:  unsafe.Pointer(h),
		cacher: cacher,
	}
	return r
}

func (r *Cache) getBucket(hash uint32) (*mNode, *mBucket) {
	h := (*mNode)(atomic.LoadPointer(&r.mHead))
	i := hash & h.mask
	b := (*mBucket)(atomic.LoadPointer(&h.buckets[i]))
	if b == nil {
		b = h.initBucket(i)
	}
	return h, b
}

func (r *Cache) delete(n *Node) bool {
	for {
		h, b := r.getBucket(n.hash)
		done, deleted := b.delete(r, h, n.hash, n.ns, n.key)
		if done {
			return deleted
		}
	}
}

// Nodes returns number of 'cache node' in the map.
func (r *Cache) Nodes() int {
	return int(atomic.LoadInt32(&r.nodes))
}

// Size returns sums of 'cache node' size in the map.
func (r *Cache) Size() int {
	return int(atomic.LoadInt32(&r.size))
}

// Capacity returns cache capacity.
func (r *Cache) Capacity() int {
	if r.cacher == nil {
		return 0
	}
	return r.cacher.Capacity()
}

// SetCapacity sets cache capacity.
func (r *Cache) SetCapacity(capacity int) {
	if r.cacher != nil {
		r.cacher.SetCapacity(capacity)
	}
}

// Get gets 'cache node' with the given namespace and key.
// If cache node is not found and setFunc is not nil, Get will atomically creates
// the 'cache node' by calling setFunc. Otherwise Get will returns nil.
//
// The returned 'cache handle' should be released after use by calling Release
// method.
func (r *Cache) Get(ns, key uint64, setFunc func() (size int, value Value)) *Handle {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil
	}

	hash := murmur32(ns, key, 0xf00)
	for { // 快速自旋
		// 快速自旋设计:
		// 1. 如果是获取了resize前的桶, 那么b.get返回done是false, 马上进行下一轮自旋
		// 2. 如果是获取了resize后的桶, 在r.getBucket的时候会进行桶的迁移
		h, b := r.getBucket(hash)
		done, _, n := b.get(r, h, hash, ns, key, setFunc == nil)
		// 如果done
		if done {
			if n != nil {
				n.mu.Lock()
				if n.value == nil {
					if setFunc == nil {
						n.mu.Unlock()
						n.unref()
						return nil
					}

					// 将值放入槽中
					n.size, n.value = setFunc()
					if n.value == nil {
						n.size = 0
						n.mu.Unlock()
						n.unref()
						return nil
					}
					atomic.AddInt32(&r.size, int32(n.size))
				}
				n.mu.Unlock()
				if r.cacher != nil {
					// 更新在淘汰算法里的位置
					r.cacher.Promote(n)
				}
				return &Handle{unsafe.Pointer(n)}
			}

			break
		}
	}
	return nil
}

// Delete removes and ban 'cache node' with the given namespace and key.
// A banned 'cache node' will never inserted into the 'cache tree'. Ban
// only attributed to the particular 'cache node', so when a 'cache node'
// is recreated it will not be banned.
//
// If onDel is not nil, then it will be executed if such 'cache node'
// doesn't exist or once the 'cache node' is released.
//
// Delete return true is such 'cache node' exist.
func (r *Cache) Delete(ns, key uint64, onDel func()) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false
	}

	hash := murmur32(ns, key, 0xf00)
	for {
		h, b := r.getBucket(hash)
		done, _, n := b.get(r, h, hash, ns, key, true)
		if done {
			if n != nil {
				if onDel != nil { // 注册删除的回调
					n.mu.Lock()
					n.onDel = append(n.onDel, onDel)
					n.mu.Unlock()
				}
				if r.cacher != nil {
					r.cacher.Ban(n) // 禁用, 从lru里面删掉
				}
				n.unref() // 引用计数释放
				return true
			}

			break
		}
	}

	if onDel != nil {
		onDel()
	}

	return false
}

// Evict evicts 'cache node' with the given namespace and key. This will
// simply call Cacher.Evict.
//
// Evict return true is such 'cache node' exist.
func (r *Cache) Evict(ns, key uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false
	}

	hash := murmur32(ns, key, 0xf00)
	for {
		h, b := r.getBucket(hash)
		done, _, n := b.get(r, h, hash, ns, key, true)
		if done {
			if n != nil {
				if r.cacher != nil {
					r.cacher.Evict(n)
				}
				n.unref()
				return true
			}

			break
		}
	}

	return false
}

// EvictNS evicts 'cache node' with the given namespace. This will
// simply call Cacher.EvictNS.
func (r *Cache) EvictNS(ns uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}

	if r.cacher != nil {
		r.cacher.EvictNS(ns)
	}
}

// EvictAll evicts all 'cache node'. This will simply call Cacher.EvictAll.
func (r *Cache) EvictAll() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return
	}

	if r.cacher != nil {
		r.cacher.EvictAll()
	}
}

// Close closes the 'cache map' and forcefully releases all 'cache node'.
func (r *Cache) Close() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true

		h := (*mNode)(r.mHead)
		h.initBuckets()

		for i := range h.buckets {
			b := (*mBucket)(h.buckets[i])
			for _, n := range b.node {
				// Call releaser.
				if n.value != nil {
					if r, ok := n.value.(util.Releaser); ok {
						r.Release()
					}
					n.value = nil
				}

				// Call OnDel.
				for _, f := range n.onDel {
					f()
				}
				n.onDel = nil
			}
		}
	}
	r.mu.Unlock()

	// Avoid deadlock.
	if r.cacher != nil {
		if err := r.cacher.Close(); err != nil {
			return err
		}
	}
	return nil
}

// CloseWeak closes the 'cache map' and evict all 'cache node' from cacher, but
// unlike Close it doesn't forcefully releases 'cache node'.
func (r *Cache) CloseWeak() error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
	}
	r.mu.Unlock()

	// Avoid deadlock.
	if r.cacher != nil {
		r.cacher.EvictAll()
		if err := r.cacher.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Node is a 'cache node'.
type Node struct {
	r *Cache

	hash    uint32
	ns, key uint64

	mu    sync.Mutex
	size  int
	value Value

	ref   int32
	onDel []func()

	CacheData unsafe.Pointer  // *lruNode lru cache中CacheData是用来标记是否在双向环装链表中的
}

// NS returns this 'cache node' namespace.
func (n *Node) NS() uint64 {
	return n.ns
}

// Key returns this 'cache node' key.
func (n *Node) Key() uint64 {
	return n.key
}

// Size returns this 'cache node' size.
func (n *Node) Size() int {
	return n.size
}

// Value returns this 'cache node' value.
func (n *Node) Value() Value {
	return n.value
}

// Ref returns this 'cache node' ref counter.
func (n *Node) Ref() int32 {
	return atomic.LoadInt32(&n.ref)
}

// GetHandle returns an handle for this 'cache node'.
func (n *Node) GetHandle() *Handle {
	if atomic.AddInt32(&n.ref, 1) <= 1 {
		panic("BUG: Node.GetHandle on zero ref")
	}
	return &Handle{unsafe.Pointer(n)}
}

func (n *Node) unref() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.r.delete(n)
	}
}

func (n *Node) unrefLocked() {
	if atomic.AddInt32(&n.ref, -1) == 0 {
		n.r.mu.RLock()
		if !n.r.closed {
			n.r.delete(n)
		}
		n.r.mu.RUnlock()
	}
}

// Handle is a 'cache handle' of a 'cache node'.
type Handle struct {
	n unsafe.Pointer // *Node
}

// Value returns the value of the 'cache node'.
func (h *Handle) Value() Value {
	n := (*Node)(atomic.LoadPointer(&h.n))
	if n != nil {
		return n.value
	}
	return nil
}

// Release releases this 'cache handle'.
// It is safe to call release multiple times.
func (h *Handle) Release() {
	nPtr := atomic.LoadPointer(&h.n)
	if nPtr != nil && atomic.CompareAndSwapPointer(&h.n, nPtr, nil) {
		n := (*Node)(nPtr)
		// 如果引用计数归零, 那么就会从hashtable里面删掉
		n.unrefLocked()
	}
}

func murmur32(ns, key uint64, seed uint32) uint32 {
	const (
		m = uint32(0x5bd1e995)
		r = 24
	)

	k1 := uint32(ns >> 32)
	k2 := uint32(ns)
	k3 := uint32(key >> 32)
	k4 := uint32(key)

	k1 *= m
	k1 ^= k1 >> r
	k1 *= m

	k2 *= m
	k2 ^= k2 >> r
	k2 *= m

	k3 *= m
	k3 ^= k3 >> r
	k3 *= m

	k4 *= m
	k4 ^= k4 >> r
	k4 *= m

	h := seed

	h *= m
	h ^= k1
	h *= m
	h ^= k2
	h *= m
	h ^= k3
	h *= m
	h ^= k4

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}
