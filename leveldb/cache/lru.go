// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package cache

import (
	"sync"
	"unsafe"
)

type lruNode struct {
	n   *Node                // 指向当前位置缓存的Node的指针
	h   *Handle              // 指向head Node的指针
	ban bool                 // 被禁标志

	next, prev *lruNode      // 双向链表
}

func (n *lruNode) insert(at *lruNode) { // 将当前节点插入到at之后
	x := at.next
	at.next = n
	n.prev = at
	n.next = x
	x.prev = n
}

func (n *lruNode) remove() { // 从lru链表中将节点移除
	if n.prev != nil {
		n.prev.next = n.next
		n.next.prev = n.prev
		n.prev = nil
		n.next = nil
	} else {
		panic("BUG: removing removed node")
	}
}

type lru struct {
	mu       sync.Mutex
	capacity int
	used     int
	recent   lruNode   // 双向环状链表的哨兵
}

func (r *lru) reset() {
	r.recent.next = &r.recent
	r.recent.prev = &r.recent
	r.used = 0
}

func (r *lru) Capacity() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.capacity
}

func (r *lru) SetCapacity(capacity int) {
	var evicted []*lruNode

	// 如果容量缩减, 那么就释放最久没被使用的节点, 直到满足容量限制
	r.mu.Lock()
	r.capacity = capacity
	for r.used > r.capacity {
		rn := r.recent.prev
		if rn == nil {
			panic("BUG: invalid LRU used or capacity counter")
		}
		rn.remove()
		rn.n.CacheData = nil
		r.used -= rn.n.Size()
		evicted = append(evicted, rn)
	}
	r.mu.Unlock()

	// 解除头指针引用
	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *lru) Promote(n *Node) {
	var evicted []*lruNode

	r.mu.Lock()
	if n.CacheData == nil {
		if n.Size() <= r.capacity {
			rn := &lruNode{n: n, h: n.GetHandle()}
			rn.insert(&r.recent) // 插入到队首(哨兵之后)
			n.CacheData = unsafe.Pointer(rn)
			r.used += n.Size()

			// 清理容量超出部分
			for r.used > r.capacity {
				rn := r.recent.prev
				if rn == nil {
					panic("BUG: invalid LRU used or capacity counter")
				}
				rn.remove()
				rn.n.CacheData = nil //
				r.used -= rn.n.Size()
				evicted = append(evicted, rn)
			}
		}
	} else {
		// 节点cache data不为空, 那么就意味着已经被插入过了
		// 那么只要不是被禁用了, 就将节点移动到队首即可
		rn := (*lruNode)(n.CacheData)
		if !rn.ban {
			rn.remove()
			rn.insert(&r.recent)
		}
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *lru) Ban(n *Node) {
	r.mu.Lock()
	if n.CacheData == nil {
		n.CacheData = unsafe.Pointer(&lruNode{n: n, ban: true})
	} else {
		// 从cache中移除该节点
		// 但是没有解开node到cache的引用
		// 目测是要配合Evict来共同使用, 才能将一个节点从lru里面手动删掉
		rn := (*lruNode)(n.CacheData)
		if !rn.ban {
			rn.remove()
			rn.ban = true
			r.used -= rn.n.Size()
			r.mu.Unlock()

			rn.h.Release()
			rn.h = nil
			return
		}
	}
	r.mu.Unlock()
}

func (r *lru) Evict(n *Node) {
	r.mu.Lock()
	rn := (*lruNode)(n.CacheData)
	if rn == nil || rn.ban {
		r.mu.Unlock()
		return
	}

	// 这里只是解开node到lru node的引用, 但是没有解开反向引用
	// 而且lruNode也有可能还在cache中, 要配合Ban使用
	n.CacheData = nil
	r.mu.Unlock()

	rn.h.Release()
}

func (r *lru) EvictNS(ns uint64) {
	var evicted []*lruNode

	r.mu.Lock()
	// 从队尾向队首遍历, 删除节点
	for e := r.recent.prev; e != &r.recent; {
		rn := e
		e = e.prev
		if rn.n.NS() == ns {
			rn.remove()
			rn.n.CacheData = nil
			r.used -= rn.n.Size()
			evicted = append(evicted, rn)
		}
	}
	r.mu.Unlock()

	for _, rn := range evicted {
		rn.h.Release()
	}
}

func (r *lru) EvictAll() {
	r.mu.Lock()
	back := r.recent.prev
	// 遍历删除节点
	for rn := back; rn != &r.recent; rn = rn.prev {
		rn.n.CacheData = nil
	}
	r.reset()
	r.mu.Unlock()

	for rn := back; rn != &r.recent; rn = rn.prev {
		rn.h.Release()
	}
}

func (r *lru) Close() error {
	return nil
}

// NewLRU create a new LRU-cache.
func NewLRU(capacity int) Cacher {
	r := &lru{capacity: capacity}
	r.reset()
	return r
}
