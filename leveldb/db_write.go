// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (db *DB) writeJournal(batches []*Batch, seq uint64, sync bool) error {
	// 获取下一个journal块
	wr, err := db.journal.Next()
	if err != nil {
		return err
	}
	// 在journal中写入数据(循环写, 可能构建出多个块)
	if err := writeBatchesWithHeader(wr, batches, seq); err != nil {
		return err
	}
	// 将最后一个journal block写入文件writer
	if err := db.journal.Flush(); err != nil {
		return err
	}
	// 同步写, 这里使用系统调用fsync进行文件同步写, 保证断电以后不丢失
	if sync {
		return db.journalWriter.Sync()
	}
	return nil
}


func (db *DB) rotateMem(n int, wait bool) (mem *memDB, err error) {
	retryLimit := 3
retry:
	// Wait for pending memdb compaction.
	// 等待mem前一个mem db compaction完成
	err = db.compTriggerWait(db.mcompCmdC)
	if err != nil {
		return
	}
	retryLimit--

	// Create new memdb and journal.
	// 把当前的mutable mem db置为immutable mem db,构建新的mutable mem db
	mem, err = db.newMem(n)
	if err != nil {
		if err == errHasFrozenMem {
			if retryLimit <= 0 {
				panic("BUG: still has frozen memdb")
			}
			goto retry
		}
		return
	}

	// Schedule memdb compaction.
	// 触发mem db compaction, 实际上就是将immutable mem db刷入sst
	if wait {
		err = db.compTriggerWait(db.mcompCmdC)
	} else {
		db.compTrigger(db.mcompCmdC)
	}
	return
}

// 为下一次写入腾出有足够空间的mem db
func (db *DB) flush(n int) (mdb *memDB, mdbFree int, err error) {
	delayed := false
	slowdownTrigger := db.s.o.GetWriteL0SlowdownTrigger()
	pauseTrigger := db.s.o.GetWriteL0PauseTrigger()
	flush := func() (retry bool) {
		// 获取当前有效的mem db
		mdb = db.getEffectiveMem()
		if mdb == nil {
			err = ErrClosed
			return false
		}
		defer func() {
			if retry {
				// 减少引用计数
				mdb.decref()
				mdb = nil
			}
		}()

		tLen := db.s.tLen(0) // level 0 长度
		mdbFree = mdb.Free()      // 可用空间 cap - len
		switch {
		case tLen >= slowdownTrigger && !delayed:
			// l0数量达到差速阈值
			// 那么就是延迟1ms, 再重新flush
			delayed = true
			// 一毫秒
			time.Sleep(time.Millisecond)
		case mdbFree >= n:
			// 有足够的空间
			return false
		case tLen >= pauseTrigger:
			// l0文件数量达到中断阈值
			// 强制中断, 触发同步compaction
			delayed = true
			// Set the write paused flag explicitly.
			atomic.StoreInt32(&db.inWritePaused, 1)
			err = db.compTriggerWait(db.tcompCmdC) // 同步compaction
			// Unset the write paused flag.
			atomic.StoreInt32(&db.inWritePaused, 0)
			if err != nil {
				return false
			}
		default:
			// Allow memdb to grow if it has no entry.
			if mdb.Len() == 0 { // mem db 是空的
				mdbFree = n
			} else {
				// mem db 不是空的, 那需要解引用, 构造一个新的mem db供使用
				mdb.decref()
				mdb, err = db.rotateMem(n, false)
				if err == nil {
					mdbFree = mdb.Free()
				} else {
					mdbFree = 0
				}
			}
			return false
		}
		return true
	}
	start := time.Now()
	for flush() {
	}
	if delayed {
		db.writeDelay += time.Since(start)
		db.writeDelayN++
	} else if db.writeDelayN > 0 {
		db.logf("db@write was delayed N·%d T·%v", db.writeDelayN, db.writeDelay)
		atomic.AddInt32(&db.cWriteDelayN, int32(db.writeDelayN))
		atomic.AddInt64(&db.cWriteDelay, int64(db.writeDelay))
		db.writeDelay = 0
		db.writeDelayN = 0
	}
	return
}

type writeMerge struct {
	sync       bool
	batch      *Batch
	keyType    keyType
	key, value []byte
}

func (db *DB) unlockWrite(overflow bool, merged int, err error) {
	for i := 0; i < merged; i++ {
		db.writeAckC <- err
	}
	if overflow {
		// 传递写锁
		// Pass lock to the next write (that failed to merge).
		db.writeMergedC <- false
	} else {
		// 释放写锁
		// Release lock.
		<-db.writeLockC
	}
}

// ourBatch is batch that we can modify.
func (db *DB) writeLocked(batch, ourBatch *Batch, merge, sync bool) error {
	// Try to flush memdb. This method would also trying to throttle writes
	// if it is too fast and compaction cannot catch-up.
	// 获取memory db和memory db的余量
	// 如果塞不下当前batch的话会将当前mutable mem db转成immutable
	// 然后构建一个空的返回
	mdb, mdbFree, err := db.flush(batch.internalLen)
	if err != nil {
		db.unlockWrite(false, 0, err)
		return err
	}
	defer mdb.decref()

	var (
		overflow bool
		merged   int
		batches  = []*Batch{batch}
	)

	if merge {
		// Merge limit.
		// 最多合并多少batch
		var mergeLimit int
		if batch.internalLen > 128<<10 { // 2^17
			mergeLimit = (1 << 20) - batch.internalLen// 2^20
		} else {
			mergeLimit = 128 << 10 // 2^17
		}

		// 根据mutable mem db的余量来修正
		mergeCap := mdbFree - batch.internalLen
		if mergeLimit > mergeCap {
			mergeLimit = mergeCap
		}

	merge:
		// 执行合并
		for mergeLimit > 0 {
			select {
			// 读取其他goroutine需要写入的kv或者batch
			case incoming := <-db.writeMergeC:
				if incoming.batch != nil {
					// 写入batch
					// Merge batch.
					if incoming.batch.internalLen > mergeLimit {
						// 溢出
						overflow = true
						break merge
					}
					// 没有溢出就继续append
					batches = append(batches, incoming.batch)
					mergeLimit -= incoming.batch.internalLen
				} else {
					// 写入kv
					// Merge put.
					// 长度计算
					internalLen := len(incoming.key) + len(incoming.value) + 8
					if internalLen > mergeLimit {
						// 溢出
						overflow = true
						break merge
					}
					// 初始化当前goroutine的batch
					if ourBatch == nil {
						ourBatch = db.batchPool.Get().(*Batch)
						ourBatch.Reset()
						batches = append(batches, ourBatch)
					}
					// We can use same batch since concurrent write doesn't
					// guarantee write order.
					// 将kv追下写入当前goroutine batch
					ourBatch.appendRec(incoming.keyType, incoming.key, incoming.value)
					mergeLimit -= internalLen
				}
				sync = sync || incoming.sync
				merged++
				db.writeMergedC <- true // 完成合并

			default:
				break merge
			}
		}
	}

	// Release ourBatch if any.
	// 注册延迟释放到对象池
	if ourBatch != nil {
		defer db.batchPool.Put(ourBatch)
	}

	// Seq number.
	// 获取写入的第一个kv的seqNo
	seq := db.seq + 1

	// Write journal.
	// 写日志, WAL
	if err := db.writeJournal(batches, seq, sync); err != nil {
		db.unlockWrite(overflow, merged, err)
		return err
	}

	// Put batches.
	// 写跳表, 更新mutable mem db
	for _, batch := range batches {
		if err := batch.putMem(seq, mdb.DB); err != nil {
			panic(err)
		}
		seq += uint64(batch.Len())
	}

	// Incr seq number.
	// 执行写时才加1, 读是根据seq进行快照读
	db.addSeq(uint64(batchesLen(batches)))

	// Rotate memdb if it's reach the threshold.
	if batch.internalLen >= mdbFree {
		// batch中的数据总长度超过了mdbFree
		// 那么就不会进行写合并, 这里需要主动触发一次旋转
		// 本质上是单独为这个batch构建了一个immutable mem table
		// 这个immutable mem table的大小是超标的
		db.rotateMem(0, false)
	}

	// 释放或者移交写锁
	db.unlockWrite(overflow, merged, nil)
	return nil
}

// Write apply the given batch to the DB. The batch records will be applied
// sequentially. Write might be used concurrently, when used concurrently and
// batch is small enough, write will try to merge the batches. Set NoWriteMerge
// option to true to disable write merge.
//
// It is safe to modify the contents of the arguments after Write returns but
// not before. Write will not modify content of the batch.
func (db *DB) Write(batch *Batch, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil || batch == nil || batch.Len() == 0 {
		return err
	}

	// If the batch size is larger than write buffer, it may justified to write
	// using transaction instead. Using transaction the batch will be written
	// into tables directly, skipping the journaling.
	if batch.internalLen > db.s.o.GetWriteBuffer() && !db.s.o.GetDisableLargeBatchTransaction() {
		tr, err := db.OpenTransaction()
		if err != nil {
			return err
		}
		if err := tr.Write(batch, wo); err != nil {
			tr.Discard()
			return err
		}
		return tr.Commit()
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		select {
		case db.writeMergeC <- writeMerge{sync: sync, batch: batch}:
			if <-db.writeMergedC {
				// Write is merged.
				return <-db.writeAckC
			}
			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	return db.writeLocked(batch, nil, merge, sync)
}

// 增删改一致的接口
func (db *DB) putRec(kt keyType, key, value []byte, wo *opt.WriteOptions) error {
	if err := db.ok(); err != nil {
		return err
	}

	merge := !wo.GetNoWriteMerge() && !db.s.o.GetNoWriteMerge()
	sync := wo.GetSync() && !db.s.o.GetNoSync()

	// Acquire write lock.
	if merge {
		// 写合并
		select {
		// 这里是只有塞进chan的话就只能等待被合并
		case db.writeMergeC <- writeMerge{sync: sync, keyType: kt, key: key, value: value}:
			// 注意这里一个很巧妙的设计
			// 如果db.writeMergedC返回false的话
			// 说明由于前一个主写goroutine所要写的batches大小已经溢出了, 不能再写了
			// 在溢出的情况下, 140行unlockWrite才会返回false
			//
			// 这里利用go的一个重要特性
			// 对于无缓冲chan, 当且仅当chan的读写动作对齐时才能完成双方动作
			// 当有goroutine进入下面这行读取时, 必须等待下述两个条件之一
			// 1. 等待主写goroutine完成合并返回true
			// 2. 等待放弃合并返回false, 则当前goroutine获得写锁成为下一个主写goroutine
			// 这就保证了下一个想要塞入writeMergeC的goroutine在完成塞入动作前
			// 1. 要么该goroutine的writeMerge已经被合并
			// 2. 要么该goroutine已经获取了写锁成为了主写goroutine
			if <-db.writeMergedC {
				// 写被合并了
				// Write is merged.
				return <-db.writeAckC // 等待写入确认
			}


			// Write is not merged, the write lock is handed to us. Continue.
		case db.writeLockC <- struct{}{}:
			// 这里获取写锁, 才能继续往下执行
			// 实际执行写合并的主写goroutine走这个分支
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	} else {
		// 没有写合并
		select {
		case db.writeLockC <- struct{}{}:
			// Write lock acquired.
		case err := <-db.compPerErrC:
			// Compaction error.
			return err
		case <-db.closeC:
			// Closed
			return ErrClosed
		}
	}

	// 本质上是转换为batch写
	// batch包含若干条数据项
	// 格式如下
	// | type | key length | key | value length | value |
	// 这里的key指的是user key, 实际写入到level db会被转换成internal key
	// internal key格式为在user key之后增加7bytes的seq和1byte的类型标识
	batch := db.batchPool.Get().(*Batch)
	batch.Reset()
	batch.appendRec(kt, key, value)
	return db.writeLocked(batch, batch, merge, sync)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map. Write merge also applies for Put, see
// Write.
//
// It is safe to modify the contents of the arguments after Put returns but not
// before.
func (db *DB) Put(key, value []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeVal, key, value, wo)
}

// Delete deletes the value for the given key. Delete will not returns error if
// key doesn't exist. Write merge also applies for Delete, see Write.
//
// It is safe to modify the contents of the arguments after Delete returns but
// not before.
func (db *DB) Delete(key []byte, wo *opt.WriteOptions) error {
	return db.putRec(keyTypeDel, key, nil, wo)
}

func isMemOverlaps(icmp *iComparer, mem *memdb.DB, min, max []byte) bool {
	iter := mem.NewIterator(nil)
	defer iter.Release()
	return (max == nil || (iter.First() && icmp.uCompare(max, internalKey(iter.Key()).ukey()) >= 0)) &&
		(min == nil || (iter.Last() && icmp.uCompare(min, internalKey(iter.Key()).ukey()) <= 0))
}

// CompactRange compacts the underlying DB for the given key range.
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data. This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// A nil Range.Start is treated as a key before all keys in the DB.
// And a nil Range.Limit is treated as a key after all keys in the DB.
// Therefore if both is nil then it will compact entire DB.
func (db *DB) CompactRange(r util.Range) error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Check for overlaps in memdb.
	mdb := db.getEffectiveMem()
	if mdb == nil {
		return ErrClosed
	}
	defer mdb.decref()
	if isMemOverlaps(db.s.icmp, mdb.DB, r.Start, r.Limit) {
		// Memdb compaction.
		if _, err := db.rotateMem(0, false); err != nil {
			<-db.writeLockC
			return err
		}
		<-db.writeLockC
		if err := db.compTriggerWait(db.mcompCmdC); err != nil {
			return err
		}
	} else {
		<-db.writeLockC
	}

	// Table compaction.
	return db.compTriggerRange(db.tcompCmdC, -1, r.Start, r.Limit)
}

// SetReadOnly makes DB read-only. It will stay read-only until reopened.
func (db *DB) SetReadOnly() error {
	if err := db.ok(); err != nil {
		return err
	}

	// Lock writer.
	select {
	case db.writeLockC <- struct{}{}:
		db.compWriteLocking = true
	case err := <-db.compPerErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}

	// Set compaction read-only.
	select {
	case db.compErrSetC <- ErrReadOnly:
	case perr := <-db.compPerErrC:
		return perr
	case <-db.closeC:
		return ErrClosed
	}

	return nil
}
