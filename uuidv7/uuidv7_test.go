package uuidv7

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestExample(t *testing.T) {
	gen := New()

	// 1. 生成原始字节 (用于数据库存储)
	id := gen.Next()

	// 2. 转为标准带横线的字符串 (用于日志或内部调试)
	fmt.Println("标准格式:", String(id))
	// 输出: 019dcef2-e9ab-7095-8c93-cc33611bfcbf

	// 3. 直接生成不带横线的字符串
	s := gen.NextStringNoDash()
	fmt.Println("无横线格式:", s)
	// 输出: 019dcef2e9ab70958c93cc33611bfcbf

	fmt.Println("Next:", Next())

	fmt.Println("NextEncryptedString:", NextEncryptedString())
	sss := NextStringNoDash()
	fmt.Println("NextStringNoDash:", sss)

	sssss, _ := EncryptString(sss)
	fmt.Println("EncryptString:", sssss)

	ssssss, _ := DecryptString(sssss)
	fmt.Println("DecryptString:", ssssss)

	fmt.Println("String:", String(Next()))

}

// 基础唯一性测试
func TestUniqueness(t *testing.T) {
	gen := New()

	seen := make(map[[16]byte]struct{})

	for i := 0; i < 100000; i++ {
		id := gen.Next()

		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate UUID detected at iteration %d", i)
		}
		seen[id] = struct{}{}
	}
}

// 单调递增测试（核心）
func TestMonotonic(t *testing.T) {
	gen := New()

	prev := gen.Next()

	for i := 0; i < 10000; i++ {
		curr := gen.Next()

		if compareUUID(curr, prev) <= 0 {
			t.Fatalf("UUID not monotonic at iteration %d", i)
		}

		prev = curr
	}
}

// UUID 比较函数（按字节序）
func compareUUID(a, b [16]byte) int {
	for i := 0; i < 16; i++ {
		if a[i] > b[i] {
			return 1
		}
		if a[i] < b[i] {
			return -1
		}
	}
	return 0
}

// 并发安全测试
func TestConcurrent(t *testing.T) {
	gen := New()

	const goroutines = 10
	const perG = 10000

	var wg sync.WaitGroup
	ch := make(chan [16]byte, goroutines*perG)

	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				ch <- gen.Next()
			}
		}()
	}

	wg.Wait()
	close(ch)

	seen := make(map[[16]byte]struct{})

	for id := range ch {
		if _, ok := seen[id]; ok {
			t.Fatal("duplicate UUID detected in concurrent test")
		}
		seen[id] = struct{}{}
	}
}

// 结构合法性测试（RFC v7）
func TestVersionAndVariant(t *testing.T) {
	gen := New()

	for i := 0; i < 100; i++ {
		id := gen.Next()
		fmt.Println(gen.NextStringNoDash())
		// version: 高4bit必须是 0111
		version := (id[6] >> 4) & 0x0F
		if version != 7 {
			t.Fatalf("invalid version: got %d", version)
		}

		// variant: 前2bit必须是 10
		variant := (id[8] >> 6) & 0x03
		if variant != 0x02 {
			t.Fatalf("invalid variant: got %b", variant)
		}
	}
}

// 时间回拨容错测试
func TestClockRollback(t *testing.T) {
	gen := New()

	id1 := gen.Next()

	// 模拟时间回拨（手动修改 lastTs）
	gen.lastTs += 1000

	id2 := gen.Next()

	if compareUUID(id2, id1) <= 0 {
		t.Fatal("UUID not monotonic after simulated clock rollback")
	}
}

// Benchmark（性能测试）
func BenchmarkUUIDv7(b *testing.B) {
	gen := New()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = gen.Next()
	}
}

// Fuzz 测试（结构稳定性）
func FuzzUUIDStructure(f *testing.F) {
	gen := New()

	f.Add(1)

	f.Fuzz(func(t *testing.T, n int) {
		for i := 0; i < 1000; i++ {
			id := gen.Next()

			// version 检查
			if (id[6]>>4)&0x0F != 7 {
				t.Fatalf("invalid version")
			}

			// variant 检查
			if (id[8]>>6)&0x03 != 0x02 {
				t.Fatalf("invalid variant")
			}
		}
	})
}

// 排序一致性测试（模拟数据库）
func TestSortOrder(t *testing.T) {
	gen := New()

	var list [][16]byte

	for i := 0; i < 10000; i++ {
		list = append(list, gen.Next())
	}

	// 拷贝一份做排序
	sorted := make([][16]byte, len(list))
	copy(sorted, list)

	sort.Slice(sorted, func(i, j int) bool {
		return compareUUID(sorted[i], sorted[j]) < 0
	})

	for i := range list {
		if list[i] != sorted[i] {
			t.Fatal("UUID order is not stable (DB index risk)")
		}
	}
}

// 极限并发压力测试
func TestHighConcurrency(t *testing.T) {
	gen := New()

	const goroutines = 50
	const perG = 20000

	var wg sync.WaitGroup
	ch := make(chan [16]byte, goroutines*perG)

	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				ch <- gen.Next()
			}
		}()
	}

	wg.Wait()
	close(ch)

	seen := make(map[[16]byte]struct{})

	for id := range ch {
		if _, ok := seen[id]; ok {
			t.Fatal("duplicate under high concurrency")
		}
		seen[id] = struct{}{}
	}
}

// 边界测试（seq 溢出）
func TestSeqOverflow(t *testing.T) {
	gen := New()

	// 强行锁住时间
	now := time.Now().UnixMilli()
	gen.lastTs = now

	for i := 0; i < 5000; i++ {
		_ = gen.Next()
	}

	// 只要不 panic / 不死锁，就算通过
}
