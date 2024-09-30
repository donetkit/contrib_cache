package memory

import (
	"bytes"
	"io/ioutil"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

type TestStruct struct {
	Num      int
	Children []*TestStruct
}

func TestCache(t *testing.T) {
	tc := New()

	a := tc.Get("a")
	if a != nil {
		t.Error("Getting A found value that shouldn't exist:", a)
	}

	b := tc.Get("b")
	if b != nil {
		t.Error("Getting B found value that shouldn't exist:", b)
	}

	c := tc.Get("c")
	if c != nil {
		t.Error("Getting C found value that shouldn't exist:", c)
	}

	tc.Set("a", 1, 0)
	tc.Set("b", "b", 0)
	tc.Set("c", 3.5, 0)

	x := tc.Get("a")
	if x == nil {
		t.Error("x for a is nil")
	} else if a2 := x.(int); a2+2 != 3 {
		t.Error("a2 (which should be 1) plus 2 does not equal 3; value:", a2)
	}

	x = tc.Get("b")
	if x == nil {
		t.Error("x for b is nil")
	} else if b2 := x.(string); b2+"B" != "bB" {
		t.Error("b2 (which should be b) plus B does not equal bB; value:", b2)
	}

	x = tc.Get("c")
	if x == nil {
		t.Error("x for c is nil")
	} else if c2 := x.(float64); c2+1.2 != 4.7 {
		t.Error("c2 (which should be 3.5) plus 1.2 does not equal 4.7; value:", c2)
	}
}

func TestCacheTimes(t *testing.T) {
	tc := New(WithDefaultExpiration(50*time.Millisecond), WithCleanupInterval(1*time.Millisecond))
	tc.Set("a", 1, 0)
	tc.Set("b", 2, -1)
	tc.Set("c", 3, 20*time.Millisecond)
	tc.Set("d", 4, 70*time.Millisecond)

	<-time.After(25 * time.Millisecond)
	v := tc.Get("c")
	if v != nil {
		t.Error("Found c when it should have been automatically deleted")
	}

	<-time.After(30 * time.Millisecond)
	v = tc.Get("a")
	if v != nil {
		t.Error("Found a when it should have been automatically deleted")
	}

	v = tc.Get("b")
	if v == nil {
		t.Error("Did not find b even though it was set to never expire")
	}

	v = tc.Get("d")
	if v == nil {
		t.Error("Did not find d even though it was set to expire later than the default")
	}

	<-time.After(20 * time.Millisecond)
	v = tc.Get("d")
	if v != nil {
		t.Error("Found d when it should have been automatically deleted (later than the default)")
	}
}

func TestStorePointerToStruct(t *testing.T) {
	tc := New()
	tc.Set("foo", &TestStruct{Num: 1}, 0)
	x := tc.Get("foo")
	if x == nil {
		t.Fatal("*TestStruct was not found for foo")
	}
	foo := x.(*TestStruct)
	foo.Num++

	y := tc.Get("foo")
	if y == nil {
		t.Fatal("*TestStruct was not found for foo (second time)")
	}
	bar := y.(*TestStruct)
	if bar.Num != 2 {
		t.Fatal("TestStruct.Num is not 2")
	}
}

func TestIncrementUint(t *testing.T) {
	tc := New()
	tc.Set("tuint", uint(1), 0)
	_, err := tc.Increment("tuint", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}

	x := tc.Get("tuint")
	if x == nil {
		t.Error("tuint was not found")
	}
	if x.(uint) != 3 {
		t.Error("tuint is not 3:", x)
	}
}

func TestIncrementUintptr(t *testing.T) {
	tc := New()
	tc.Set("tuintptr", uintptr(1), 0)
	_, err := tc.Increment("tuintptr", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}

	x := tc.Get("tuintptr")
	if x == nil {
		t.Error("tuintptr was not found")
	}
	if x.(uintptr) != 3 {
		t.Error("tuintptr is not 3:", x)
	}
}

func TestIncrementUint8(t *testing.T) {
	tc := New()
	tc.Set("tuint8", uint8(1), 0)
	_, err := tc.Increment("tuint8", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}

	x := tc.Get("tuint8")
	if x == nil {
		t.Error("tuint8 was not found")
	}
	if x.(uint8) != 3 {
		t.Error("tuint8 is not 3:", x)
	}
}

func TestIncrementUint16(t *testing.T) {
	tc := New()
	tc.Set("tuint16", uint16(1), 0)
	_, err := tc.Increment("tuint16", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}

	x := tc.Get("tuint16")
	if x == nil {
		t.Error("tuint16 was not found")
	}
	if x.(uint16) != 3 {
		t.Error("tuint16 is not 3:", x)
	}
}

func TestIncrementUint32(t *testing.T) {
	tc := New()
	tc.Set("tuint32", uint32(1), 0)
	_, err := tc.Increment("tuint32", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}

	x := tc.Get("tuint32")
	if x == nil {
		t.Error("tuint32 was not found")
	}
	if x.(uint32) != 3 {
		t.Error("tuint32 is not 3:", x)
	}
}

func TestIncrementUint64(t *testing.T) {
	tc := New()
	tc.Set("tuint64", uint64(1), 0)
	_, err := tc.Increment("tuint64", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}

	x := tc.Get("tuint64")
	if x == nil {
		t.Error("tuint64 was not found")
	}
	if x.(uint64) != 3 {
		t.Error("tuint64 is not 3:", x)
	}
}

func TestIncrementInt(t *testing.T) {
	tc := New()
	tc.Set("tint", 1, 0)
	_, err := tc.Increment("tint", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	x := tc.Get("tint")
	if x == nil {
		t.Error("tint was not found")
	}
	if x.(int) != 3 {
		t.Error("tint is not 3:", x)
	}
}

func TestIncrementInt8(t *testing.T) {
	tc := New()
	tc.Set("tint8", int8(1), 0)
	_, err := tc.Increment("tint8", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	x := tc.Get("tint8")
	if x == nil {
		t.Error("tint8 was not found")
	}
	if x.(int8) != 3 {
		t.Error("tint8 is not 3:", x)
	}
}

func TestIncrementInt16(t *testing.T) {
	tc := New()
	tc.Set("tint16", int16(1), 0)
	_, err := tc.Increment("tint16", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	x := tc.Get("tint16")
	if x == nil {
		t.Error("tint16 was not found")
	}
	if x.(int16) != 3 {
		t.Error("tint16 is not 3:", x)
	}
}

func TestIncrementInt32(t *testing.T) {
	tc := New()
	tc.Set("tint32", int32(1), 0)
	_, err := tc.Increment("tint32", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	x := tc.Get("tint32")
	if x == nil {
		t.Error("tint32 was not found")
	}
	if x.(int32) != 3 {
		t.Error("tint32 is not 3:", x)
	}
}

func TestIncrementInt64(t *testing.T) {
	tc := New()
	tc.Set("tint64", int64(1), 0)
	_, err := tc.Increment("tint64", 2)
	if err != nil {
		t.Error("Error incrementing:", err)
	}
	x := tc.Get("tint64")
	if x == nil {
		t.Error("tint64 was not found")
	}
	if x.(int64) != 3 {
		t.Error("tint64 is not 3:", x)
	}
}

func TestDecrementInt64(t *testing.T) {
	tc := New()
	tc.Set("int64", int64(5), 0)
	_, err := tc.Decrement("int64", 2)
	if err != nil {
		t.Error("Error decrementing:", err)
	}
	x := tc.Get("int64")
	if x == nil {
		t.Error("int64 was not found")
	}
	if x.(int64) != 3 {
		t.Error("int64 is not 3:", x)
	}
}

func TestAdd(t *testing.T) {
	tc := New()
	err := tc.Set("foo", "bar", 0)
	if err != nil {
		t.Error("Couldn't add foo even though it shouldn't exist")
	}
	err = tc.Set("foo", "baz", 0)
	if err != nil {
		t.Error("Successfully added another foo when it should have returned an error")
	}
}

func TestDelete(t *testing.T) {
	tc := New()
	tc.Set("foo", "bar", 0)
	tc.Delete("foo")
	x := tc.Get("foo")
	if x != nil {
		t.Error("x is not nil:", x)
	}
}

func TestFlush(t *testing.T) {
	tc := New()
	tc.Set("foo", "bar", 0)
	tc.Set("baz", "yes", 0)
	tc.Flush()
	x := tc.Get("foo")
	if x != nil {
		t.Error("x is not nil:", x)
	}
	x = tc.Get("baz")
	if x != nil {
		t.Error("x is not nil:", x)
	}
}

func TestIncrementOverflowInt(t *testing.T) {
	tc := New()
	tc.Set("int8", int8(127), 0)
	_, err := tc.Increment("int8", 1)
	if err != nil {
		t.Error("Error incrementing int8:", err)
	}
	x := tc.Get("int8")
	int8 := x.(int8)
	if int8 != -128 {
		t.Error("int8 did not overflow as expected; value:", int8)
	}

}

func TestIncrementOverflowUint(t *testing.T) {
	tc := New()
	tc.Set("uint8", uint8(255), 0)
	_, err := tc.Increment("uint8", 1)
	if err != nil {
		t.Error("Error incrementing int8:", err)
	}
	x := tc.Get("uint8")
	uint8 := x.(uint8)
	if uint8 != 0 {
		t.Error("uint8 did not overflow as expected; value:", uint8)
	}
}

func TestDecrementUnderflowUint(t *testing.T) {
	tc := New()
	tc.Set("uint8", uint8(0), 0)
	_, err := tc.Decrement("uint8", 1)
	if err != nil {
		t.Error("Error decrementing int8:", err)
	}
	x := tc.Get("uint8")
	uint8 := x.(uint8)
	if uint8 != 0 {
		t.Error("uint8 was not capped at 0; value:", uint8)
	}
}

func TestCacheSerialization(t *testing.T) {
	tc := New()
	testFillAndSerialize(t, tc)

	// Check if gob.Register behaves properly even after multiple gob.Register
	// on c.Items (many of which will be the same type)
	testFillAndSerialize(t, tc)
}

func testFillAndSerialize(t *testing.T, tc *Cache) {
	tc.Set("a", "a", 0)
	tc.Set("b", "b", 0)
	tc.Set("c", "c", 0)
	tc.Set("expired", "foo", 1*time.Millisecond)
	tc.Set("*struct", &TestStruct{Num: 1}, 0)
	tc.Set("[]struct", []TestStruct{
		{Num: 2},
		{Num: 3},
	}, 0)
	tc.Set("[]*struct", []*TestStruct{
		&TestStruct{Num: 4},
		&TestStruct{Num: 5},
	}, 0)
	tc.Set("structception", &TestStruct{
		Num: 42,
		Children: []*TestStruct{
			&TestStruct{Num: 6174},
			&TestStruct{Num: 4716},
		},
	}, 0)

	fp := &bytes.Buffer{}
	err := tc.Save(fp)
	if err != nil {
		t.Fatal("Couldn't save cache to fp:", err)
	}

	oc := New()
	err = oc.Load(fp)
	if err != nil {
		t.Fatal("Couldn't load cache from fp:", err)
	}

	a := oc.Get("a")
	if a == nil {
		t.Error("a was not found")
	}
	if a.(string) != "a" {
		t.Error("a is not a")
	}

	b := oc.Get("b")
	if b == nil {
		t.Error("b was not found")
	}
	if b.(string) != "b" {
		t.Error("b is not b")
	}

	c := oc.Get("c")
	if c == nil {
		t.Error("c was not found")
	}
	if c.(string) != "c" {
		t.Error("c is not c")
	}

	<-time.After(5 * time.Millisecond)
	v := oc.Get("expired")
	if v != nil {
		t.Error("expired was found")
	}

	s1 := oc.Get("*struct")
	if s1 == nil {
		t.Error("*struct was not found")
	}
	if s1.(*TestStruct).Num != 1 {
		t.Error("*struct.Num is not 1")
	}

	s2 := oc.Get("[]struct")
	if s2 == nil {
		t.Error("[]struct was not found")
	}
	s2r := s2.([]TestStruct)
	if len(s2r) != 2 {
		t.Error("Length of s2r is not 2")
	}
	if s2r[0].Num != 2 {
		t.Error("s2r[0].Num is not 2")
	}
	if s2r[1].Num != 3 {
		t.Error("s2r[1].Num is not 3")
	}

	s3 := oc.Get("[]*struct")
	if s3 == nil {
		t.Error("[]*struct was not found")
	}
	s3r := s3.([]*TestStruct)
	if len(s3r) != 2 {
		t.Error("Length of s3r is not 2")
	}
	if s3r[0].Num != 4 {
		t.Error("s3r[0].Num is not 4")
	}
	if s3r[1].Num != 5 {
		t.Error("s3r[1].Num is not 5")
	}

	s4 := oc.Get("structception")
	if s4 == nil {
		t.Error("structception was not found")
	}
	s4r := s4.(*TestStruct)
	if len(s4r.Children) != 2 {
		t.Error("Length of s4r.Children is not 2")
	}
	if s4r.Children[0].Num != 6174 {
		t.Error("s4r.Children[0].Num is not 6174")
	}
	if s4r.Children[1].Num != 4716 {
		t.Error("s4r.Children[1].Num is not 4716")
	}
}

func TestFileSerialization(t *testing.T) {
	tc := New()
	tc.Set("a", "a", 0)
	tc.Set("b", "b", 0)
	f, err := ioutil.TempFile("", "go-cache-cache.dat")
	if err != nil {
		t.Fatal("Couldn't create cache file:", err)
	}
	fname := f.Name()
	f.Close()
	tc.SaveFile(fname)

	oc := New()
	oc.Set("a", "aa", 0) // this should not be overwritten
	err = oc.LoadFile(fname)
	if err != nil {
		t.Error(err)
	}
	a := oc.Get("a")
	if a == nil {
		t.Error("a was not found")
	}
	astr := a.(string)
	if astr != "aa" {
		if astr == "a" {
			t.Error("a was overwritten")
		} else {
			t.Error("a is not aa")
		}
	}
	b := oc.Get("b")
	if b == nil {
		t.Error("b was not found")
	}
	if b.(string) != "b" {
		t.Error("b is not b")
	}
}

func TestSerializeUnserializable(t *testing.T) {
	tc := New()
	ch := make(chan bool, 1)
	ch <- true
	tc.Set("chan", ch, 0)
	fp := &bytes.Buffer{}
	err := tc.Save(fp) // this should fail gracefully
	if err.Error() != "gob NewTypeObject can't handle type: chan bool" {
		t.Error("Error from Save was not gob NewTypeObject can't handle type chan bool:", err)
	}
}

func BenchmarkCacheGet(b *testing.B) {
	b.StopTimer()
	tc := New()
	tc.Set("foo", "bar", 0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.Get("foo")
	}
}

func BenchmarkMutexMapGet(b *testing.B) {
	b.StopTimer()
	m := map[string]string{
		"foo": "bar",
	}
	mu := sync.Mutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		_, _ = m["foo"]
		mu.Unlock()
	}
}

func BenchmarkCacheGetConcurrent(b *testing.B) {
	b.StopTimer()
	tc := New()
	tc.Set("foo", "bar", 0)
	wg := new(sync.WaitGroup)
	workers := runtime.NumCPU()
	each := b.N / workers
	wg.Add(workers)
	b.StartTimer()
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < each; j++ {
				tc.Get("foo")
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkMutexMapGetConcurrent(b *testing.B) {
	b.StopTimer()
	m := map[string]string{
		"foo": "bar",
	}
	mu := sync.Mutex{}
	wg := new(sync.WaitGroup)
	workers := runtime.NumCPU()
	each := b.N / workers
	wg.Add(workers)
	b.StartTimer()
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < each; j++ {
				mu.Lock()
				_, _ = m["foo"]
				mu.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkCacheGetManyConcurrent(b *testing.B) {
	// This is the same as BenchmarkCacheGetConcurrent, but its result
	// can be compared against BenchmarkShardedCacheGetManyConcurrent.
	b.StopTimer()
	n := 10000
	tc := New()
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		k := "foo" + strconv.Itoa(n)
		keys[i] = k
		tc.Set(k, "bar", 0)
	}
	each := b.N / n
	wg := new(sync.WaitGroup)
	wg.Add(n)
	for _, v := range keys {
		go func() {
			for j := 0; j < each; j++ {
				tc.Get(v)
			}
			wg.Done()
		}()
	}
	b.StartTimer()
	wg.Wait()
}

func BenchmarkShardedCacheGetManyConcurrent(b *testing.B) {
	b.StopTimer()
	n := 10000
	tsc := unexportedNewSharded(20, 0, 0)
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		k := "foo" + strconv.Itoa(n)
		keys[i] = k
		tsc.Set(k, "bar", 0)
	}
	each := b.N / n
	wg := new(sync.WaitGroup)
	wg.Add(n)
	for _, v := range keys {
		go func() {
			for j := 0; j < each; j++ {
				tsc.Get(v)
			}
			wg.Done()
		}()
	}
	b.StartTimer()
	wg.Wait()
}

func BenchmarkCacheSet(b *testing.B) {
	b.StopTimer()
	tc := New()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.Set("foo", "bar", 0)
	}
}

func BenchmarkMutexMapSet(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.Mutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		mu.Unlock()
	}
}

func BenchmarkCacheSetDelete(b *testing.B) {
	b.StopTimer()
	tc := New()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.Set("foo", "bar", 0)
		tc.Delete("foo")
	}
}

func BenchmarkMutexMapSetDelete(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.Mutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		mu.Unlock()
		mu.Lock()
		delete(m, "foo")
		mu.Unlock()
	}
}

func BenchmarkCacheSetDeleteSingleLock(b *testing.B) {
	b.StopTimer()
	tc := New()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.Lock()
		tc.set("foo", "bar", 0)
		tc.delete("foo")
		tc.Unlock()
	}
}

func BenchmarkMutexMapSetDeleteSingleLock(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.Mutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		delete(m, "foo")
		mu.Unlock()
	}
}
