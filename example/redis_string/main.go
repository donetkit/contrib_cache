package main

import (
	"fmt"
	"github.com/donetkit/contrib_cache/example/client"
	"github.com/donetkit/contrib_cache/redis"
	"time"
)

func main() {

	rdb := client.NewClient()

	rdb.Set("a", "123456", time.Second*60)
	fmt.Println(rdb.GetString("a"))
	fmt.Println(rdb.Get("a"))

	type TestData struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}

	testData := ([]*TestData)(nil)
	testData = append(testData, &TestData{Id: "Id1", Name: "Name1"})
	testData = append(testData, &TestData{Id: "Id2", Name: "Name2"})
	testData = append(testData, &TestData{Id: "Id3", Name: "Name3"})
	testData = append(testData, &TestData{Id: "Id4", Name: "Name4"})

	b, _ := redis.Marshal(testData)
	rdb.SetString("b", string(b), time.Second*60)
	s1, _ := rdb.GetString("b")
	fmt.Println(s1)
	fmt.Println(rdb.Get("b"))

	rdb.Set("c", testData, time.Second*60)
	s2, _ := rdb.GetString("c")

	if s1 == s2 {
		fmt.Println("s1 == s2")
	}
	testData1 := ([]*TestData)(nil)
	rdb.Get("c", &testData1)

	if len(testData1) == len(testData) {
		fmt.Println("s1 == s3")
	}

}
