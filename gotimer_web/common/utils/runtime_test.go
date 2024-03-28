package utils

import (
	"fmt"
	"sync"
	"testing"
)

var wg sync.WaitGroup

//func TestGetCurrentGoroutineID(t *testing.T) {
//	for i := 0; i < 1; i++ {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//			fmt.Println("goroutinue start")
//			a := GetCurrentGoroutineID()
//			fmt.Println("a = ", a)
//		}()
//	}
//	wg.Wait()
//}

func TestGetProcessAndGoroutineIDStr(t *testing.T) {
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fmt.Println("goroutinue start")
			a := GetProcessAndGoroutineIDStr()
			b := GetCurrentGoroutineID()
			fmt.Println("a = ", a)
			fmt.Println("b = ", b)
		}()
	}
	wg.Wait()
}

/*
=== RUN   TestGetCurrentGoroutineID
goroutinue start
goroutinue start
buf =  [103 111 114 111 117 116 105 110 101 32 49 49 32 91 114 117 110 110 105 110 103 93 58 10 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 46 71 101 116 67 117 114 114 101 110 116 71 111 114 111 117 116 105 110 101 73 68 40 41 10 9 47 104 111 109 101 47 122 111 101 100 111 101 116 47 119 111 114 107 115 112 97 99 101 47 103 111 47 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 47 114 117 110 116 105 109 101 46 103 111]
stackinfo =  goroutine 11 [running]:
gotimer/common/utils.GetCurrentGoroutineID()
        /home/zoedoet/workspace/go/gotimer/common/utils/runtime.go
buf =  [103 111 114 111 117 116 105 110 101 32 55 32 91 114 117 110 110 105 110 103 93 58 10 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 46 71 101 116 67 117 114 114 101 110 116 71 111 114 111 117 116 105 110 101 73 68 40 41 10 9 47 104 111 109 101 47 122 111 101 100 111 101 116 47 119 111 114 107 115 112 97 99 101 47 103 111 47 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 47 114 117 110 116 105 109 101 46 103 111 58]
goroutinue start
goroutinue start
buf =  [103 111 114 111 117 116 105 110 101 32 56 32 91 114 117 110 110 105 110 103 93 58 10 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 46 71 101 116 67 117 114 114 101 110 116 71 111 114 111 117 116 105 110 101 73 68 40 41 10 9 47 104 111 109 101 47 122 111 101 100 111 101 116 47 119 111 114 107 115 112 97 99 101 47 103 111 47 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 47 114 117 110 116 105 109 101 46 103 111 58]
stackinfo =  goroutine 8 [running]:
gotimer/common/utils.GetCurrentGoroutineID()
        /home/zoedoet/workspace/go/gotimer/common/utils/runtime.go:
buf =  [103 111 114 111 117 116 105 110 101 32 57 32 91 114 117 110 110 105 110 103 93 58 10 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 46 71 101 116 67 117 114 114 101 110 116 71 111 114 111 117 116 105 110 101 73 68 40 41 10 9 47 104 111 109 101 47 122 111 101 100 111 101 116 47 119 111 114 107 115 112 97 99 101 47 103 111 47 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 47 114 117 110 116 105 109 101 46 103 111 58]
stackinfo =  goroutine 9 [running]:
gotimer/common/utils.GetCurrentGoroutineID()
        /home/zoedoet/workspace/go/gotimer/common/utils/runtime.go:
a =  9
a =  11
a =  8
stackinfo =  goroutine 7 [running]:
gotimer/common/utils.GetCurrentGoroutineID()
        /home/zoedoet/workspace/go/gotimer/common/utils/runtime.go:
a =  7
goroutinue start
buf =  [103 111 114 111 117 116 105 110 101 32 49 48 32 91 114 117 110 110 105 110 103 93 58 10 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 46 71 101 116 67 117 114 114 101 110 116 71 111 114 111 117 116 105 110 101 73 68 40 41 10 9 47 104 111 109 101 47 122 111 101 100 111 101 116 47 119 111 114 107 115 112 97 99 101 47 103 111 47 103 111 116 105 109 101 114 47 99 111 109 109 111 110 47 117 116 105 108 115 47 114 117 110 116 105 109 101 46 103 111]
stackinfo =  goroutine 10 [running]:
gotimer/common/utils.GetCurrentGoroutineID()
        /home/zoedoet/workspace/go/gotimer/common/utils/runtime.go
a =  10
--- PASS: TestGetCurrentGoroutineID (0.00s)
PASS
ok      gotimer/common/utils    0.001s
*/
