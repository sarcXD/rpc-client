package main

import (
	"fmt"
	"log"
	"math"
	"net/rpc"
	"time"

	pssTypes "github.com/sarcxd/psstore/constants"
)

var MaxCount int32 = 1000000
var chanSz int32 = 1024

// Just demonstrates why its a horrible idea to use this to insert many things
// A good demo of perhaps the system db under load
//
// ALSO OUTDATED
func RepeatAdds(client *rpc.Client) {
	var i int32
	var addArgs pssTypes.InsArgs_i32
	chanSz = MaxCount
	respChan := make(chan *rpc.Call, chanSz)
	replyChan := make(chan *rpc.Call, chanSz)
	sTime := time.Now()
	go func() {
		for i = 0; i < MaxCount; i++ {
			addArgs.Key = fmt.Sprintf("testKey%d", i)
			addArgs.Value = i
			addReply := 0
			res := client.Go("SimpleStore.Add", addArgs, &addReply, respChan)
			replyChan <- res
		}
		close(replyChan)
	}()

	var succCount int32 = 0
	errList := make([]string, 0, MaxCount)
	for passedVal := range replyChan {
		x := <-passedVal.Done
		succ := *((x.Reply).(*int))
		if succ == 0 {
			errList = append(errList, (x.Args).(pssTypes.InsArgs_i32).Key)
		}
		succCount += int32(succ)
	}
	endTime := time.Now()
	duration := endTime.Sub(sTime)
	if succCount < MaxCount {
		fmt.Println("Error adding bulk operations with keys", errList)
	}
	fmt.Printf("Repeat Add Complete!\n%d/%d operations successful\nelapsed(ms): %d\nelapsed(mics): %d\n", succCount, MaxCount, duration.Milliseconds(), duration.Microseconds())
}

func BulkAddI32(client *rpc.Client) {
	var i int32
	var args pssTypes.BulkInsArgs_i32
	args.InsList = make([]pssTypes.CheckedIns_i32, 0, MaxCount)
	var reply pssTypes.BulkInsReply_i32
	reply.Rejects = make([]pssTypes.InsArgs_i32, 0, MaxCount)
	sTime := time.Now()
	for i = 0; i < MaxCount; i++ {
		var ins pssTypes.CheckedIns_i32
		ins.Key = fmt.Sprintf("testKey%d", i)
		ins.Value = i
		args.InsList = append(args.InsList, ins)
	}
	err := client.Call("SimpleKV.BulkAddI32", args, &reply)
	endTime := time.Now()
	duration := endTime.Sub(sTime)
	if err != nil {
		fmt.Println("BulkAddI32 error", err)
		return
	}
	output := fmt.Sprintf("BulkAddI32 complete\nelapsed(ms): %d\nelapsed(mics): %d\n", duration.Milliseconds(), duration.Microseconds())
	if len(reply.Rejects) > 1 {
		output += fmt.Sprintf("WARNING:\n\t%d values were rejected\n", len(reply.Rejects))
	}
	fmt.Println(output)
}

func BulkUpdateI32(client *rpc.Client) {
	var i int32
	var args pssTypes.BulkInsArgs_i32
	args.InsList = make([]pssTypes.CheckedIns_i32, 0, MaxCount)
	var reply pssTypes.BulkInsReply_i32
	reply.Rejects = make([]pssTypes.InsArgs_i32, 0, MaxCount)
	sTime := time.Now()
	for i = 0 + 500; i < MaxCount+500; i++ {
		var ins pssTypes.CheckedIns_i32
		ins.Key = fmt.Sprintf("testKey%d", i)
		ins.Value = i + 45
		args.InsList = append(args.InsList, ins)
	}
	err := client.Call("SimpleKV.BulkUpdateI32", args, &reply)
	endTime := time.Now()
	duration := endTime.Sub(sTime)
	if err != nil {
		fmt.Println("BulkUpdateI32 error", err)
		return
	}
	output := fmt.Sprintf("BulkUpdateI32 complete\nelapsed(ms): %d\nelapsed(mics): %d\n", duration.Milliseconds(), duration.Microseconds())
	if len(reply.Rejects) > 1 {
		output += fmt.Sprintf("WARNING:\n\t%d values were rejected\n", len(reply.Rejects))
	}
	fmt.Println(output)
}

func GetI32(client *rpc.Client) {
	var err error
	key := fmt.Sprintf("testKey%d", 10)
	getReply := 0
	err = client.Call("SimpleKV.GetI32", key, &getReply)
	if err != nil {
		fmt.Printf("SimpleKV.GetI32(%s): %v\n", key, err)
	}
	fmt.Printf("Getted Value %d\n", getReply)
}

func main() {
	client, err := rpc.DialHTTP("tcp", ":8020")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	reply := -1
	var arg string
	err = client.Call("SimpleKV.ClearI32", arg, &reply)
	if err != nil {
		fmt.Println(err)
	}
	if MaxCount < 10 {
		chanSz = int32(math.Floor(float64(MaxCount) / 4.0))
	}
	// RepeatAdds(client)
	BulkAddI32(client)
	BulkUpdateI32(client)
	GetI32(client)
}
