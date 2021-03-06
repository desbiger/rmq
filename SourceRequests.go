package rmq

import (
	"encoding/json"
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

type SourceRequest struct {
	Ip        string
	Ua        string
	Port      int
	DateTime  time.Time
	Url       string
	WafAction string
	AgentID   string
	Method    string
	Referer   string
}

func (request SourceRequest) ToJson() []byte {
	bytes, _ := json.Marshal(request)

	return bytes
}

type SourceRequests struct {
	sync.RWMutex
	list []SourceRequest
}

func (requests *SourceRequests) Register(request SourceRequest) {

	requests.list = append(requests.list, request)

	CountToDump, err := strconv.ParseInt(os.Getenv("CLICKHOUSE_COUNT_TO_DUMP"), 10, 16)
	requestsCount := len(requests.list)

	if err != nil {
		log.Print(err)
	}

	if requestsCount >= int(CountToDump) {

		mq, err := NewEngine(os.Getenv("MQ_HOST"), os.Getenv("MQ_USER"), os.Getenv("MQ_PASS"), os.Getenv("MQ_PORT"))

		if err != nil {
			log.Println("Error init NewEngine. Method Register SourceRequests",err.Error())
			return
		}

		mq.Send("dumpRequests", requests.ToJson())
		mq.Close()
		requests.Reset()

	}

}

func (requests *SourceRequests) ToJson() []byte {
	if requests.list == nil{
		return nil
	}
	bytes, err := json.Marshal(requests.list)
	if err != nil {
		if os.Getenv("DEBUG") == "true"{
 debug.PrintStack() 
} 
		log.Println("Error marshal to json SourceRequest.list",err)
	}
	return bytes
}

func (requests *SourceRequests) Reset() {
	requests.Lock()
	requests.list = make([]SourceRequest, 0)
	requests.Unlock()
}
