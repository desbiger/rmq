package rmq

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
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
}

func (request SourceRequest) ToJson() []byte {
	bytes, _ := json.Marshal(request)

	return bytes
}

type SourceRequests []SourceRequest

func (requests *SourceRequests) Register(request SourceRequest) {

	*requests = append(*requests, request)

	CountToDump, err := strconv.ParseInt(os.Getenv("CLICKHOUSE_COUNT_TO_DUMP"), 10, 16)

	if err != nil {
		log.Print(err)
	}

	if len(*requests) >= int(CountToDump) {

		mq, err := NewEngine(os.Getenv("MQ_HOST"), os.Getenv("MQ_USER"), os.Getenv("MQ_PASS"), os.Getenv("MQ_PORT"))
		if err != nil {
			log.Println(err.Error())
			return
		}

		mq.Send("dumpRequests", requests.ToJson())
		*requests = []SourceRequest{}

		_ = mq.Connection.Close()
	}

}

func (requests SourceRequests) ToJson() []byte {
	bytes, _ := json.Marshal(requests)

	return bytes
}
