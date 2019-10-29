package rmq

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

type SourceRequest struct {
	Ip       string
	Ua       string
	Port     int
	DateTime time.Time
	Url      string
	AgentID  int
}

func (request SourceRequest) ToJson() []byte {
	bytes, _ := json.Marshal(request)

	return bytes
}

type SourceRequests []SourceRequest

func (requests *SourceRequests) Register(request SourceRequest) {

	*requests = append(*requests, request)

	if len(*requests) >= 100 {

		mq, err := NewEngine(os.Getenv("MQ_HOST"), os.Getenv("MQ_USER"), os.Getenv("MQ_PASS"), os.Getenv("MQ_PORT"))
		if err != nil {
			log.Println(err.Error())
			return
		}

		for _, r := range *requests {
			mq.Send("dumpRequests", r.ToJson())
		}
		*requests = []SourceRequest{}

		_ = mq.Connection.Close()
	}

}

func (requests *SourceRequests) ToJson() []byte {
	bytes, _ := json.Marshal(requests)

	return bytes
}