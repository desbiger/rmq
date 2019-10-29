package RabbitMQ_go

import (
	"github.com/joho/godotenv"
	"log"
	"os"
	"testing"
)
func init(){
	err := godotenv.Load("../../../../.env")
	if err != nil {
		log.Fatal(err)
	}
}

func TestEngine_Send(t *testing.T) {
	mq,_ := NewEngine(os.Getenv("MQ_HOST"),os.Getenv("MQ_USER"), os.Getenv("MQ_PASS"), os.Getenv("MQ_PORT"))

	for i:=0;i < 1000; i++{
		mq.Send("test", []byte("qweqweqweqwe"))
	}
}

func TestEngine_Get(t *testing.T) {
	mq ,_ := NewEngine(os.Getenv("MQ_HOST"),os.Getenv("MQ_USER"), os.Getenv("MQ_PASS"), os.Getenv("MQ_PORT"))
	for i:=0;i < 1000; i++{
		mq.Get("test")
	}
}
