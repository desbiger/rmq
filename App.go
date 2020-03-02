package rmq

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"runtime/debug"
	"time"
)

type Engine struct {
	Host       string
	User       string
	Pass       string
	Port       string
	Connection *amqp.Connection
}

func (engine *Engine) Get(Action string) []byte {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", engine.User, engine.Pass, engine.Host, engine.Port))
	fatalOnError(err, "Error connection to RabbitMQ Dial. Method Get ")

	defer conn.Close()

	ch, err := conn.Channel()
	fatalOnError(err, "Error channel connection. Method Get. ")

	res, _, _ := ch.Get(Action, true)

	return res.Body
}

func (engine *Engine) Send(s string, body []byte) {

	ch, err := engine.Connection.Channel()
	if err != nil {
		if os.Getenv("DEBUG") == "true" {
			debug.PrintStack()
		}
		log.Println("Error init rqm channel. Method Send. ", err)
		return
	}

	_, err = ch.QueueDeclare(s, true, false, false, false, nil)
	if err != nil {
		if os.Getenv("DEBUG") == "true" {
			debug.PrintStack()
		}
		log.Println("Error declare queue", err)
		return
	}

	err = ch.Publish("", s, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	})
	if err != nil {
		if os.Getenv("DEBUG") == "true" {
			debug.PrintStack()
		}
		log.Println("Error publish ro rqm", err)
		return
	}

}
func (engine *Engine) RPC(body []byte, agent string) ([]byte, error) {

	chCloses := make(chan bool)
	rpcID := "1"
	ch, err := engine.Connection.Channel()
	if err != nil {

		if os.Getenv("DEBUG") == "true" {
			debug.PrintStack()
		}
		log.Println("Error init rqm channel", err)

		return nil, err
	}
	defer func() {
		e := ch.Close()
		if e != nil {
			log.Println("Error closing channel. Method RPC.", e)
		}
	}()

	ReplyTo, err := ch.QueueDeclare("", false, false, true, false,
		amqp.Table{
			"x-message-ttl": 1000,
			"x-expires":     3000,
		},
	)
	if err != nil {

		return nil, err
	}
	err = ch.Publish("", "RPC", false, false, amqp.Publishing{
		ContentType:   "application/json",
		Body:          body,
		ReplyTo:       ReplyTo.Name,
		AppId:         agent,
		CorrelationId: rpcID,
	})
	if err != nil {

		return nil, err
	}

	res, err := ch.Consume(ReplyTo.Name, agent, true, false, false, false, nil)
	if err != nil {

		if os.Getenv("DEBUG") == "true" {
			debug.PrintStack()
		}
		log.Println("Error consume to queue"+ReplyTo.Name, err)
	}

	go func() {
		time.Sleep(1 * time.Second)
		select {
		case _ = <-chCloses:
			return
		default:
			err = ch.Cancel(agent, false)
			if err != nil {
				if os.Getenv("DEBUG") == "true" {
					debug.PrintStack()
				}
				log.Println("Error cancel channel. Method RPC ", err)
			}
			close(chCloses)
			return
		}
	}()

	for msg := range res {
		return msg.Body, err
	}

	return nil, errors.New("No answer from server ")
}
func (engine *Engine) ListenSourceMessage(s string, exclusive bool, Func func(msg amqp.Delivery, connection *amqp.Connection)) {

	conn := engine.Connection

	ch, err := conn.Channel()
	defer ch.Close()

	fatalOnError(err, "Error channel connection. Method ListenSourceMessage")
	_, err = ch.QueueDeclare(s, true, false, false, false, nil)
	fatalOnError(err, "Error declare queue. Method ListenSourceMessage. ")

	msgs, err := ch.Consume(s, "RootServer", true, exclusive, false, false, nil)

	if err != nil {
		if os.Getenv("DEBUG") == "true" {
			debug.PrintStack()
		}
		log.Println("Error consume to queue"+s, err)
	}
	for d := range msgs {
		go Func(d, conn)
	}
}

func (engine *Engine) Listen(s string, exclusive bool, Func func(res []byte)) {
	conn := engine.Connection

	defer conn.Close()

	ch, err := conn.Channel()
	fatalOnError(err, "Error channel connection. Method Listen.")
	defer ch.Close()

	_, err = ch.QueueDeclare(s, true, false, false, false, nil)
	msgs, err := ch.Consume(s, "RootServer", true, exclusive, false, false, nil)

	if err != nil {
		log.Println("Error consuming queue "+s+".Method Listen", err)
		return
	}

	for d := range msgs {
		Func(d.Body)
	}
}

func (engine *Engine) Close() {
	engine.Connection.Close()
}

func NewEngine(Host string, User string, Pass string, Port string) (*Engine, error) {

	engine := &Engine{
		Host: Host,
		User: User,
		Pass: Pass,
		Port: Port,
	}

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", engine.User, engine.Pass, engine.Host, engine.Port))
	if err != nil {
		fatalOnError(err, "Error RMQ connection. Method NewEngine")
		return nil, err
	}

	engine.Connection = conn
	return engine, nil
}

func fatalOnError(err error, i string) {
	if err != nil {
		log.Println(i, err.Error())
	}
}
