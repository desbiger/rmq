package rmq

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
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
	fatalOnError(err)

	defer conn.Close()

	ch, err := conn.Channel()
	fatalOnError(err)

	res, _, _ := ch.Get(Action, true)

	return res.Body
}

func (engine *Engine) Send(s string, body []byte) {

	ch, err := engine.Connection.Channel()
	if err != nil {
		log.Println(err)
		return
	}

	_, err = ch.QueueDeclare(s, true, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	err = ch.Publish("", s, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	})
	if err != nil {
		log.Println(err)
		return
	}

}
func (engine *Engine) RPC(body []byte, agent string) ([]byte, error) {

	rpcID := "1"
	ch, err := engine.Connection.Channel()
	if err != nil {
		debug.PrintStack()
		log.Println(err)
	}
	defer ch.Close()

	ReplyTo, err := ch.QueueDeclare("", false, false, true, false,
		amqp.Table{
			"x-message-ttl": 1000,
			"x-expires":     3000,
		},
	)
	if err != nil {
		debug.PrintStack()
		return nil, err
	}
	err = ch.Publish("", "RPC", false, false, amqp.Publishing{
		ContentType:   "application/json",
		Body:          body,
		ReplyTo:       ReplyTo.Name,
		CorrelationId: rpcID,
	})
	if err != nil {
		debug.PrintStack()
		return nil, err
	}



	res, err := ch.Consume(ReplyTo.Name, agent, true, false, false, false, nil)
	if err != nil {
		debug.PrintStack()
		log.Println(err)
	}

	go func() {
		time.Sleep(2 * time.Second)
		err = ch.Cancel(agent, false)
		if err != nil{
			log.Println(err)
			debug.PrintStack()
		}
	}()

	for msg := range res {
		return msg.Body, err
	}

	return nil, errors.New("No answer from server ")
}
func (engine *Engine) ListenSourceMessage(s string, exclusive bool, Func func(msg amqp.Delivery, connection *amqp.Connection)) {

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", engine.User, engine.Pass, engine.Host, engine.Port))
	fatalOnError(err)

	defer conn.Close()

	ch, err := conn.Channel()
	defer ch.Close()

	fatalOnError(err)
	_, err = ch.QueueDeclare(s, true, false, false, false, nil)
	fatalOnError(err)

	msgs, err := ch.Consume(s, "RootServer", true, exclusive, false, false, nil)

	if err != nil {
		log.Println(err)
	}
	for d := range msgs {
		go Func(d, conn)
	}
}

func (engine *Engine) Listen(s string, exclusive bool, Func func(res []byte)) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", engine.User, engine.Pass, engine.Host, engine.Port))
	fatalOnError(err)

	defer conn.Close()

	ch, err := conn.Channel()
	fatalOnError(err)
	_, err = ch.QueueDeclare(s, true, false, false, false, nil)
	msgs, err := ch.Consume(s, "RootServer", true, exclusive, false, false, nil)

	for d := range msgs {
		log.Println(d.ReplyTo)
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
		return nil, err
	}

	engine.Connection = conn
	return engine, nil
}

func fatalOnError(err error) {
	if err != nil {
		log.Println(err.Error())
	}
}
