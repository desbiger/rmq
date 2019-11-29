package rmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Engine struct {
	Host       string
	User       string
	Pass       string
	Port       string
	Connection *amqp.Connection
}

func (engine Engine) Get(Action string) []byte {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", engine.User, engine.Pass, engine.Host, engine.Port))
	fatalOnError(err)

	defer conn.Close()

	ch, err := conn.Channel()
	fatalOnError(err)

	res, _, _ := ch.Get(Action, true)

	return res.Body
}

func (engine Engine) Send(s string, body []byte) {

	ch, err := engine.Connection.Channel()
	if err != nil {
		log.Println(err)
		return
	}

	_, err = ch.QueueDeclare(s, false, false, false, false, nil)
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
func (engine Engine) RPC(body []byte,agent string) ([]byte, error) {

	ch, err := engine.Connection.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return nil, err
	}

	err = ch.Publish("", "RPC", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
		ReplyTo:     q.Name,
	})
	if err != nil {
		return nil, err
	}

	res, err := ch.Consume(q.Name, agent, true, false, false, false, nil)

	for msg := range res {
		_ = ch.Close()
		return msg.Body, err
	}
	return nil, nil
}

func (engine *Engine) Listen(s string, exclusive bool, Func func(res []byte)) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", engine.User, engine.Pass, engine.Host, engine.Port))
	fatalOnError(err)

	defer conn.Close()

	ch, err := conn.Channel()
	fatalOnError(err)

	msgs, err := ch.Consume(s, "hoff", true, exclusive, false, false, nil)
	for d := range msgs {
		log.Println("geting data")
		Func(d.Body)
	}
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
