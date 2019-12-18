package rmq

import (
	"fmt"
	"github.com/k0kubun/pp"
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
	ch,err := engine.Connection.Channel()
	if err != nil{
		log.Println(err)
	}
	defer ch.Close()

	ReplayTo, err := ch.QueueDeclare("RPC_ANSWERS", false, false, false, true, nil)
	if err != nil {
		return nil, err
	}
	err = ch.Publish("", "RPC", false, false, amqp.Publishing{
		ContentType:   "application/json",
		Body:          body,
		ReplyTo:       ReplayTo.Name,
		CorrelationId: rpcID,
	})
	if err != nil {

		return nil, err
	}

	res, err := ch.Consume(ReplayTo.Name, agent, false, false, false, false, nil)
	if err != nil {
		pp.Println(err)
		log.Println(err)
	}
	for msg := range res {
		if msg.CorrelationId == rpcID {
			err := msg.Ack(true)
			if err != nil {
				log.Println(err)
			}
			return msg.Body, err
		}

	}

	return nil, nil
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
