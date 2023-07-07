package golangblogs_rabbitmq

import (
	"bytes"
	"fmt"

	"github.com/streadway/amqp"
)

type Callback func(msg string)

func Connect() (*amqp.Connection, error) {
	//消费者读配置信息的值会报错
	//conn, err := amqp.Dial(beego.AppConfig.String("mqhost"))
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	return conn, err
}

// 发送端函数
func Publish(exchange string, queueName string, body string) error {
	//建立连接
	conn, err := Connect()
	if err != nil {
		return err
	}
	//关闭链接
	defer conn.Close()

	//创建通道channel
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	//关闭通道
	defer channel.Close()

	//创建队列
	q, err := channel.QueueDeclare(
		queueName, //传过来的队列名称
		true,      //是否持久化
		false,     //是否自动删除
		false,
		false, // 是否等待服务器确认
		nil,   //其它配置
	)
	if err != nil {
		return err
	}

	//发送消息
	//exchange 交换机的名称
	err = channel.Publish(exchange, q.Name, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, //amqp.Persistent 设置为持久化消息
		ContentType:  "text/plain",
		Body:         []byte(body),
	})
	return err
}

// 接收者方法
func Consumer(exchange string, queueName string, callback Callback) {
	//建立连接
	conn, err := Connect()
	defer conn.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	//创建通道channel
	channel, err := conn.Channel()
	defer channel.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	//创建queue
	q, err := channel.QueueDeclare(
		queueName,
		true, //持久化消息
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	//channel.Consume 从队列中获取数据
	//第三个参数是否自动应答案 false 则为手动应答
	msgs, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	//在起一个channel（不退出）
	//通道的发送使用特殊的操作符<-，将数据通过通道发送的格式为
	forever := make(chan bool)
	//守护进程不能退出
	go func() {
		for d := range msgs {
			//转换成string型
			s := BytesToString(&(d.Body))
			//回调函数执行一下
			callback(*s)
			//回调处理成功之后手动确认消息
			d.Ack(false)
		}
	}()
	fmt.Printf("Waiting for messages")
	<-forever
}

// 转换成string型
func BytesToString(b *[]byte) *string {
	s := bytes.NewBuffer(*b)
	r := s.String()
	return &r
}

// 发布订阅模式 生产端
// exchange 交换机的名字
// types 交换机的类型
func PublishEx(exchange string, types string, routingKey string, body string) error {
	//建立连接
	conn, err := Connect()
	defer conn.Close()
	if err != nil {
		return err
	}
	//创建channel
	channel, err := conn.Channel()
	defer channel.Close()
	if err != nil {
		return err
	}

	//创建交换机
	err = channel.ExchangeDeclare(
		exchange,
		types,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = channel.Publish(exchange, routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, //持久化
		ContentType:  "text/plain",
		Body:         []byte(body),
	})
	return err
}

// 发布订阅模式 消费端
func ConsumerEx(exchange string, types string, routingKey string, callback Callback) {
	//建立连接
	conn, err := Connect()
	defer conn.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	//创建通道channel
	channel, err := conn.Channel()
	defer channel.Close()
	if err != nil {
		//如果有问题进行打印出来
		fmt.Println(err)
		return
	}

	//创建交换机
	err = channel.ExchangeDeclare(
		exchange,
		types,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	//创建队列
	q, err := channel.QueueDeclare(
		"", //临时队列，没有名字
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	//队列创建完成之后 队列要与交换机绑定起来
	err = channel.QueueBind(
		q.Name,
		routingKey,
		exchange,
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	msgs, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			s := BytesToString(&(d.Body))
			callback(*s)
			d.Ack(false)
		}
	}()
	fmt.Printf("Waiting for messages\n")
	<-forever
}

// 死信队列生产端
// 死信队列会把消息放到A交换机
// body 信息
func PublishDlx(exchangeA string, body string) error {
	//建立连接
	conn, err := Connect()
	if err != nil {
		return err
	}
	defer conn.Close()

	//创建一个Channel
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	//消息发送到A交换机
	err = channel.Publish(exchangeA, "", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, //持久化消息
		ContentType:  "text/plain",
		Body:         []byte(body), //消息内容
	})

	return err
}

// 死信队列消费端
func ConsumerDlx(exchangeA string, queueAName string, exchangeB string, queueBName string, ttl int, callback Callback) {
	//建立连接
	conn, err := Connect()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	//创建一个Channel
	channel, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer channel.Close()

	// 思路 ：1.创建A交换机 2.创建A队列 3.A交换机和A队列绑定

	//创建A交换机
	err = channel.ExchangeDeclare(
		exchangeA, // A交换机名称
		"fanout",  // 订阅类型（扇形交换机）
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	//创建一个A队列 queue，指定消息过期时间，并且绑定过期以后发送到那个交换机
	queueA, err := channel.QueueDeclare(
		queueAName, // name
		true,       // durable 持久化消息
		false,      // delete when usused
		false,      // exclusive
		false,      // no-wait
		amqp.Table{
			// 当消息过期时把消息发送到 exchangeB
			"x-message-ttl":          ttl,        //设置A交换机过期时间
			"x-dead-letter-exchange": exchangeB,  //绑定B交换机
			"x-dead-letter-queue":    queueBName, //绑定哪个队列
			//"x-dead-letter-routing-key" :			//绑定哪个关键字
		},
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	//A交换机和A队列绑定
	err = channel.QueueBind(
		queueA.Name, // queue name
		"",          // routing key
		exchangeA,   // exchange
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	//A交换机和A队列操作完成了，接下来
	//创建B交换机
	//创建B队列
	//B交换机和B队列绑定
	err = channel.ExchangeDeclare(
		exchangeB, // name
		"fanout",  // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	//创建一个queue
	queueB, err := channel.QueueDeclare(
		queueBName, // name
		true,       // durable
		false,      // delete when usused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	//B交换机和B队列绑定
	err = channel.QueueBind(
		queueB.Name, // queue name
		"",          // routing key
		exchangeB,   // exchange
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	//接收消息 B交换机在接收B队列的信息，不是接收A队列的信息
	msgs, err := channel.Consume(queueB.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			s := BytesToString(&(d.Body))
			callback(*s)
			d.Ack(false)
		}
	}()

	fmt.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")
	<-forever
}
