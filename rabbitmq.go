package rabbitmq

import (
	"bytes"
	"fmt"

	"github.com/streadway/amqp"
)

// 调用消费者返回的回调函数，返回的值就是消息内容
// msg 消息内容
type Callback func(msg string)

// 默认rabbitmq的链接地址&账号密码
var (
	host     = "localhost"
	port     = 5672
	name     = "guest"
	password = "guest"
)

// 进行初始化rabbitmq的链接信息，如果不初始化则使用默认的数据
func InitConfig(rabbitMqHost string, rabbitMqPort int, rabbitMqName string, rabbitMqPassword string) {
	host = rabbitMqHost
	port = rabbitMqPort
	name = rabbitMqName
	password = rabbitMqPassword
}

// 链接rabbitmq
func Connect(name, password, host string, port int) (*amqp.Connection, error) {
	//消费者读配置信息的值会报错
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/", name, password, host, port))
	return conn, err
}

// PublishQueue 简单模式之生产者，直接把消息推送到队列中
// queueName 队列名
// body消息内容
func PublishQueue(queueName string, body string) error {
	//建立连接
	conn, err := Connect(name, password, host, port)
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
	err = channel.Publish("", q.Name, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, //amqp.Persistent 设置为持久化消息
		ContentType:  "text/plain",
		Body:         []byte(body),
	})
	if err != nil {
		return err
	}

	return nil
}

// ConsumerQueue 简单模式之消费者，直接从队列中读取消息进行消费
// queueName 队列名
// callback回调函数
func ConsumerQueue(queueName string, callback Callback) error {
	//建立连接
	conn, err := Connect(name, password, host, port)
	defer conn.Close()
	if err != nil {
		return err
	}

	//创建通道channel
	channel, err := conn.Channel()
	defer channel.Close()
	if err != nil {
		return err
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
		return err
	}

	//channel.Consume 从队列中获取数据
	//第三个参数是否自动应答案 false 则为手动应答
	msgs, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
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
	return nil
}

// 发布订阅模式 生产端
// exchange 交换机的名字
// types 交换机的类型
func PublishExChange(exchangeName string, types string, routingKey string, body string) error {
	//建立连接
	conn, err := Connect(name, password, host, port)
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
		exchangeName,
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

	err = channel.Publish(exchangeName, routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, //持久化
		ContentType:  "text/plain",
		Body:         []byte(body),
	})
	if err != nil {
		return err
	}
	return nil
}

// 发布订阅模式 消费端
func ConsumerExChange(exchangeName string, types string, routingKey string, callback Callback) error {
	//建立连接
	conn, err := Connect(name, password, host, port)
	defer conn.Close()
	if err != nil {
		return err
	}
	//创建通道channel
	channel, err := conn.Channel()
	defer channel.Close()
	if err != nil {
		return err
	}

	//创建交换机
	err = channel.ExchangeDeclare(
		exchangeName,
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
		return err
	}

	//队列创建完成之后 队列要与交换机绑定起来
	err = channel.QueueBind(
		q.Name,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	msgs, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
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
	return nil
}

// 死信队列生产端
// 死信队列会把消息放到A交换机
// body 信息
func PublishDlx(exchangeA string, body string) error {
	//建立连接
	conn, err := Connect(name, password, host, port)
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

	if err != nil {
		return err
	}
	return nil
}

// 死信队列消费端
func ConsumerDlx(exchangeA string, queueAName string, exchangeB string, queueBName string, ttl int, callback Callback) error {
	//建立连接
	conn, err := Connect(name, password, host, port)
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
		return err
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
		return err
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
		return err
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
		return err
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
		return err
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
		return err
	}

	//接收消息 B交换机在接收B队列的信息，不是接收A队列的信息
	msgs, err := channel.Consume(queueB.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
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
	return nil
}

// 转换成string型
func BytesToString(b *[]byte) *string {
	s := bytes.NewBuffer(*b)
	r := s.String()
	return &r
}
