# 安装

```bash
go get github.com/golangblogs/rabbitmq
```

# 案例

### 1.简单模式-生产消息


```go
import "github.com/golangblogs/rabbitmq"

//golangblogs_queue 队列的名字，自定义
//hello rabbitmq 消息内容，自定义
rabbitmq.PublishQueue("golangblogs_queue", "hello rabbitmq")
```


### 2.简单模式-消费消息

```go

import (
	"github.com/golangblogs/rabbitmq"
)

func main() {
    //golangblogs_queue 队列的名字，自定义
    //hello rabbitmq 消息内容，自定义
	//callback 为回调函数名
	err := rabbitmq.ConsumerQueue("golangblogs_queue", callback)
	if err != nil {
		panic(err)
	}
}

//回调函数
//s 代表读取队列的内容
func callback(s string) {
	//打印数据
	fmt.Println(s)
}

```
