# Installation

```bash
go get github.com/golangblogs/rabbitmq
```

# Example

### 1.Simple Mode - Production Messages


```go
import "github.com/golangblogs/rabbitmq"

//Golangblogs_ The name of the queue, customized
//Hello rabbitmq message content, custom
rabbitmq.PublishQueue("golangblogs_queue", "hello rabbitmq")
```


### 2. Simple Mode - Consumer Message

```go

import (
	"github.com/golangblogs/rabbitmq"
)

func main() {
    //Golangblogs_ The name of the queue, customized
    //Hello rabbitmq message content, custom
    //Callback is the name of the callback function
	err := rabbitmq.ConsumerQueue("golangblogs_queue", callback)
	if err != nil {
		panic(err)
	}
}

//Callback function
//S represents the content of the read queue
func callback(s string) {
    //Print Data
	fmt.Println(s)
}

```
