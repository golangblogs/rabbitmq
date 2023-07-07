#Installation
```Bash
Go get github. com/golangblogs/rabbitmq
```
#Cases
###1. Simple Mode - Production Messages
```Go
Import "github. com/golangblogs/rabbitmq"
//Golangblogs_ The name of the queue, customized
//Hello rabbitmq message content, custom
Rabbitmq. PublishQueue ("golangblogs_queue", "hello rabbitmq")
```
###2. Simple Mode - Consumer Message
```Go
Import（
Github. com/golangblogs/rabbitmq
）
Func main(){
//Golangblogs_ The name of the queue, customized
//Hello rabbitmq message content, custom
//Callback is the name of the callback function
Err:=rabbitmq. ConsumerQueue ("golangblogs_queue", callback)
If err= Nil{
Panic (err)
}
}
//Callback function
//S represents the content of the read queue
Func callback (s string){
//Print Data
Fmt. Println (s)
}
```