package main

import "fmt"

type AppContext struct {
	ConnectionString string
	Topic            string
	Subscription     string
}

func PrintContext(ctx *AppContext) {
	fmt.Printf("Connection String: %s\n", ctx.ConnectionString)
	fmt.Printf("Active topic: %s\n", ctx.Topic)
	fmt.Printf("Active subscription: %s\n", ctx.Subscription)
}

func (ctx *AppContext) SetConnectionString(connStr string) {
	ctx.ConnectionString = connStr
}

func (ctx *AppContext) SetTopic(topic string) {
	ctx.Topic = topic
}

func (ctx *AppContext) Clear() {
	ctx.ConnectionString = ""
	ctx.Topic = ""
}
