package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
)

func closer(cancel func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	cancel()
}

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	if len(os.Args) < 3 {
		fmt.Println("needs an arg")
		return
	}
	pgc, err := NewPGConsumer(ctx, os.Args[1])
	if err != nil {
		slog.Error(err.Error())
		return
	}
	defer pgc.Close()
	err = pgc.Start(ctx, os.Args[2])
	if err != nil {
		slog.Error(err.Error())
		return
	}
	closer(cancel)
}
