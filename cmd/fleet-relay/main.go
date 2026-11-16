package main

import (
	"context"
	gateway "fleetworker/gateway"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	database, url := os.Getenv("DATABASE_URL"), os.Getenv("WORKER_AMQP_URL")
	if database == "" || url == "" {
		log.Fatal("DATABASE_URL and WORKER_AMQP_URL are required")
	}
	pool, e := pgxpool.New(ctx, database)
	if e != nil {
		log.Fatal("database configuration invalid")
	}
	defer pool.Close()
	store := gateway.NewStore(pool, "worker_fleet")
	if e = store.Init(ctx); e != nil {
		log.Fatal("worker schema initialization failed")
	}
	gateway.NewBridge(store, url).Run(ctx)
}
