package main

import (
	"context"
	gateway "fleetworker/gateway"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	database, admin := os.Getenv("DATABASE_URL"), os.Getenv("WORKER_ADMIN_TOKEN")
	if database == "" || admin == "" {
		log.Fatal("DATABASE_URL and WORKER_ADMIN_TOKEN are required")
	}
	pool, e := pgxpool.New(ctx, database)
	if e != nil {
		log.Fatal("database configuration invalid")
	}
	defer pool.Close()
	store := gateway.NewStore(pool, "worker_fleet")
	if directory := os.Getenv("WORKER_ARTIFACT_DIR"); directory != "" {
		store.Artifacts, e = gateway.NewArtifactFiles(directory)
		if e != nil {
			log.Fatal("artifact storage initialization failed")
		}
	}
	if e = store.Init(ctx); e != nil {
		log.Fatal("worker schema initialization failed")
	}
	go store.RunNotifications(ctx)
	go store.RunMaintenance(ctx)
	api := &gateway.API{Store: store, AdminToken: admin, ReleaseDir: os.Getenv("WORKER_RELEASE_DIR")}
	if u := os.Getenv("WORKER_AMQP_URL"); u != "" {
		go gateway.NewBridge(store, u).Run(ctx)
	}
	bind := os.Getenv("WORKER_BIND")
	if bind == "" {
		bind = "127.0.0.1:18220"
	}
	server := &http.Server{Addr: bind, Handler: api.Handler(), ReadHeaderTimeout: 15 * time.Second, IdleTimeout: 90 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		// Drain active streams; operators can still explicitly force-stop the process.
		server.Shutdown(context.Background())
	}()
	log.Print("worker gateway ready")
	if e = server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		log.Print("worker listener failed")
		stop()
	}
	<-done
}
