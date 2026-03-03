package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func pgxconfidk() *pgxpool.Config {
	const defaultMaxConns = int32(4)
	const defaultMinConns = int32(0)
	const defaultMaxConnLifetime = time.Hour
	const defaultMaxConnIdleTime = time.Minute * 30
	const defaultHealthCheckPeriod = time.Minute
	const defaultConnectTimeout = time.Second * 5
	//TODO: add the fucking .env package to fill this,cant believe this isn't vanilla
	const DATABASE_URL string = "postgres://somepostgresurl :)"

	dbConfig, err := pgxpool.ParseConfig(DATABASE_URL)
	if err != nil {
		log.Fatal("Failed to create a config, error: ", err)
	}

	dbConfig.MaxConns = defaultMaxConns
	dbConfig.MinConns = defaultMinConns
	dbConfig.MaxConnLifetime = defaultMaxConnLifetime
	dbConfig.MaxConnIdleTime = defaultMaxConnIdleTime
	dbConfig.HealthCheckPeriod = defaultHealthCheckPeriod
	dbConfig.ConnConfig.ConnectTimeout = defaultConnectTimeout
	return dbConfig
}

func main() {
	fmt.Print("Initializing Redis Client...")
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	err := rdb.Ping(context.Background()).Err()
	if err != nil {
		log.Fatalf("could not connect to Redis:%v", err)
	}
	fmt.Println("Success")
	fmt.Print("Initializing pgsql Client...")
	connPool, err := pgxpool.NewWithConfig(context.Background(), pgxconfidk())
	if err != nil {
		log.Fatalf("Failed to create database conn pool:%v", err)
	}
	fmt.Println("Success")
	for {
		streams, err := rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    "report_workers",
			Consumer: "worker-1", //2nd param in stream is either unique ID selection or priority(newest/pending/old etc...)
			Streams:  []string{"reports_stream", ">"},
			Count:    50,
			Block:    5 * time.Second,
		}).Result()
		if err != nil && err != redis.Nil {
			log.Fatalf("error trying to XRead:%v", err)
		}
		if len(streams) == 0 {
			msgs, _, err := rdb.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{
				Stream:   "reports_stream",
				Group:    "report_workers",
				Consumer: "worker-1",

				// Only claim messages idle for at least this duration
				MinIdle: 30 * time.Second,

				// Start scanning from beginning of PEL
				Start: "0-0",

				Count: 50,
			}).Result()
			if err != nil {
				fmt.Printf("error reclaiming:%v", err)
				continue
			}
			for _, msg := range msgs {
				payload, err := getBytes(msg.Values["payload"])
				if err != nil {
					println("failed to decode payload:%v", err)
					continue
				}
				ctx := context.Background()
				conn, err := connPool.Acquire(ctx)
				if err != nil {
					fmt.Printf("failed to acquire conn:%v", err)
				}
				err = ParseCombatEvents(ctx, conn, payload)
				if err != nil {
					fmt.Printf("failed to parse combat event:%v", err)
					continue
				}
				rdb.XAck(ctx, "reports_stream", "report_workers", msg.ID)
			}
		}
		for _, s := range streams {
			for _, msg := range s.Messages {

				payload, err := getBytes(msg.Values["payload"])
				if err != nil {
					println("failed to decode payload:%v", err)
					continue
				}
				ctx := context.Background()
				conn, err := connPool.Acquire(ctx)
				if err != nil {
					fmt.Printf("failed to acquire conn:%v", err)
					continue
				}
				err = ParseCombatEvents(ctx, conn, payload)
				if err != nil {
					fmt.Printf("failed to parse combat event:%v", err)
					continue
				}
				rdb.XAck(ctx, "reports_stream", "report_workers", msg.ID)
			}
		}
	}
}

// for some reason redis does not seem to be consistent with the proto message return type??
// sometime it's string sometime it's []byte...
// couldn't find anything about it online so here I guess....
func getBytes(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case string:
		return []byte(val), nil
	case []byte:
		return val, nil
	default:
		return nil, fmt.Errorf("unexpected payload type %T", v)
	}
}
