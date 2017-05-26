package main

import (
    "net"
    "os"

    "oxpecker"

    "os/signal"
    "syscall"
    "fmt"
)

var address = ":22222"

func main() {
    if len(os.Args) > 1 {
        address = ":" + os.Args[1]
    }

    conn, err := net.Dial("tcp", address)
    check(err)
    oxpecker.SendProtocolHeader(conn, 0, 0, 9, 1)
    oxpecker.ReceiveConnectionStart(conn)
    oxpecker.SendConnectionStartOK(conn, []oxpecker.Field {}, "", "", "en_US")
    oxpecker.ReceiveConnectionTune(conn)
    oxpecker.SendConnectionTuneOK(conn, 0xffff, 0xffffffff, 60)

    oxpecker.SendConnectionOpen(conn, "/")
    oxpecker.ReceiveConnectionOpenOK(conn)

    oxpecker.SendQueueDeclare(conn, "elixir", 0, 0, 0, 0, 0, []oxpecker.Field {})
    oxpecker.ReceiveQueueDeclareOK(conn)

    oxpecker.SendExchangeDeclare(
        conn,
        "my_fanout_exchange",  // exchange
        "x-fanout",  // type
        0,  // passive
        0,  // durable
        1,  // noWait
        []oxpecker.Field{},  // arguments
    )
    oxpecker.ReceiveExchangeDeclareOK(conn)

    oxpecker.SendQueueBind(
        conn,
        "elixir",  // queue
        "my_fanout_exchange",  // exchange
        "my_routing_key",  // routingKey
        0,  // noWait
        []oxpecker.Field{},  // arguments
    )
    oxpecker.ReceiveQueueBindOK(conn)

    // defaultExchange := ""
    // oxpecker.SendBasicPublish(conn, defaultExchange, "elixir", 0, 0)

    oxpecker.SendBasicConsume(conn, "elixir", "consumer0", 0, 0, 0, 0, []oxpecker.Field {})

    sigs := make(chan os.Signal, 1)
    done := make(chan bool, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        <- sigs
        fmt.Println("signal!")
        oxpecker.SendConnectionClose(conn, 0, "close", 0, 0)
        done <- true
    } ()

    for {
        fmt.Println("1")
        select {
        case <- done:
            fmt.Println("exiting...")
            return
        default:
            oxpecker.ClientReceiveMessage(conn)
        }
    }
}

func check(err error) {
    if err != nil {
        panic(err)
    }
}
