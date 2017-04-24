package main

import (
    "net"
    "os"

    "oxpecker"
)

var address = ":22222"

func main() {
    if len(os.Args) > 1 {
        address = ":" + os.Args[1]
    }

    conn, err := net.Dial("tcp", address)
    check(err)
    oxpecker.WriteHeader(conn, 0, 0, 9, 1)
    oxpecker.ReceiveConnectionStart(conn)
    oxpecker.SendConnectionStartOK(conn, []oxpecker.Field {}, "", "", "en_US")
    oxpecker.ReceiveConnectionTune(conn)
    oxpecker.SendConnectionTuneOK(conn, 0xffff, 0xffffffff, 60)

    oxpecker.SendConnectionOpen(conn, "/")
    oxpecker.ReceiveConnectionOpenOK(conn)

    oxpecker.SendQueueDeclare(conn, "elixir", 0, 0, 0, 0, 0, []oxpecker.Field {})
    oxpecker.ReceiveQueueDeclareOK(conn)

    defaultExchange := ""
    oxpecker.SendBasicPublish(conn, defaultExchange, "clojure", 0, 0)

    oxpecker.SendBasicConsume(conn, "clojure", "consumer0", 0, 0, 0, 0, []oxpecker.Field {})

    for {
        oxpecker.ClientReceiveMessage(conn)
    }
}

func check(err error) {
    if err != nil {
        panic(err)
    }
}
