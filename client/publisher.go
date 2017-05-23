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
    oxpecker.SendProtocolHeader(conn, 0, 0, 9, 1)
    oxpecker.ReceiveConnectionStart(conn)
    oxpecker.SendConnectionStartOK(conn, []oxpecker.Field {}, "", "", "en_US")
    oxpecker.ReceiveConnectionTune(conn)
    oxpecker.SendConnectionTuneOK(conn, 0xffff, 0xffffffff, 60)

    oxpecker.SendConnectionOpen(conn, "/")
    oxpecker.ReceiveConnectionOpenOK(conn)

    oxpecker.SendQueueDeclare(conn, "elixir", 0, 0, 0, 0, 0, []oxpecker.Field {})
    oxpecker.ReceiveQueueDeclareOK(conn)

    defaultExchange := ""
    oxpecker.SendBasicPublish(conn, defaultExchange, "elixir", 0, 0)
    data := "hello from publisher!\n"
    // oxpecker.SendBasicPublishData(conn, data)
    var propertyFlags uint16 = 0x9000
    properties := oxpecker.BasicProperties {
        ContentType: "text/plain",
        DeliveryMode: 2,
    }
    oxpecker.SendContentHeader(conn, uint64(len(data)), propertyFlags, properties)
    oxpecker.SendContentBody(conn, data)
}

func check(err error) {
    if err != nil {
        panic(err)
    }
}
