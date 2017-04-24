package main

import (
    "net"
    // "fmt"
    // "errors"
    "os"

    "oxpecker"
)

func listen(conn net.Conn) {
    for {
        header, err := oxpecker.ReceiveProtocolHeader(conn)
        if err != nil {
            continue
        }
        oxpecker.SendConnectionStart(conn, header.VersionMajor, header.VersionMinor,
            []oxpecker.Field {}, "", "en_US")
        return
    }
}

var address = ":22222"

var queueMap = map[string]*oxpecker.Queue {}

func main() {
    if len(os.Args) > 1 {
        address = ":" + os.Args[1]
    }

    ln, err := net.Listen("tcp", address)
    check(err)
    for {
        conn, err := ln.Accept()
        check(err)
        defer conn.Close()

        go func() {
            listen(conn)
            for {
                oxpecker.ServerReceiveMessage(conn)
            }
        }()
    }
}

func check(err error) {
    if err != nil {
        panic(err)
    }
}
