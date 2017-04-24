package oxpecker

import (
    "net"
    "errors"
)

type ProtocolHeader struct {
    protocolIDMajor byte
    protocolIDMinor byte
    VersionMajor byte
    VersionMinor byte
}

func SendProtocolHeader(conn net.Conn, protocolIDMajor byte, protocolIDMinor byte,
    versionMajor byte, versionMinor byte) {
    buf := append([]byte("AMQP"), []byte {
        protocolIDMajor,
        protocolIDMinor,
        versionMajor,
        versionMinor,
    }...)
    conn.Write(buf)
}

func ReceiveProtocolHeader(conn net.Conn) (ProtocolHeader, error) {
    buf := make([]byte, 4)
    conn.Read(buf)
    if string(buf) == "AMQP" {
        conn.Read(buf)
        return ProtocolHeader { buf[0], buf[1], buf[2], buf[3] }, nil
    } else {
        return ProtocolHeader {}, errors.New("bad header")
    }
}
