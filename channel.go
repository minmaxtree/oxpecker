package oxpecker

import (
    "net"
)

// class channel methods
const (
    CHANNEL_OPEN uint16 = 10
    CHANNEL_OPEN_OK uint16 = 11
    CHANNEL_FLOW uint16 = 20
    CHANNEL_FLOW_OK uint16 = 21
    CHANNEL_CLOSE uint16 = 40
    CHANNEL_CLOSE_OK uint16 = 41
)

type ChannelOpen struct {
    // reserved-1
}

type ChannelOpenOK struct {
    // reserved-1
}

type ChannelFlow struct {
    active byte  // bit
}

type ChannelFlowOK struct {
    active byte  // bit
}

type ChannelClose struct {
    replyCode uint16
    replyText string  // ShortStr
    classId uint16
    methodId uint16
}

func SendChannelOpen(conn net.Conn) {}

func SendChannelOpenOK(conn net.Conn) {}

func SendChannelFlow(conn net.Conn, active byte) {
    params := []interface {} {
        CHANNEL,
        CHANNEL_FLOW,
        active,
    }
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
}

func SendChannelClose(conn net.Conn,
                      replyCode uint16,
                      replyText string,
                      classId uint16,
                      methodId uint16) {
    params := []interface {} {
        CHANNEL,
        CHANNEL_CLOSE,
        replyCode,
        []byte(replyText),
        classId,
        methodId,
    }
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
}
