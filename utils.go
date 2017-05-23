package oxpecker

import (
    "net"
)

func sendMethodParams(conn net.Conn, params []interface{}) {
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
}
