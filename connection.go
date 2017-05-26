package oxpecker

import (
    "net"
    "strings"
)

// class connection methods
const (
    CONNECTION_START uint16 = 10
    CONNECTION_START_OK uint16 = 11
    CONNECTION_SECURE uint16 = 20
    CONNECTION_SECURE_OK uint16 = 21
    CONNECTION_TUNE uint16 = 30
    CONNECTION_TUNE_OK uint16 = 31
    CONNECTION_OPEN uint16 = 40
    CONNECTION_OPEN_OK uint16 = 41
    CONNECTION_CLOSE uint16 = 50
    CONNECTION_CLOSE_OK uint16 = 51
)

type ConnectionStart struct {
    versionMajor byte
    versionMinor byte
    serverProperties []Field
    mechanisms []string  // longStr, separeted by spaces
    locales []string  // longStr, separated by spaces
}

type ConnectionStartOK struct {
    clientProperties []Field
    mechanism string  // shortStr
    response string  // longStr
    locale string  // shortStr
}

type ConnectionOpen struct {
    virtualHost string  // path aka ShortStr
}

type ConnectionTune struct {
    channelMax uint16
    frameMax  uint32
    heartbeat uint16
}

type ConnectionTuneOK struct {
    channelMax uint16
    frameMax uint32
    heartbeat uint16
}

type ConnectionClose struct {
    replyCode uint16
    replyText string  // ShortStr
    classId uint16
    methodId uint16
}

func SendConnectionStart(conn net.Conn, versionMajor byte, versionMinor byte,
        serverProperties []Field, mechanisms string, locales string) {
    classBuf := marshalUint16(10)  // connection
    methodBuf := marshalUint16(10)  // start
    versionBuf := []byte { versionMajor, versionMinor }
    serverPropertiesBuf := marshalFieldTable(serverProperties)
    mechanismsBuf := marshalLongStr(mechanisms)
    localesBuf := marshalLongStr(locales)
    bodyBuf := addBufs( classBuf, methodBuf, versionBuf, serverPropertiesBuf,
        mechanismsBuf, localesBuf)

    header := Header {
        typ: METHOD,
        channel: 0,
        length: uint32(len(bodyBuf)),
    }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func ReceiveConnectionStart(conn net.Conn) {
    readFrame(conn)
}

func SendConnectionStartOK(conn net.Conn, clientProperties []Field, mechanism string,
        response string, locale string) {
    params := []interface{} { CONNECTION, CONNECTION_START_OK, clientProperties,
        []byte(mechanism), response, []byte(locale) }
    bodyBuf := marshalM(params)

    header := Header {
        typ: METHOD,
        channel: 0,
        length: uint32(len(bodyBuf)),
    }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func ReceiveConnectionStartOK(conn net.Conn) {
    readFrame(conn)
}

func SendConnectionTune(conn net.Conn, channelMax uint16,
        frameMax uint32, heartbeat uint16) {
    params := []interface{} { CONNECTION, CONNECTION_TUNE,
        channelMax, frameMax, heartbeat }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func ReceiveConnectionTune(conn net.Conn) {
    readFrame(conn)
}

func SendConnectionTuneOK(conn net.Conn, channelMax uint16,
        frameMax uint32, heartbeat uint16) {
    params := []interface{} { CONNECTION, CONNECTION_TUNE_OK,
        channelMax, frameMax, heartbeat }
    sendMethodParams(conn, params)
}

func ReceiveConnectionTuneOK(conn net.Conn) {
    readFrame(conn)
}

func SendConnectionOpen(conn net.Conn, virtualHost string) {
    params := []interface{} { CONNECTION, CONNECTION_OPEN,
            []byte(virtualHost), RESERVED8, RESERVED8 }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func SendConnectionClose(conn net.Conn,
                         replyCode uint16,
                         replyText string,
                         classId uint16,
                         methodId uint16) {
    params := []interface {} {
        CONNECTION,
        CONNECTION_CLOSE,
        replyCode,
        []byte(replyText),
        classId,
        methodId,
    }
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
}

func SendConnectionCloseOK(conn net.Conn) {
    params := []interface {} {
        CONNECTION,
        CONNECTION_CLOSE_OK,
    }
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
}

func ReceiveConnectionOpen(conn net.Conn) {
    readFrame(conn)
}

func SendConnectionOpenOK(conn net.Conn) {
    params := []interface{} { CONNECTION, CONNECTION_OPEN_OK, RESERVED8 }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func ReceiveConnectionOpenOK(conn net.Conn) {
    readFrame(conn)
}

func unmarshalConnectionStart(buf []byte) ConnectionStart {
    connectionStart := ConnectionStart {}
    connectionStart.versionMajor = buf[0]
    connectionStart.versionMinor = buf[1]
    offs := 2
    connectionStart.serverProperties, offs = unmarshalFieldTable(buf, offs)
    mechanismsStr, offs := unmarshalLongStr(buf, offs)
    connectionStart.mechanisms = strings.Fields(mechanismsStr)
    localesStr, offs := unmarshalLongStr(buf, offs)
    connectionStart.locales = strings.Fields(localesStr)

    return connectionStart
}

func unmarshalConnectionStartOK(buf []byte) ConnectionStartOK {
    connectionStartOK := ConnectionStartOK {}
    offs := 0
    connectionStartOK.clientProperties, offs = unmarshalFieldTable(buf, offs)
    connectionStartOK.mechanism, offs = unmarshalShortStr(buf, offs)
    connectionStartOK.response, offs = unmarshalLongStr(buf, offs);
    connectionStartOK.locale, offs = unmarshalShortStr(buf, offs);

    return connectionStartOK
}

func unmarshalConnectionOpen(buf []byte) ConnectionOpen {
    connectionOpen := ConnectionOpen {}
    offs := 0
    connectionOpen.virtualHost, _ = unmarshalShortStr(buf, offs)

    return connectionOpen
}

func unmarshalConnectionClose(buf []byte) ConnectionClose {
    connectionClose := ConnectionClose {}
    offs := 0
    connectionClose.replyCode, offs = unmarshalUint16(buf, offs)
    connectionClose.replyText ,offs = unmarshalShortStr(buf, offs)
    connectionClose.classId, offs = unmarshalUint16(buf, offs)
    connectionClose.methodId, offs = unmarshalUint16(buf, offs)

    return connectionClose
}
