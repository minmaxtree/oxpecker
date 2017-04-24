package oxpecker

import (
    "fmt"
    "log"
    "net"
    "encoding/binary"
    "os"
    "errors"
    "strings"
    "io"
)

var queues = map[string]*Queue {}

const (
    METHOD = 1
    HEADER = 2
    BODY = 3
    HEARTBEAT = 4
)

type Field struct {
    name string
    content interface{}
}

func marshalBool(value bool) []byte {
    ret := make([]byte, 1)
    if value == true {
        ret[0] = 0x01
    } else {
        ret[0] = 0x00
    }
    return ret
}

func unmarshalBool(buf []byte, offs int) (bool, int) {
    var val bool
    if buf[offs] == 0x00 {
        val = false
    } else {
        val = true
    }
    return val, offs + 1
}

func marshalShortStr(value string) []byte {
    ret := make([]byte, 1 + len(value))
    ret[0] = byte(len(value))
    copy(ret[1:], value)
    return ret
}

func unmarshalShortStr(buf []byte, offs int) (string, int) {
    ln := int(buf[offs])
    val := string(buf[offs+1:offs+1+ln])
    offs += 1 + ln
    return val, offs
}

func marshalLongStr(value string) []byte {
    ret := make([]byte, 4 + len(value))
    binary.BigEndian.PutUint32(ret[0:4], uint32(len(value)))
    copy(ret[4:], value)
    return ret
}

func unmarshalLongStr(buf []byte, offs int) (string, int) {
    ln := int(binary.BigEndian.Uint32(buf[offs:4+offs]))
    val := string(buf[offs+4:offs+4+ln])
    offs += 4 + ln
    return val, offs
}

func marshalUint8(value uint8) []byte {
    ret := make([]byte, 1)
    ret[0] = value
    return ret
}

func marshalUint16(value uint16) []byte {
    ret := make([]byte, 2)
    binary.BigEndian.PutUint16(ret, value)
    return ret
}

func marshalUint32(value uint32) []byte {
    ret := make([]byte, 4)
    binary.BigEndian.PutUint32(ret, value)
    return ret
}

func marshalUint64(value uint64) []byte {
    ret := make([]byte, 8)
    binary.BigEndian.PutUint64(ret, value)
    return ret
}

func marshalFieldTable(fieldTable []Field) []byte {
    content := ""
    for _, field := range fieldTable {
        content += string(marshalField(field))
    }
    return marshalLongStr(content)
}

func unmarshalFieldTable(buf []byte, offs int) ([]Field, int) {
    ret := []Field {}
    tableStr, retOffs := unmarshalLongStr(buf, offs)
    tableBuf := []byte(tableStr)
    tableBufOffs := 0
    var val Field
    for tableBufOffs < len(tableBuf) {
        val, tableBufOffs = unmarshalField(tableBuf, tableBufOffs)
        ret = append(ret, val)
    }
    return ret, retOffs
}

func marshalField(field Field) []byte {
    ret := []byte {}
    ret = append(ret, byte(len(field.name)))
    ret = append(ret, []byte(field.name)...)
    switch field.content.(type) {
    case string:
        ret = append(ret, 'S')
        ret = append(ret, marshalLongStr(field.content.(string))...)
    case bool:
        ret = append(ret, 't')
        ret = append(ret, marshalBool(field.content.(bool))...)
    case []Field:
        ret = append(ret, 'F')
        ret = append(ret, marshalFieldTable(field.content.([]Field))...)
    }
    return ret
}

func unmarshalField(buf []byte, offs int) (Field, int) {
    name, offs := unmarshalShortStr(buf, offs)
    var val interface{}
    switch (buf[offs]) {
    case 'S':
        val, offs = unmarshalLongStr(buf, offs + 1)
    case 't':
        val, offs = unmarshalBool(buf, offs + 1)
    case 'F':
        val, offs = unmarshalFieldTable(buf, offs + 1)
    }
    field := Field { name: name, content: val }
    return field, offs
}

func sendHeader(conn net.Conn) {
    protoHdr := "AMQP" + string(0x00) + string(0x00) + string(0x09) + string(0x01)
    conn.Write([]byte(protoHdr))
}

func WriteHeader(conn net.Conn, protocolIDMajor byte, protocolIDMinor byte,
    versionMajor byte, versionMinor byte) {
    buf := append([]byte("AMQP"), []byte {
        protocolIDMajor,
        protocolIDMinor,
        versionMajor,
        versionMinor,
    }...)
    conn.Write(buf)
}

type ProtocolHeader struct {
    protocolIDMajor byte
    protocolIDMinor byte
    VersionMajor byte
    VersionMinor byte
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

var eoframe = []byte { 0xce }

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

func marshal(value interface{}) []byte {
    switch value.(type) {
    case byte:
        return []byte { value.(byte) }
    case uint16:
        return marshalUint16(value.(uint16))
    case uint32:
        return marshalUint32(value.(uint32))
    case []byte:
        return marshalShortStr(string(value.([]byte)))
    case string:
        return marshalLongStr(value.(string))
    case []Field:
        return marshalFieldTable(value.([]Field))
    default:
        return nil
    }
}

// class ids
const (
    CONNECTION uint16 = 10
    CHANNEL uint16 = 20
    EXCHANGE uint16 = 40
    QUEUE uint16 = 50
    BASIC uint16 = 60
    TX uint16 = 90
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

// class channel methods
const (
    CHANNEL_OPEN uint16 = 10
    CHANNEL_OPEN_OK uint16 = 11
    CHANNEL_FLOW uint16 = 20
    CHANNEL_FLOW_OK uint16 = 21
    CHANNEL_CLOSE uint16 = 40
    CHANNEL_CLOSE_OK uint16 = 41
)

// class exchange methods
const (
    EXCHANGE_DECLARE uint16 = 10
    EXCHANGE_DECLARE_OK uint16 = 11
    EXCHANGE_DELETE uint16 = 20
    EXCHANGE_DELETE_OK uint16 = 21
)

// class queue methods
const (
    QUEUE_DECLARE uint16 = 10
    QUEUE_DECLARE_OK uint16 = 11
    QUEUE_BIND uint16 = 20
    QUEUE_BIND_OK uint16 = 21
    QUEUE_UNBIND uint16 = 50
    QUEUE_UNBIND_OK uint16 = 51
    QUEUE_PURGE uint16 = 30
    QUEUE_PURGE_OK uint16 = 31
    QUEUE_DELETE uint16 = 40
    QUEUE_DELETE_OK uint16 = 41
)

// class basic methods
const (
    BASIC_QOS uint16 = 10
    BASIC_QOS_OK uint16 = 11
    BASIC_CONSUME uint16 = 20
    BASIC_CONSUME_OK uint16 = 21
    BASIC_CANCEL uint16 = 30
    BASIC_CANCEL_OK uint16 = 31
    BASIC_PUBLISH uint16 = 40
    BASIC_RETURN uint16 = 50
    BASIC_DELIVER uint16 = 60
    BASIC_GET uint16 = 70
    BASIC_GET_OK uint16 = 71
    BASIC_GET_EMPTY uint16 = 72
    BASIC_ACK uint16 = 80
    BASIC_REJECT uint16 = 90
    BASIC_RECOVER_ASYNC uint16 = 100
    BASIC_RECOVER uint16 = 110
    BASIC_RECOVER_OK uint16 = 111
)

// class tx methods
const (
    TX_SELECT uint16 = 10
    TX_SELECT_OK uint16 = 11
    TX_COMMIT uint16 = 20
    TX_COMMIT_OK uint16 = 21
    TX_ROLLBACK uint16 = 30
    TX_ROLLBACK_OK uint16 = 31
)

func marshalM(values []interface{}) [] byte {
    buf := []byte {}
    for _, value := range values {
        buf = append(buf, marshal(value)...)
    }
    return buf
}

func SendConnectionStartOK(conn net.Conn, clientProperties []Field, mechanism string,
        response string, locale string) {
    params := []interface{} { CONNECTION, CONNECTION_START_OK, clientProperties, []byte(mechanism), response,
        []byte(locale) }
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

func SendConnectionTune(conn net.Conn, channelMax uint16, frameMax uint32, heartbeat uint16) {
    params := []interface{} { CONNECTION, CONNECTION_TUNE, channelMax, frameMax, heartbeat }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func ReceiveConnectionTune(conn net.Conn) {
    readFrame(conn)
}

func SendConnectionTuneOK(conn net.Conn, channelMax uint16, frameMax uint32, heartbeat uint16) {
    params := []interface{} { CONNECTION, CONNECTION_TUNE_OK, channelMax, frameMax, heartbeat }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func ReceiveConnectionTuneOK(conn net.Conn) {
    readFrame(conn)
}

func SendConnectionOpen(conn net.Conn, virtualHost string) {
    params := []interface{} { CONNECTION, CONNECTION_OPEN, []byte(virtualHost), RESERVED8, RESERVED8 }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
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

func SendQueueDeclare(conn net.Conn, queue string, passive byte, durable byte, exclusive byte,
        autoDelete byte, noWait byte, arguments []Field) {
    var flags byte = passive + (durable << 1) + (exclusive << 2) + (autoDelete << 3) + (noWait << 4)

    params := []interface{} { QUEUE, QUEUE_DECLARE, RESERVED16, []byte(queue), flags, arguments }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func ReceiveQueueDeclare(conn net.Conn) {
    readFrame(conn)
}

func SendQueueDeclareOK(conn net.Conn, queue string, messageCount uint32, consumerCount uint32) {
    params := []interface{} { QUEUE, QUEUE_DECLARE_OK, []byte(queue), messageCount, consumerCount }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func ReceiveQueueDeclareOK(conn net.Conn) {
    readFrame(conn)
}

func SendBasicPublish(conn net.Conn, exchange string, routingKey string, mandatory uint8, immediate uint8) {
    var flags byte = (mandatory << 0) + (immediate << 1)
    params := []interface{} { BASIC, BASIC_PUBLISH, RESERVED16, []byte(exchange), []byte(routingKey), flags }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func ReceiveBasicPublish(conn net.Conn) {
    readFrame(conn)
}

func SendContentHeader(conn net.Conn, bodySize uint64, propertyFlags uint16, properties BasicProperties) {
    propertiesBuf := marshalProperties(propertyFlags, properties)

    bodyBuf := marshalUint16(BASIC)
    bodyBuf = append(bodyBuf, marshalUint16(0)...)
    bodyBuf = append(bodyBuf, marshalUint64(bodySize)...)
    bodyBuf = append(bodyBuf, propertiesBuf...)

    header := Header { HEADER, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func SendContentBody(conn net.Conn, body string) {
    header := Header { BODY, 0, uint32(len(body)) }
    headerBuf := marshalHeader(header)
    frame := addBufs(headerBuf, []byte(body), eoframe)
    conn.Write(frame)
}

func SendBasicPublishData(conn net.Conn, data string) {
}

func marshalFrame(typ byte, channel uint16, params []interface{}) []byte {
    bodyBuf := marshalM(params)
    header := Header { typ, channel, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)
    frame := addBufs(headerBuf, bodyBuf, eoframe)
    return frame
}

func marshalMethodFrame(channel uint16, params []interface{}) []byte {
    return marshalFrame(METHOD, channel, params)
}

func SendBasicConsume(conn net.Conn, queue string, consumerTag string, noLocal byte, noAck byte,
        exclusive byte, noWait byte, arguments []Field) {
    params := []interface{} { BASIC, BASIC_CONSUME, RESERVED16, []byte(queue), []byte(consumerTag),
        packingBits(noLocal, noAck, exclusive, noWait), arguments }
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
}

func packingBits(bits ...byte) byte {
    var ret byte = 0
    for i, bit := range bits {
        ret += (bit << uint(i))
    }
    return ret
}

func SendBasicDeliver(conn net.Conn, consumerTag string, deliveryTag string,
        redelivered byte, exchange string, routingKey string) {
    params := []interface{} { BASIC, BASIC_DELIVER, []byte(consumerTag), []byte(deliveryTag),
        packingBits(redelivered), []byte(exchange), []byte(routingKey)}
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
}

func readFrame(conn net.Conn) {
    headerBuf := make([]byte, 7)
    _, err := conn.Read(headerBuf)
    check(err)
    header := unmarshalHeader(headerBuf)

    bodyBuf := make([]byte, header.length + 1)  // end of frame 0xce
    _, err = conn.Read(bodyBuf)
    check(err)
}

func readHeader(conn net.Conn) (Header, error) {
    headerBuf := make([]byte, 7)
    _, err := conn.Read(headerBuf)
    if err == io.EOF {
        return Header {}, err
    }
    check(err)
    header := unmarshalHeader(headerBuf)
    return header, nil
}

type ConnectionStart struct {
    versionMajor byte
    versionMinor byte
    serverProperties []Field
    mechanisms []string  // longStr, separeted by spaces
    locales []string  // longStr, separated by spaces
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

type ConnectionStartOK struct {
    clientProperties []Field
    mechanism string  // shortStr
    response string  // longStr
    locale string  // shortStr
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

type ConnectionOpen struct {
    virtualHost string  // path aka ShortStr
}

func unmarshalConnectionOpen(buf []byte) ConnectionOpen {
    connectionOpen := ConnectionOpen {}
    offs := 0
    connectionOpen.virtualHost, _ = unmarshalShortStr(buf, offs)

    return connectionOpen
}

type QueueDeclare struct {
    // RESERVED16
    queue string  // queue-name aka ShortStr
    passive byte  // bit
    durable byte  // bit
    exclusive byte  // bit
    autoDelete byte  // bit
    noWait byte  // no-wait aka bit
    arguments []Field  // table aka FieldTable
}

func unmarshalUint8(buf []byte, offs int) (byte, int) {
    return buf[offs], offs + 1
}

func unmarshalUint16(buf []byte, offs int) (uint16, int) {
    return binary.BigEndian.Uint16(buf[offs:offs+2]), offs + 2
}

func unmarshalUint32(buf []byte, offs int) (uint32, int) {
    return binary.BigEndian.Uint32(buf[offs:offs+4]), offs + 4
}

func unmarshalUint64(buf []byte, offs int) (uint64, int) {
    return binary.BigEndian.Uint64(buf[offs:offs+8]), offs + 8
}

func unmarshalQueueDeclare(buf []byte) QueueDeclare {
    queueDeclare := QueueDeclare {}
    offs := 0
    offs += 2  // skip RESERVED16
    queueDeclare.queue, offs = unmarshalShortStr(buf, offs)
    flags, offs := unmarshalUint8(buf, offs)
    queueDeclare.passive = (flags >> 0) & 1
    queueDeclare.durable = (flags >> 1) & 1
    queueDeclare.exclusive = (flags >> 2) & 1
    queueDeclare.autoDelete = (flags >> 3) & 1
    queueDeclare.noWait = flags & (flags >> 4) & 1
    queueDeclare.arguments, offs = unmarshalFieldTable(buf, offs)

    return queueDeclare
}

type BasicPublish struct {
    // RESERVED16
    exchange string  // exchange-name aka ShortStr
    routingKey string  // ShortStr
    mandatory byte  // bit
    immediate byte  // bit
}

func unmarshalBasicPublish(buf []byte) BasicPublish {
    basicPublish := BasicPublish {}
    offs := 0
    offs += 2  // skip RESERVED16
    basicPublish.exchange, offs = unmarshalShortStr(buf, offs)
    basicPublish.routingKey, offs = unmarshalShortStr(buf, offs)
    flags, offs := unmarshalUint8(buf, offs)
    basicPublish.mandatory = (flags >> 0) & 1
    basicPublish.immediate = (flags >> 1) & 1

    return basicPublish
}

type BasicConsume struct {
    // RESERVED
    queue string  // queue-name aka ShortSTr
    consumerTag string  // consumer-tag aka ShortStr
    noLocal byte  // no-local aka bit
    noAck byte  // no-ack aka bit
    exclusive byte  // bit
    noWait byte  // no-wait aka bit
    arguments []Field
}

func unmarshalBasicConsume(buf []byte) BasicConsume {
    basicConsume := BasicConsume {}
    offs := 0
    offs += 2  // skip RESERVED16
    basicConsume.queue, offs = unmarshalShortStr(buf, offs)
    basicConsume.consumerTag, offs = unmarshalShortStr(buf, offs)
    flags, offs := unmarshalUint8(buf, offs)
    basicConsume.noLocal = (flags >> 0) & 1
    basicConsume.noAck = (flags >> 1) & 1
    basicConsume.exclusive = (flags >> 2) & 1
    basicConsume.noWait = (flags >> 3) & 1
    basicConsume.arguments, offs = unmarshalFieldTable(buf, offs)

    return basicConsume
}

type BasicDeliver struct {
    consumerTag string  // ShortStr
    deliveryTag string  // ShortStr
    redelivered byte  // redelivered aka bit
    exchange string  // ShortStr
    routingKey string  // ShortStr
}

func unmarshalBasicDeliver(buf []byte) BasicDeliver {
    basicDeliver := BasicDeliver {}
    offs := 0
    basicDeliver.consumerTag, offs = unmarshalShortStr(buf, offs)
    basicDeliver.deliveryTag, offs = unmarshalShortStr(buf, offs)
    flags, offs := unmarshalUint8(buf, offs)
    basicDeliver.redelivered = flags & 1
    basicDeliver.exchange, offs = unmarshalShortStr(buf, offs)
    basicDeliver.routingKey, offs = unmarshalShortStr(buf, offs)

    return basicDeliver
}

func ServerReceiveMessage(conn net.Conn) (byte, uint16, interface{}, error) {
    header, err := readHeader(conn)
    if err == io.EOF {
        return 0, 0, nil, err
    }
    check(err)
    bodyBuf := make([]byte, header.length + 1)
    _, err = conn.Read(bodyBuf)
    check(err)
    var payload interface{}

    if header.typ == METHOD {
        class := binary.BigEndian.Uint16(bodyBuf)
        method := binary.BigEndian.Uint16(bodyBuf[2:])

        switch class {
        case CONNECTION:
            switch method {
            case CONNECTION_START_OK:
                connectionStartOK := unmarshalConnectionStartOK(bodyBuf[4:])
                fmt.Println("connectionStartOK is:", connectionStartOK)
                payload = connectionStartOK
                SendConnectionTune(conn, 0xffff, 0xffffffff, 60)
            case CONNECTION_OPEN:
                connectionOpen := unmarshalConnectionOpen(bodyBuf[4:])
                fmt.Println("connectionOpen is:", connectionOpen)
                SendConnectionOpenOK(conn)
            }
        case QUEUE:
            switch method {
            case QUEUE_DECLARE:
                queueDeclare := unmarshalQueueDeclare(bodyBuf[4:])
                fmt.Println("queueDeclare is:", queueDeclare)

                if queues["clojure"] == nil {
                    newQueue := new(Queue)
                    newQueue.Messages = [][]byte {}
                    newQueue.Consumers = []net.Conn {}
                    queues["clojure"] = newQueue
                }

                SendQueueDeclareOK(conn, "clojure", 0, 0)
            }
        case BASIC:
            switch method {
            case BASIC_PUBLISH:
                basicPublish := unmarshalBasicPublish(bodyBuf[4:])
                fmt.Println("basicPublish is:", basicPublish)
                handleBasicPublish(conn, basicPublish)
            case BASIC_CONSUME:
                basicConsume := unmarshalBasicConsume(bodyBuf[4:])
                fmt.Println("basicConsume is:", basicConsume)
                go handleBasicConsume(conn, basicConsume)
            }
        }
    } else if header.typ == HEADER {
        // class := binary.BigEndian.Uint16(bodyBuf)
        // skip unused 2 bytes
        bodySize := binary.BigEndian.Uint64(bodyBuf[4:])
        propertyFlags := binary.BigEndian.Uint16(bodyBuf[12:])
        properties := parseProperties(propertyFlags, bodyBuf[14:])
        contentHeader := ContentHeader {
            bodySize: bodySize,
            propertyFlags: propertyFlags,
            properties: properties,
        }
        return header.typ, header.channel, contentHeader, nil
    } else if header.typ == BODY {
        payload := bodyBuf[:len(bodyBuf)-1]
        return header.typ, header.channel, payload, nil
    } else if header.typ == HEARTBEAT {}

    return header.typ, header.channel, payload, nil
}

type ContentHeader struct {
    bodySize uint64
    propertyFlags uint16
    properties BasicProperties
}

func handleBasicConsume(conn net.Conn, basicConsume BasicConsume) {
    queue := queues[basicConsume.queue]
    fmt.Println("[handleBasicConsume] queue is:", queue)
    fmt.Println("[handleBasicConsume] queue name is:", basicConsume.queue)
    queue.Consumers = append(queue.Consumers, conn)
    fmt.Println("[handleBasicConsume] queue.Consumers is:", queue.Consumers)

    if len(queue.Messages) > 0 {
        for len(queue.Messages) > 0 {
            message := queue.Messages[0]

            SendBasicDeliver(queue.Consumers[0], "consumer_tag", "delivery_tag", 0, "", basicConsume.queue)
            var propertyFlags uint16 = 0x9000
            properties := BasicProperties {
                ContentType: "text/plain",
                DeliveryMode: 2,
            }
            SendContentHeader(queue.Consumers[0], uint64(len(message)), propertyFlags, properties)
            SendContentBody(queue.Consumers[0], string(message))

            queue.Messages = queue.Messages[1:]
        }
    }
    fmt.Println("[2] queue.Consumers is:", queue.Consumers)
}

func handleBasicPublish(conn net.Conn, basicPublish BasicPublish) {
    queue := queues[basicPublish.routingKey]
    fmt.Println("[0] queue is:", queue)

    typ, _, payload, _ := ServerReceiveMessage(conn)

    if typ == HEADER {
        switch payload.(type) {
        case ContentHeader:
            // process content header info
        default:
            // error 505
        }
    } else {
        // error 505
    }

    // receive content body
    body := []byte {}
    for {
        typ, _, payload, err := ServerReceiveMessage(conn)
        if err == io.EOF {
            break
        }
        check(err)
        if typ == BODY {
            body = append(body, payload.([]byte)...)
        } else {
            break
        }
    }
    queue.Messages = append(queue.Messages, body)
    fmt.Println("queue.Messages is:", queue.Messages)
    fmt.Println("len(queue.Messages) is:", len(queue.Messages))
    fmt.Println("len(queue.Consumers) is:", len(queue.Consumers))
    fmt.Println("queue.Consumers is:", queue.Consumers)
    fmt.Println("routingKey is:", basicPublish.routingKey)
    if len(queue.Consumers) > 0 {
        for len(queue.Messages) > 0 {
            message := queue.Messages[0]

            SendBasicDeliver(queue.Consumers[0], "consumer_tag", "delivery_tag", 0, "", basicPublish.routingKey)
            var propertyFlags uint16 = 0x9000
            properties := BasicProperties {
                ContentType: "text/plain",
                DeliveryMode: 2,
            }
            SendContentHeader(queue.Consumers[0], uint64(len(message)), propertyFlags, properties)
            SendContentBody(queue.Consumers[0], string(message))

            queue.Messages = queue.Messages[1:]
        }
    }
}

type BasicProperties struct {
    ContentType string  // shortStr, 15
    ContentEncoding string  // shortStr, 14
    Headers []Field  // 13
    DeliveryMode  byte  // 12
    Priority byte  // 11
    CorrelationId string  // shortStr, 10
    ReplyTo string  // shortStr, 9
    Expiration string  // shortStr, 8
    Timestamp string  // shortStr, 7
    Typ string  // shortStr, 6
    UserId string  // shortStr, 5
    AppId string  // shortStr, 4
    Reserved string  // shortStr, 3
}

func parseProperties(propertyFlags uint16, propertiesBuf []byte) BasicProperties {
    basicProperties := BasicProperties {}
    offs := 0
    if ((propertyFlags >> 15) & 1) == 1 {
        basicProperties.ContentType, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    if ((propertyFlags >> 14) & 1) == 1 {
        basicProperties.ContentEncoding, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    if ((propertyFlags >> 13) & 1) == 1 {
        basicProperties.Headers, offs = unmarshalFieldTable(propertiesBuf, offs)
    }
    if ((propertyFlags >> 12) & 1) == 1 {
        basicProperties.DeliveryMode, offs = unmarshalUint8(propertiesBuf, offs)
    }
    if ((propertyFlags >> 11) & 1) == 1 {
        basicProperties.Priority, offs = unmarshalUint8(propertiesBuf, offs)
    }
    if ((propertyFlags >> 10) & 1) == 1 {
        basicProperties.CorrelationId, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    if ((propertyFlags >> 9) & 1) == 1 {
        basicProperties.ReplyTo, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    if ((propertyFlags >> 8) & 1) == 1 {
        basicProperties.Expiration, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    if ((propertyFlags >> 7) & 1) == 1 {
        basicProperties.Timestamp, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    if ((propertyFlags >> 6) & 1) == 1 {
        basicProperties.Typ, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    if ((propertyFlags >> 5) & 1) == 1 {
        basicProperties.UserId, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    if ((propertyFlags >> 4) & 1) == 1 {
        basicProperties.AppId, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    if ((propertyFlags >> 3) & 1) == 1 {
        basicProperties.Reserved, offs = unmarshalShortStr(propertiesBuf, offs)
    }
    return basicProperties
}

func marshalProperties(propertyFlags uint16, basicProperties BasicProperties) []byte {
    buf := marshalUint16(propertyFlags)
    if ((propertyFlags >> 15) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.ContentType)...)
    }
    if ((propertyFlags >> 14) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.ContentEncoding)...)
    }
    if ((propertyFlags >> 13) & 1) == 1 {
        buf = append(buf, marshalFieldTable(basicProperties.Headers)...)
    }
    if ((propertyFlags >> 12) & 1) == 1 {
        buf = append(buf, marshalUint8(basicProperties.DeliveryMode)...)
    }
    if ((propertyFlags >> 11) & 1) == 1 {
        buf = append(buf, marshalUint8(basicProperties.Priority)...)
    }
    if ((propertyFlags >> 10) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.CorrelationId)...)
    }
    if ((propertyFlags >> 9) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.ReplyTo)...)
    }
    if ((propertyFlags >> 8) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.Expiration)...)
    }
    if ((propertyFlags >> 7) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.Timestamp)...)
    }
    if ((propertyFlags >> 6) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.Typ)...)
    }
    if ((propertyFlags >> 5) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.UserId)...)
    }
    if ((propertyFlags >> 4) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.AppId)...)
    }
    if ((propertyFlags >> 3) & 1) == 1 {
        buf = append(buf, marshalShortStr(basicProperties.Reserved)...)
    }

    return buf

}

func ClientReceiveMessage(conn net.Conn) (byte, uint16, interface{}, error) {
    header, err := readHeader(conn)
    if err == io.EOF {
        return 0, 0, nil, err
    }
    check(err)
    bodyBuf := make([]byte, header.length + 1)
    _, err = conn.Read(bodyBuf)
    check(err)

    if header.typ == METHOD {
        class := binary.BigEndian.Uint16(bodyBuf)
        method := binary.BigEndian.Uint16(bodyBuf[2:])

        switch class {
        case CONNECTION:
            switch method {
            case CONNECTION_START:
                connectionStart := unmarshalConnectionStart(bodyBuf[4:])
                fmt.Println("connectionStart is:", connectionStart)
            }
        case BASIC:
            switch method {
            case BASIC_DELIVER:
                basicDeliver := unmarshalBasicDeliver(bodyBuf[4:])
                fmt.Println("basicDeliver is:", basicDeliver)
                handleBasicDeliver(conn, basicDeliver)
            }
        }
    } else if header.typ == HEADER {
        // class := binary.BigEndian.Uint16(bodyBuf)
        // skip unused 2 bytes
        bodySize := binary.BigEndian.Uint64(bodyBuf[4:])
        propertyFlags := binary.BigEndian.Uint16(bodyBuf[12:])
        properties := parseProperties(propertyFlags, bodyBuf[14:])
        contentHeader := ContentHeader {
            bodySize: bodySize,
            propertyFlags: propertyFlags,
            properties: properties,
        }
        return header.typ, header.channel, contentHeader, nil
    } else if header.typ == BODY {
        payload := bodyBuf[:len(bodyBuf)-1]
        return header.typ, header.channel, payload, nil
    } else if header.typ == HEARTBEAT {}

    return 0, 0, nil, nil
}

func handleBasicDeliver(conn net.Conn, basicDeliver BasicDeliver) {
    typ, _, payload, _ := ClientReceiveMessage(conn)

    var bodySize uint64
    if typ == HEADER {
        switch payload.(type) {
        case ContentHeader:
            // process content header info
            contentHeader := payload.(ContentHeader)
            bodySize = contentHeader.bodySize
        default:
            // error 505
        }
    } else {
        // error 505
    }

    // receive content body
    body := []byte {}
    for {
        typ, _, payload, err := ClientReceiveMessage(conn)
        fmt.Println("typ is:", typ)
        fmt.Println("err is:", err)
        if err == io.EOF {
            break
        }
        check(err)
        if typ == BODY {
            body = append(body, payload.([]byte)...)
            fmt.Println("len(body) is:", len(body))
            fmt.Println("bodySize is:", bodySize)
            if uint64(len(body)) == bodySize {
                break
            }
        } else {
            break
        }
    }

    fmt.Println("[handleBasicDeliver] received:", string(body))
}

const RESERVED8 byte = 0x00
const RESERVED16 uint16 = 0x0000
const RESERVED32 uint32 = 0x00000000

type Header struct {
    typ byte
    channel uint16
    length uint32
}

func addBufs(bufs ...[]byte) []byte {
    ret := []byte {}
    for _, buf := range bufs {
        ret = append(ret, buf...)
    }
    return ret
}

func unmarshalHeader(hdrBuf []byte) Header {
    header := Header {}
    header.typ = hdrBuf[0]
    header.channel = binary.BigEndian.Uint16(hdrBuf[1:3])
    header.length = binary.BigEndian.Uint32(hdrBuf[3:7])
    return header
}

func marshalHeader(header Header) []byte {
    ret := make([]byte, 7)
    ret[0] = header.typ
    binary.BigEndian.PutUint16(ret[1:3], header.channel)
    binary.BigEndian.PutUint32(ret[3:7], header.length)
    return ret
}

func check(err error) {
    if err != nil {
        log.Println(err)
        os.Exit(-1)
    }
}

type Queue struct {
    Name string
    // MessageCount int
    // ConsumerCount int
    Messages [][]byte
    Consumers []net.Conn
}
