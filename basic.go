package oxpecker

import (
    "net"
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

type BasicPublish struct {
    // RESERVED16
    exchange string  // exchange-name aka ShortStr
    routingKey string  // ShortStr
    mandatory byte  // bit
    immediate byte  // bit
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

type BasicConsumeOK struct {
    consumerTag string  // ShortStr
}

type BasicDeliver struct {
    consumerTag string  // ShortStr
    deliveryTag string  // ShortStr
    redelivered byte  // redelivered aka bit
    exchange string  // ShortStr
    routingKey string  // ShortStr
}

type BasicGet struct {
    // reserved-1
    queue string  // ShortStr
    noAck byte  // bit
}

func SendBasicGet(conn net.Conn, queue string, noAck byte) {
    params := []interface{} {
        BASIC,
        BASIC_GET,
        []byte(queue),
        noAck,
    }
    sendMethodParams(conn, params)
}

type BasicGetOK struct {
    deliveryTag string  // ShortStr
    redelivered byte
    exchange string  // ShortStr
    routingKey string  // ShortStr
    messageCount uint32
}

func SendBasicGetOK(conn net.Conn,
                    deliveryTag string,
                    redelivered byte,
                    exchange string,
                    routingKey string,
                    messageCount uint32) {
    params := []interface{} {
        BASIC,
        BASIC_GET,
        []byte(deliveryTag),
        redelivered,
        []byte(exchange),
        []byte(routingKey),
        messageCount,
    }
    sendMethodParams(conn, params)
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

type ContentHeader struct {
    bodySize uint64
    propertyFlags uint16
    properties BasicProperties
}

func SendBasicPublish(conn net.Conn, exchange string, routingKey string,
        mandatory uint8, immediate uint8) {
    var flags byte = (mandatory << 0) + (immediate << 1)
    params := []interface{} { BASIC, BASIC_PUBLISH, RESERVED16,
        []byte(exchange), []byte(routingKey), flags }
    bodyBuf := marshalM(params)

    header := Header { METHOD, 0, uint32(len(bodyBuf)) }
    headerBuf := marshalHeader(header)

    frame := addBufs(headerBuf, bodyBuf, eoframe)
    conn.Write(frame)
}

func SendBasicConsume(conn net.Conn, queue string, consumerTag string,
        noLocal byte, noAck byte,
        exclusive byte, noWait byte, arguments []Field) {
    params := []interface{} { BASIC, BASIC_CONSUME, RESERVED16,
        []byte(queue), []byte(consumerTag),
        packingBits(noLocal, noAck, exclusive, noWait), arguments }
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
}

func SendBasicConsumeOK(conn net.Conn, consumerTag string) {
    params := []interface{} { BASIC, BASIC_CONSUME_OK, []byte(consumerTag) }
    sendMethodParams(conn, params)
}

func SendBasicDeliver(conn net.Conn, consumerTag string, deliveryTag string,
        redelivered byte, exchange string, routingKey string) {
    params := []interface{} { BASIC, BASIC_DELIVER,
        []byte(consumerTag), []byte(deliveryTag),
        packingBits(redelivered), []byte(exchange), []byte(routingKey)}
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
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
