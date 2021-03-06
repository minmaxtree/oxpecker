package oxpecker

import (
    "net"
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

type QueueBind struct {
    // reserved-1
    queue string  // ShortStr
    exchange string  // ShortStr
    routingKey string  // ShortStr
    noWait byte  // bit
    arguments []Field
}

func unmarshalQueueBind(buf []byte) QueueBind {
    queueBind := QueueBind {}
    offs := 0
    offs += 1 // reserved-1
    queueBind.queue, offs = unmarshalShortStr(buf, offs)
    queueBind.exchange, offs = unmarshalShortStr(buf, offs)
    queueBind.routingKey, offs = unmarshalShortStr(buf, offs)
    queueBind.noWait, offs = unmarshalUint8(buf, offs)
    queueBind.arguments, offs = unmarshalFieldTable(buf, offs)

    return queueBind
}

func SendQueueBind(conn net.Conn,
                   queue string,
                   exchange string,
                   routingKey string,
                   noWait byte,
                   arguments []Field) {
    params := [] interface{} {
        QUEUE,
        QUEUE_BIND,
        RESERVED8,
        []byte(queue),
        []byte(exchange),
        []byte(routingKey),
        noWait,
        arguments,
    }
    sendMethodParams(conn, params)
}

func SendQueueBindOK(conn net.Conn) {
    params := []interface{} {
        QUEUE,
        QUEUE_BIND_OK,
    }
    sendMethodParams(conn, params)
}

func ReceiveQueueBindOK(conn net.Conn) {
    readFrame(conn)
}

type QueueUnbind struct {
    // reserved-1
    queue string  // ShortStr
    exchange string  // ShortStr
    routingKey string  // ShortStr
    arguments []Field
}

func unmarshalQueueUnbind(buf []byte) QueueUnbind {
    queueUnbind := QueueUnbind {}
    offs := 0
    offs += 8  // reserved-1
    queueUnbind.queue, offs = unmarshalShortStr(buf, offs)
    queueUnbind.exchange, offs = unmarshalShortStr(buf, offs)
    queueUnbind.routingKey, offs = unmarshalShortStr(buf, offs)
    queueUnbind.arguments, offs = unmarshalFieldTable(buf, offs)

    return queueUnbind
}

type QueuePurge struct {
    // reserved-1
    queue string  // ShortStr
    noWait byte  // bit
}

func unmarshalQueuePurge(buf []byte) QueuePurge {
    queuePurge := QueuePurge {}
    offs := 0
    offs += 8  // reserved-1
    queuePurge.queue, offs = unmarshalShortStr(buf, offs)
    queuePurge.noWait, offs = unmarshalUint8(buf, offs)

    return queuePurge
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
