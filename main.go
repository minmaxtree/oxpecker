package oxpecker

import (
    "fmt"
    "log"
    "net"
    "encoding/binary"
    "os"
    // "errors"
    // "strings"
    "io"
)

var eoframe = []byte { 0xce }
var queues = map[string]*Queue {}

const RESERVED8 byte = 0x00
const RESERVED16 uint16 = 0x0000
const RESERVED32 uint32 = 0x00000000

const (
    METHOD = 1
    HEADER = 2
    BODY = 3
    HEARTBEAT = 4
)

// class ids
const (
    CONNECTION uint16 = 10
    CHANNEL uint16 = 20
    EXCHANGE uint16 = 40
    QUEUE uint16 = 50
    BASIC uint16 = 60
    TX uint16 = 90
)

type Queue struct {
    Name string
    // MessageCount int
    // ConsumerCount int
    Messages [][]byte
    Consumers []net.Conn
}

func SendContentHeader(conn net.Conn, bodySize uint64,
        propertyFlags uint16, properties BasicProperties) {
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

            SendBasicDeliver(queue.Consumers[0],
                             "consumer_tag",
                             "delivery_tag",
                             0,
                             "",
                             basicPublish.routingKey,
            )
            var propertyFlags uint16 = 0x9000
            properties := BasicProperties {
                ContentType: "text/plain",
                DeliveryMode: 2,
            }
            SendContentHeader(queue.Consumers[0], uint64(len(message)),
                propertyFlags, properties)
            SendContentBody(queue.Consumers[0], string(message))

            queue.Messages = queue.Messages[1:]
        }
    }
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

func check(err error) {
    if err != nil {
        log.Println(err)
        os.Exit(-1)
    }
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
