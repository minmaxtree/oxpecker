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
    // "sync"
)

var defaultExchangeName string = ""

type Oxpecker struct {
    vHosts []VHost
    connections map[net.Conn]Connection
    // rwMutex sync.RWMutex
}

type VHost struct {
    path string
    exchanges map[string]Exchange
    mQueues map[string]*MQueue
}

func newVHost(path string) VHost {
    vHost := VHost {}
    vHost.path = path
    vHost.exchanges = map[string]Exchange {}
    vHost.mQueues = map[string]*MQueue {}

    // create default exchange
    // defaultExchange := newExchange(defaultExchangeName, EXCHANGE_DIRECT)
    defaultExchange := newDirectExchange(defaultExchangeName)
    vHost.exchanges[defaultExchangeName] = defaultExchange
    return vHost
}

type Exchange interface {
    routing(routingKey string, arguments []Field) []*MQueue
    getName() string
    bind(routingKey string, arguments []Field, queue *MQueue) bool
}

func (directExchange *DirectExchange)routing(routingKey string, arguments []Field) []*MQueue {
    queues := []*MQueue {}
    queues = append(queues, directExchange.binds[routingKey])
    return queues
}

func (directExchange DirectExchange)getName() string {
    return directExchange.name
}

func (directExchange *DirectExchange)bind(routingKey string, arguments []Field, queue *MQueue) bool {
    directExchange.binds[routingKey] = queue
    return true
}

func (fanoutExchange *FanoutExchange)routing(routingKey string, arguments []Field) []*MQueue {
    queues := []*MQueue {}
    for _, queue := range fanoutExchange.binds {
        queues = append(queues, queue)
    }
    return queues
}

func (fanoutExchange FanoutExchange)getName() string {
    return fanoutExchange.name
}

func (fanoutExchange FanoutExchange)bind(routingKey string, arguments []Field, queue *MQueue) bool {
    fanoutExchange.binds = append(fanoutExchange.binds, queue)
    return true
}

func (headersExchange HeadersExchange)bind(routingKey string, arguments []Field, queue *MQueue) bool {
    binding := HEBinding {}
    for _, argument := range arguments {
        name := argument.name
        content, ok := argument.content.(string)
        if !ok {
            return false
        }
        if name == "x-match" {
            if content == "all" {
                binding.typ = HEBindingAll
            } else if content == "any" {
                binding.typ = HEBindingAny
            } else {
                return false
            }
        } else {
            binding.headers[name] = content
        }
    }

    headersExchange.binds = append(headersExchange.binds, &binding)

    return true
}

type DirectExchange struct {
    name string
    binds map[string]*MQueue
}

type FanoutExchange struct {
    name string
    binds []*MQueue
}

type TopicExchange struct {
    name string
    binds map[string]*MQueue
}

type HeadersExchange struct {
    name string
    binds []*HEBinding
}

type HEBinding struct {
    typ int
    // headers []MHeader
    headers map[string]string
    mQueue *MQueue
}

const (
    HEBindingAll = iota
    HEBindingAny
)

// type MHeader struct {
//     typ int
//     fields map[string]string
// }

// type Exchange struct {
//     typ int
//     name string
//     binds map[string]*MQueue
// }

func newDirectExchange(name string) *DirectExchange {
    exchange := DirectExchange {}
    exchange.name = name
    exchange.binds = map[string]*MQueue {}
    return &exchange
}

func newFanoutExchange(name string) *FanoutExchange {
    exchange := FanoutExchange {}
    exchange.name = name
    exchange.binds = []*MQueue {}
    return &exchange
}

type MQueue struct {
    name string
    messages []Message
    consumers map[net.Conn]int
}

func newMQueue() *MQueue {
    mQueue := MQueue {}
    mQueue.messages = []Message {}
    mQueue.consumers = map[net.Conn]int {}
    return &mQueue
}

func (queue *MQueue)nextConsumer() net.Conn {
    var least int
    var ret net.Conn
    isFirst := true
    for consumer, count := range queue.consumers {
        if count <= least || isFirst {
            least = count
            ret = consumer
        }
        isFirst = false
    }
    queue.consumers[ret]++
    return ret
}

type Message struct {
    content []byte
    exchange string
    routingKey string
}

func New() Oxpecker {
    oxpecker := Oxpecker {}
    oxpecker.vHosts = []VHost {}
    oxpecker.connections = map[net.Conn]Connection {}

    return oxpecker
}

type Connection struct {
    vHost VHost
    conn net.Conn
}

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

func (oxpecker *Oxpecker)closeClient(conn net.Conn) {
    delete(oxpecker.connections, conn)
    for _, vHost := range oxpecker.vHosts {
        for _, mQueue := range vHost.mQueues {
            delete(mQueue.consumers, conn)
        }
    }
}

func (oxpecker *Oxpecker)ServerReceiveMessage(conn net.Conn) (byte, uint16, interface{}, error) {
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
                var connection Connection
                vHostExists := false
                for _, vHost := range oxpecker.vHosts {
                    if vHost.path == connectionOpen.virtualHost {
                        vHostExists = true
                        connection = Connection { vHost: vHost, conn: conn }
                    }
                }
                if !vHostExists {
                    vHost := newVHost(connectionOpen.virtualHost)
                    oxpecker.vHosts = append(oxpecker.vHosts, vHost)
                    connection = Connection { vHost: vHost, conn: conn }
                }
                oxpecker.connections[conn] = connection
                SendConnectionOpenOK(conn)
            case CONNECTION_CLOSE:
                connectionClose := unmarshalConnectionClose(bodyBuf[4:])
                fmt.Println("connectionClose is:", connectionClose)
                SendConnectionCloseOK(conn)

                oxpecker.closeClient(conn)
            }
        case QUEUE:
            switch method {
            case QUEUE_DECLARE:
                queueDeclare := unmarshalQueueDeclare(bodyBuf[4:])
                fmt.Println("queueDeclare is:", queueDeclare)

                connection := oxpecker.connections[conn]
                vHost := connection.vHost
                var queue *MQueue
                queueExists := false
                for _, mQueue := range vHost.mQueues {
                    if queueDeclare.queue == mQueue.name {
                        queueExists = true
                        queue = mQueue
                        break
                    }
                }

                if !queueExists && queueDeclare.passive == 1 {
                    break
                }

                if !queueExists && queueDeclare.passive == 0 {
                    // create queue and default binding
                    queue = newMQueue()
                    queue.name = queueDeclare.queue
                    vHost.mQueues[queue.name] = queue

                    fmt.Println("queue.name is:", queue.name)
                    fmt.Printf("queue is %#v\n", queue)

                    exchange := vHost.exchanges[defaultExchangeName]
                    exchange.bind(queueDeclare.queue, []Field {}, queue)

                    // for _, exchange := range vHost.exchanges {
                    //     if exchange.getName() == defaultExchangeName {
                    //         exchange.binds[queue.name] = queue
                    //     }
                    // }
                }

                // if queues["clojure"] == nil {
                //     newQueue := new(Queue)
                //     newQueue.Messages = [][]byte {}
                //     newQueue.Consumers = []net.Conn {}
                //     queues["clojure"] = newQueue
                // }

                SendQueueDeclareOK(conn, queue.name, 0, 0)
            case QUEUE_BIND:
                queueBind := unmarshalQueueBind(bodyBuf[4:])
                fmt.Println("queueBind is:", queueBind)
                if oxpecker.handleQueueBind(conn, queueBind) {
                    SendQueueBindOK(conn)
                }
            }
        case BASIC:
            switch method {
            case BASIC_PUBLISH:
                basicPublish := unmarshalBasicPublish(bodyBuf[4:])
                fmt.Println("basicPublish is:", basicPublish)
                oxpecker.handleBasicPublish(conn, basicPublish)
            case BASIC_CONSUME:
                basicConsume := unmarshalBasicConsume(bodyBuf[4:])
                fmt.Println("basicConsume is:", basicConsume)
                if oxpecker.handleBasicConsume(conn, basicConsume) {
                    consumerTag := basicConsume.consumerTag
                    if consumerTag == "" {
                        // consumerTag = oxpecker.getChannel().nextConsumerTag()
                    }
                    SendBasicConsumeOK(conn, consumerTag)
                }
            }
        case EXCHANGE:
            switch method {
            case EXCHANGE_DECLARE:
                exchangeDeclare := unmarshalExchangeDeclare(bodyBuf[4:])
                fmt.Println("exchangeDeclare is:", exchangeDeclare)
                if oxpecker.handleExchangeDeclare(conn, exchangeDeclare) {
                    SendExchangeDeclareOK(conn)
                }
            case EXCHANGE_DELETE:
                exchangeDelete := unmarshalExchangeDelete(bodyBuf[4:])
                fmt.Println("exchangeDelete is:", exchangeDelete)
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

func (oxpecker *Oxpecker)findConnVHost(conn net.Conn) (VHost, bool) {
    connection, ok := oxpecker.connections[conn]
    if !ok {
        return VHost {}, false
    }
    return connection.vHost, true
}

func (oxpecker *Oxpecker)handleExchangeDeclare(conn net.Conn, exchangeDeclare ExchangeDeclare) bool {
    // exchange := newExchange(exchangeDeclare.exchange, 0)

    // switch exchangeDeclare.typ {
    // case "fanout":
    //     exchange.typ = EXCHANGE_FANOUT
    // default:
    //     return false
    // }

    vHost, ok := oxpecker.findConnVHost(conn)
    if !ok {
        return false
    }
    var exchange Exchange
    switch exchangeDeclare.typ {
    case "x-direct":
        exchange = newDirectExchange(exchangeDeclare.exchange)
    case "x-fanout":
        exchange = newFanoutExchange(exchangeDeclare.exchange)
    default:
        return false
    }
    vHost.exchanges[exchangeDeclare.exchange] = exchange

    return true
}

func (oxpecker *Oxpecker)handleQueueBind(conn net.Conn, queueBind QueueBind) bool {
    vHost, ok := oxpecker.findConnVHost(conn)
    if !ok {
        return false
    }
    // vHost.exchanges[queueBind.exchange] = queueBind.queue
    // queueBind.routingKey
    exchange, ok := vHost.exchanges[queueBind.exchange]
    if !ok {
        return false
    }
    queue, ok := vHost.mQueues[queueBind.queue]
    if !ok {
        return false
    }
    // headers, ok := makeMHeader(queueBind.arguments)
    // if !ok {
    //     return false
    // }
    return exchange.bind(queueBind.routingKey, queueBind.arguments, queue)
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
            case CONNECTION_CLOSE:
                SendConnectionCloseOK(conn)
                return 0, 0, nil, nil
            case CONNECTION_CLOSE_OK:
                return 0, 0, nil, nil
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

func (oxpecker *Oxpecker)routing(conn net.Conn, exchangeName string,
        routingKey string, arguments []Field) []*MQueue {
    queues := []*MQueue {}

    connection := oxpecker.connections[conn]
    vHost := connection.vHost
    exchange, ok := vHost.exchanges[exchangeName]
    if ok {
        return exchange.routing(routingKey, arguments)
    }

    return queues
}

func (oxpecker *Oxpecker)findQueue(conn net.Conn, queueName string) (*MQueue, bool) {
    connection := oxpecker.connections[conn]
    vHost := connection.vHost
    mQueue, ok := vHost.mQueues[queueName]
    return mQueue, ok
}

func (oxpecker *Oxpecker)handleBasicConsume(conn net.Conn, basicConsume BasicConsume) bool {
    // queue := queues[basicConsume.queue]
    queue, ok := oxpecker.findQueue(conn, basicConsume.queue)
    if ok {
        fmt.Printf("[*] queue is %#v\n", queue)

        fmt.Println("[handleBasicConsume] queue is:", queue)
        fmt.Println("[handleBasicConsume] queue name is:", basicConsume.queue)
        // queue.consumers = append(queue.consumers, conn)
        queue.consumers[conn] = len(queue.messages)
        for _, message := range queue.messages {
            deliverMessage(conn, message)
        }
        queue.messages = []Message {}
        fmt.Println("[handleBasicConsume] queue.consumers is:", queue.consumers)
        return true
    } else {
        return false
    }
}

func (oxpecker *Oxpecker)handleBasicPublish(conn net.Conn, basicPublish BasicPublish) {
    // queues := oxpecker.routing(conn, basicPublish.exchange, basicPublish.routingKey)


    // queue := queues[basicPublish.routingKey]
    // fmt.Println("[0] queue is:", queue)

    typ, _, payload, _ := oxpecker.ServerReceiveMessage(conn)

    var arguments []Field

    if typ == HEADER {
        switch payload.(type) {
        case ContentHeader:
            // process content header info
            contentHeader := payload.(ContentHeader)
            propertyFlags := contentHeader.propertyFlags
            properties := payload.(ContentHeader).properties
            if ((propertyFlags >> 13) & 1) == 1 {
                arguments = properties.Headers
            }
        default:
            // error 505
        }
    } else {
        // error 505
    }

    // receive content body
    body := []byte {}
    for {
        typ, _, payload, err := oxpecker.ServerReceiveMessage(conn)
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

    queues := oxpecker.routing(conn, basicPublish.exchange, basicPublish.routingKey, arguments)

    for _, queue := range queues {
        fmt.Printf("[#] queue is %#v\n", queue)

        message := Message {
            content: body,
            exchange: basicPublish.exchange,
            routingKey: basicPublish.routingKey,
        }
        // queue.messages = append(queue.messages, message)

        if len(queue.consumers) > 0 {
            consumer := queue.nextConsumer()
            deliverMessage(consumer, message)
        } else {
            queue.messages = append(queue.messages, message)
        }

        fmt.Println("queue.messages is:", queue.messages)
        fmt.Println("len(queue.messages) is:", len(queue.messages))
        fmt.Println("len(queue.consumers) is:", len(queue.consumers))
        fmt.Println("queue.consumers is:", queue.consumers)
        fmt.Println("routingKey is:", basicPublish.routingKey)

        for _, vHost := range oxpecker.vHosts {
            fmt.Println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
            fmt.Printf("vHost is %#v\n\n", vHost)
            fmt.Printf("vHost.mQueues is %#v\n\n", vHost.mQueues)
            for _, mQueue := range vHost.mQueues {
                fmt.Printf("mQueue is %#v\n\n", mQueue)
                fmt.Printf("mQueue.messages is %#v\n\n", mQueue.messages)
            }
            fmt.Println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        }
    }
}

func deliverMessage(conn net.Conn, message Message) {
    SendBasicDeliver(conn, "consumer_tag", "delivery_tag", 0, message.exchange, message.routingKey)
    var propertyFlags uint16 = 0x9000
    properties := BasicProperties {
        ContentType: "text/plain",
        DeliveryMode: 2,
    }
    SendContentHeader(conn, uint64(len(message.content)), propertyFlags, properties)
    SendContentBody(conn, string(message.content))
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

func readFrame(conn net.Conn) {
    headerBuf := make([]byte, 7)
    _, err := conn.Read(headerBuf)
    check(err)
    header := unmarshalHeader(headerBuf)

    bodyBuf := make([]byte, header.length + 1)  // end of frame 0xce
    _, err = conn.Read(bodyBuf)
    check(err)
}

// func (oxpecker *Oxpecker)serveConsumers() {
//     for {
//         for _, vHost := range oxpecker.vHosts {
//             for _, mQueue := range vHost.mQueues {
//                 fmt.Println("[s] len(mQueue.messages) is:", len(mQueue.messages))
//                 if len(mQueue.messages) > 0 {
//                     p := len(mQueue.messages) / len(mQueue.consumers)
//                     q := len(mQueue.messages) % len(mQueue.consumers)

//                     i := 0
//                     for k, _ := range mQueue.consumers {
//                         n := p
//                         if i < q { n++ }

//                         for j := 0; j < n; j++ {
//                             fmt.Println("************* will deliverMessage")
//                             deliverMessage(k, mQueue.messages[0])
//                             mQueue.messages = mQueue.messages[1:]
//                         }
//                         i++
//                     }
//                 }
//             }
//         }
//     }
// }

func check(err error) {
    if err != nil {
        log.Println(err)
        os.Exit(-1)
    }
}
