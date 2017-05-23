package oxpecker

import (
    "net"
)

// class exchange methods
const (
    EXCHANGE_DECLARE uint16 = 10
    EXCHANGE_DECLARE_OK uint16 = 11
    EXCHANGE_DELETE uint16 = 20
    EXCHANGE_DELETE_OK uint16 = 21
)

type ExchangeDeclare struct {
    // reserved-1
    exchange string  // shortStr
    typ string  // shortStr
    passive byte  // bit
    durable byte  // bit
    // reserved-2
    // reserved-3
    noWait byte  // bit
    arguments  []Field
}

type ExchangeDeclareOK struct {
}

type ExchangeDelete struct {
    // reserved-1
    exchange string  // shortStr
    ifUnused byte  // bit
    noWait byte  // bit
}

type ExchangeDeleteOK struct {}

func unmarshalExchangeDeclare(buf []byte) ExchangeDeclare {
    exchangeDeclare := ExchangeDeclare {}
    offs := 0
    offs += 2  // reserved-1
    exchangeDeclare.exchange, offs = unmarshalShortStr(buf, offs)
    exchangeDeclare.typ, offs = unmarshalShortStr(buf, offs)
    flags, offs := unmarshalUint8(buf, offs);
    exchangeDeclare.passive = (flags >> 0) & 1
    exchangeDeclare.durable = (flags >> 1) & 1
    offs += 1  // reserved-2
    offs += 1  // reserved-3
    flags, offs = unmarshalUint8(buf, offs)
    exchangeDeclare.noWait = (flags >> 0) & 1
    exchangeDeclare.arguments, offs = unmarshalFieldTable(buf, offs)

    return exchangeDeclare
}

func unmarshalExchangeDelete(buf []byte) ExchangeDelete {
    exchangeDelete := ExchangeDelete {}
    offs := 0
    offs += 2  // reserved-1
    exchangeDelete.exchange, offs = unmarshalShortStr(buf, offs)
    flags, offs := unmarshalUint8(buf, offs);
    exchangeDelete.ifUnused = (flags >> 0) & 1
    exchangeDelete.noWait = (flags >> 1) & 1

    return exchangeDelete
}

func SendExchangeDeclare(conn net.Conn,
                         exchange string,
                         typ string,
                         passive byte,
                         durable byte,
                         noWait byte,
                         arguments []Field) {
    var flags byte = passive << 0 + durable << 1 + noWait << 2
    params := []interface {} {
        EXCHANGE,
        EXCHANGE_DECLARE,
        RESERVED8,
        []byte(exchange),
        []byte(typ),
        flags,
        arguments,
    }
    frame := marshalMethodFrame(0, params)
    conn.Write(frame)
}
