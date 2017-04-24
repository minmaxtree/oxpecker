package oxpecker

import (
    "encoding/binary"
)

type Field struct {
    name string
    content interface{}
}

func packingBits(bits ...byte) byte {
    var ret byte = 0
    for i, bit := range bits {
        ret += (bit << uint(i))
    }
    return ret
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

func marshalM(values []interface{}) [] byte {
    buf := []byte {}
    for _, value := range values {
        buf = append(buf, marshal(value)...)
    }
    return buf
}

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
