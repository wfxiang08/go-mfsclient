package moosefs

import "errors"

type Chunk struct {
    id      uint64
    length  uint64
    version uint32
    csdata  []byte
}

func (ck *Chunk) Read(buf []byte, offset uint32) (int, error) {
    csdata := ck.csdata
    for len(csdata) > 0 {
        for try := 0; try < 2; try++ {
            cs, err := newCSConn(csdata, false)
            if err != nil {
                break
            }

            n, err := cs.ReadBlock(ck.id, ck.version, buf, offset)
            if err != nil {
                cs.Close()
            } else {
                freeCSConn(cs)
                return n, err
            }
        }
        csdata = csdata[6:]
    }
    return 0, errors.New("no chunk server avail")
}

func (ck *Chunk) Write(buf []byte, offset uint32) (int, error) {
    cs, err := newCSConn(ck.csdata, true)
    if err != nil {
        return 0, err
    }

    msg := pack(CUTOCS_WRITE, ck.id, ck.version, ck.csdata)
    _, err = cs.Write(msg)
    if err != nil {
        return 0, err
    }

    writeid := uint32(0)
    pos := uint16(offset>>16) & 0x3FF
    from := int(offset & 0xFFFF)
    size := len(buf)
    start := 0
    w := 0
    for size > 0 {
        if size > 0x10000-from {
            w = 0x10000 - from
        } else {
            w = size
        }
        if err := cs.WriteBlock(ck.id, writeid, pos, uint16(from), buf[start:start+w]); err != nil {
            return start, err
        }
        size -= w
        start += w
        pos += 1
        from = 0
        writeid += 1
    }
    return start, nil
}
