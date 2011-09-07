package moosefs

import (
    "net"
    "os"
    "bytes"
    "sync"
    "hash/crc32"
    "strconv"
    "fmt"
    "io"
)

type csConn struct {
    sync.Mutex
    net.Conn
}

func (cs *csConn) Read(b []byte) (int, os.Error) {
    n, err := io.ReadFull(cs.Conn, b)
    //    fmt.Println("<<<", n, b[:n])
    return n, err
}

var (
    pool  = map[string][]*csConn{}
    mutex sync.Mutex
)

func newCSConn(csdata []byte, write bool) (conn *csConn, err os.Error) {
    if len(csdata) < 6 {
        return nil, os.NewError("no chunk server avail")
    }

    // parse chunk server ip and port
    var ip uint32
    var port uint16
    read(bytes.NewBuffer(csdata), &ip, &port)
    addr := fmt.Sprintf("%d.%d.%d.%d:%d", ip>>24, 0xff&(ip>>16), 0xff&(ip>>8), 0xff&ip, port)

    mutex.Lock()
    cs, ok := pool[addr]
    if ok && len(cs) > 0 {
        pool[addr] = nil
        pool[addr] = cs[:len(cs)-1]
        mutex.Unlock()
        return cs[len(cs)-1], nil
    }
    mutex.Unlock()

    conn = new(csConn)
    conn.Conn, err = net.Dial("tcp", addr)
    if err != nil {
        return nil, err
    }
    return conn, nil
}

func freeCSConn(conn *csConn) {
    if conn == nil {
        return
    }

    mutex.Lock()
    defer mutex.Unlock()

    addr := conn.RemoteAddr().String()
    cs, ok := pool[addr]
    if !ok {
        pool[addr] = nil
    }
    pool[addr] = append(cs, conn)
}

func (cs *csConn) ReadBlock(chunkid uint64, version uint32, buf []byte, offset uint32) (n int, err os.Error) {
    size := uint32(len(buf))
    msg := pack(CUTOCS_READ, chunkid, version, offset, size)

    if _, err = cs.Write(msg); err != nil {
        return
    }

    for {
        b := make([]byte, 8)
        if _, err := cs.Read(b); err != nil {
            return
        }
        var cmd, l uint32
        read(bytes.NewBuffer(b), &cmd, &l)
        switch cmd {
        case CSTOCU_READ_STATUS:
            if l != 9 {
                return n, os.NewError("readblock: READ_STATUS incorrect message size")
            }
            data := make([]byte, l)
            if _, err := cs.Read(data); err != nil {
                return n, err
            }
            var cid uint64
            read(bytes.NewBuffer(data), &cid)
            if cid != chunkid {
                return n, os.NewError("readblock; READ_STATUS incorrect chunkid")
            }
            if n != len(buf) {
                return n, os.NewError("readblock; READ_STATUS incorrect data size")
            }
            return n, nil
        case CSTOCU_READ_DATA:
            if l < 20 {
                return n, os.NewError("readblock; READ_DATA incorrect message size")
            }
            data := make([]byte, 20)
            if _, err := cs.Read(data); err != nil {
                return
            }
            r := bytes.NewBuffer(data)
            var cid uint64
            var blockno, blockoffset uint16
            var blocksize, blockcrc uint32
            read(r, &cid, &blockno, &blockoffset, &blocksize, &blockcrc)
            if cid != chunkid {
                return n, os.NewError("readblock; READ_DATA incorrect chunkid ")
            }
            if l != 20+blocksize {
                return n, os.NewError("readblock; READ_DATA incorrect message size ")
            }
            if blocksize == 0 { // FIXME
                return n, os.NewError("readblock; READ_DATA empty block")
            }
            if blockno != uint16(offset>>16) {
                return n, os.NewError("readblock; READ_DATA incorrect block number")
            }
            if blockoffset != uint16(offset&0xFFFF) {
                return n, os.NewError("readblock; READ_DATA incorrect block offset")
            }
            breq := 65536 - uint32(blockoffset)
            if size < breq {
                breq = size
            }
            if blocksize != breq {
                return n, os.NewError("readblock; READ_DATA incorrect block size")
            }
            data = buf[n : n+int(blocksize)]
            if _, err = cs.Read(data); err != nil {
                return n, err
            }
            if blockcrc != crc32.ChecksumIEEE(data) {
                return n, os.NewError("readblock; READ_DATA crc checksum error")
            }
            n += int(blocksize)
            offset += blocksize
            size -= blocksize
        default:
            return n, os.NewError("readblock; unknown message:" + strconv.Itoa(int(cmd)))
        }
    }
    return n, nil
}

type Chunk struct {
    id      uint64
    length  uint64
    version uint32
    csdata  []byte
}

func (ck *Chunk) Read(buf []byte, offset uint32) (int, os.Error) {
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
    return 0, os.NewError("no chunk server avail")
}

func (ck *Chunk) Write(buf []byte, offset uint32) (int, os.Error) {
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

func (cs *csConn) WriteBlock(chunkid uint64, writeid uint32, blockno, offset uint16, buf []byte) os.Error {
    size := uint32(len(buf))
    crc := crc32.ChecksumIEEE(buf)
    msg := pack(CUTOCS_WRITE_DATA, chunkid, writeid, blockno, offset, size, crc, buf)
    if n, err := cs.Write(msg); err != nil || n < len(msg) {
        return os.NewError("write block " + err.String())
    }
    rbuf := make([]byte, 21)
    n, err := cs.Read(rbuf)
    if err != nil || n < 21 {
        return err
    }
    var cmd, leng, wid uint32
    var cid uint64
    var status uint8
    r := bytes.NewBuffer(rbuf)
    read(r, &cmd, &leng)
    if cmd == 0 && leng == 0 {
        // skip anon cmd
        copy(rbuf, rbuf[8:])
        if _, err := cs.Read(rbuf[n-8:]); err != nil {
            return err
        }
        r = bytes.NewBuffer(rbuf)
        read(r, &cmd, &leng)
    }
    read(r, &cid, &writeid, &status)
    if cmd != CSTOCU_WRITE_STATUS || leng != 13 {
        return os.NewError("write block: got unrecognized packet from chunkserver ")
    }
    if cid != chunkid || wid != writeid {
        return os.NewError("write block: got unexpected packet")
    }
    if status != STATUS_OK {
        return Error(status)
    }

    return nil
}
