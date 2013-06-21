package moosefs

import (
    "bytes"
    "errors"
    "fmt"
    "hash/crc32"
    "io"
    "net"
    "strconv"
    "sync"
)

type csConn struct {
    net.Conn
}

func (cs *csConn) Read(b []byte) (int, error) {
    n, err := io.ReadFull(cs.Conn, b)
    return n, err
}

var (
    pool  = map[string][]*csConn{}
    mutex sync.Mutex
)

func newCSConn(csdata []byte, write bool) (conn *csConn, err error) {
    if len(csdata) < 6 {
        return nil, errors.New("no chunk server avail")
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
        cs = nil
    }
    pool[addr] = append(cs, conn)
}

func (cs *csConn) ReadBlock(chunkid uint64, version uint32, buf []byte, offset uint32) (n int, err error) {
    size := uint32(len(buf))
    msg := pack(CUTOCS_READ, chunkid, version, offset, size)

    if _, err = cs.Write(msg); err != nil {
        return
    }

    for {
        b := make([]byte, 8)
        if _, err = cs.Read(b); err != nil {
            return
        }
        var cmd, l uint32
        read(bytes.NewBuffer(b), &cmd, &l)
        switch cmd {
        case CSTOCU_READ_STATUS:
            if l != 9 {
                return n, errors.New("readblock: READ_STATUS incorrect message size")
            }
            data := make([]byte, l)
            if _, err = cs.Read(data); err != nil {
                return n, err
            }
            var cid uint64
            read(bytes.NewBuffer(data), &cid)
            if cid != chunkid {
                return n, errors.New("readblock; READ_STATUS incorrect chunkid")
            }
            if n != len(buf) {
                return n, errors.New("readblock; READ_STATUS incorrect data size")
            }
            return n, nil
        case CSTOCU_READ_DATA:
            if l < 20 {
                return n, errors.New("readblock; READ_DATA incorrect message size")
            }
            data := make([]byte, 20)
            if _, err = cs.Read(data); err != nil {
                return
            }
            r := bytes.NewBuffer(data)
            var cid uint64
            var blockno, blockoffset uint16
            var blocksize, blockcrc uint32
            read(r, &cid, &blockno, &blockoffset, &blocksize, &blockcrc)
            if cid != chunkid {
                return n, errors.New("readblock; READ_DATA incorrect chunkid ")
            }
            if l != 20+blocksize {
                return n, errors.New("readblock; READ_DATA incorrect message size ")
            }
            if blocksize == 0 { // FIXME
                return n, errors.New("readblock; READ_DATA empty block")
            }
            if blockno != uint16(offset>>16) {
                return n, errors.New("readblock; READ_DATA incorrect block number")
            }
            if blockoffset != uint16(offset&0xFFFF) {
                return n, errors.New("readblock; READ_DATA incorrect block offset")
            }
            breq := 65536 - uint32(blockoffset)
            if size < breq {
                breq = size
            }
            if blocksize != breq {
                return n, errors.New("readblock; READ_DATA incorrect block size")
            }
            data = buf[n : n+int(blocksize)]
            if _, err = cs.Read(data); err != nil {
                return n, err
            }
            if blockcrc != crc32.ChecksumIEEE(data) {
                return n, errors.New("readblock; READ_DATA crc checksum error")
            }
            n += int(blocksize)
            offset += blocksize
            size -= blocksize
        default:
            return n, errors.New("readblock; unknown message:" + strconv.Itoa(int(cmd)))
        }
    }
    return n, nil
}

func (cs *csConn) WriteBlock(chunkid uint64, writeid uint32, blockno, offset uint16, buf []byte) error {
    size := uint32(len(buf))
    crc := crc32.ChecksumIEEE(buf)
    msg := pack(CUTOCS_WRITE_DATA, chunkid, writeid, blockno, offset, size, crc, buf)
    if n, err := cs.Write(msg); err != nil || n < len(msg) {
        return errors.New("write block " + err.Error())
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
        return errors.New("write block: got unrecognized packet from chunkserver ")
    }
    if cid != chunkid || wid != writeid {
        return errors.New("write block: got unexpected packet")
    }
    if status != STATUS_OK {
        return Error(status)
    }

    return nil
}
