package moosefs

import (
    "io"
    "os"
    "net"
    "bytes"
    "sync"
    "strconv"
    "strings"
//    "fmt"
    "time"
)

type MasterConn struct {
    addr string
    subdir string
  
    uid,gid uint32

    sessionid uint32
    conn net.Conn
    sync.Mutex
}

func NewMasterConn(addr, subdir string) *MasterConn {
    if !strings.Contains(addr, ":") {
        addr += ":9421"
    }
    mc := new(MasterConn)
    mc.addr = addr
    mc.subdir = subdir
    return mc
}

func (mc *MasterConn) Connect() (err os.Error) {
//    mc.Lock("connect")
//    defer mc.Unlock("connect")
    if mc.conn != nil {
        return nil
    }

    // FIXME timeout
    println("dial tcp", mc.addr)
    mc.conn, err = net.Dial("tcp", mc.addr)
    if err != nil {
        println("connect to ", mc.addr, err.String())
        return
    }
    defer func() {
        if err != nil {
            mc.conn.Close()
            mc.conn = nil
            mc.sessionid = 0
        }
    }()

    var regbuf []byte
    if mc.sessionid == 0 {
        regbuf = pack(CUTOMA_FUSE_REGISTER, FUSE_REGISTER_BLOB_ACL, REGISTER_NEWSESSION, VERSION,
            uint32(2), "/\000", uint32(len(mc.subdir)+1), mc.subdir+"\000")
    }else{
        regbuf = pack(CUTOMA_FUSE_REGISTER, FUSE_REGISTER_BLOB_ACL, REGISTER_RECONNECT, mc.sessionid, VERSION)
    }

    if _, err = mc.Write(regbuf); err != nil {
        return
    }

    recv := regbuf[:8]
    if _,err = mc.Read(recv); err != nil {
        return
    }

    var cmd,i uint32
    r := bytes.NewBuffer(recv)
    read(r, &cmd, &i)
    if cmd != MATOCU_FUSE_REGISTER {
        err = os.NewError("got incorrect answer from mfsmaster")
        return
    }
    if !(i ==1 || i==13 || i==21) {
        err = os.NewError("got incorrect size from mfsmaster")
        return
    }
   
    buf := make([]byte, i)
    if n,e := mc.Read(buf); e != nil || n != int(i) {
        if e == nil {
           e  = os.NewError("unexpected end")
        }
        err = e
        return
    }
    if i == 1 && buf[0] != 0 {
        err = os.NewError("mfsmaster register error: "+mfs_strerror(int(buf[0])))
        return
    }
    if mc.sessionid == 0 {
        r = bytes.NewBuffer(buf)
        read(r, &mc.sessionid)
        // read sesflags and uid ...
    }
    go func() {
        for {
            if mc.nop() != nil {
                break
            }
            time.Sleep(2e9)
        }
    }()
    return
}

func (mc *MasterConn) Close() {
    if mc.conn != nil {
        mc.conn.Close()
        mc.conn = nil
    }
}

func (mc *MasterConn) Lock(caller string) {
    mc.Mutex.Lock()
//    println("lock master by ", caller)
}

func (mc *MasterConn) Unlock(caller string) {
    mc.Mutex.Unlock()
//    println("unlock master by ", caller)
}

func (mc *MasterConn) Read(b []byte) (int, os.Error) {
    n, err := io.ReadFull(mc.conn, b)
//    fmt.Println("<<<", b[:n])
    return n, err
}

func (mc *MasterConn) Write(b []byte) (int, os.Error) {
//    fmt.Println(">>>", b)
    return mc.conn.Write(b)
}

func (mc *MasterConn) nop() os.Error {
    mc.Lock("nop")
    defer mc.Unlock("nop")
    if mc.conn == nil {
        return os.NewError("not connected")
    }
    msg := pack(ANTOAN_NOP, uint32(0))
    if n, err := mc.Write(msg); err != nil || n != 12 {
        mc.Close()
        return err
    }
    return nil
}

func (mc *MasterConn) sendAndReceive(cmd uint32, args ...interface{}) (r []byte, err os.Error) {
    // try to connect
    //println(cmd, args)
    packetid := uint32(1)
    nargs := make([]interface{}, len(args)+1)
    nargs[0] = packetid
    for i,a := range args {
        nargs[i+1] = a
    }
    send_bytes := pack(cmd, nargs...)
    mc.Lock("sendandrecv")
    defer mc.Unlock("sendandrecv")

    for ii:=0; ii<2; ii++ {
        mc.Connect()
        if mc.conn == nil {
            return nil, os.NewError("session lost")
        }
        if _, err = mc.Write(send_bytes); err != nil {
            mc.Close()
            println("write error", err.String())
            continue
        }
        buf := make([]byte, 12)
        if _, err = mc.Read(buf); err != nil {
            mc.Close()
            println("read error", err.String())
            continue
        }
        var rcmd,size,id uint32
        read(bytes.NewBuffer(buf), &rcmd, &size, &id)
        for rcmd == ANTOAN_NOP && size == 4 {
            if _, err = mc.Read(buf); err != nil {
                mc.Close()
                //println("read error", err.String())
                return nil, err
            }
            read(bytes.NewBuffer(buf), &rcmd, &size, &id)
        }
        if rcmd != cmd+1 || id != packetid {
            mc.Close()
            println("unexpected cmd:", cmd+1, rcmd)
            continue
//            return nil, os.NewError(fmt.Sprintf("unexpected cmd:", cmd+1, rcmd))
        }
        if size <= 4 {
            mc.Close()
            println("unexpected size:", size)
            continue
        }
        buf = make([]byte, size-4)
        if n, err := mc.Read(buf); err != nil {
            mc.Close()
            println("read response body failed", err.String())
        }else{
            if n == 1 && buf[0] != 0 {
                //println("cmd", cmd, buf[0], Error(buf[0]).String())
                return nil, Error(buf[0])
            }
            return buf, nil
        }
    }
    if err == nil {
        err = os.NewError("IO Error")
    }
    return nil, err
}

type StatInfo struct {
    totalspace uint64
    availspace uint64
    trashspace uint64
    reservedspace uint64
    inodes      uint32
}

func (mc *MasterConn) StatFS() (*StatInfo, os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_STATFS)
    if err != nil {
        return nil, err
    }
    var stat StatInfo
    r := bytes.NewBuffer(ans)
    read(r, &stat.totalspace, &stat.availspace, &stat.trashspace, &stat.reservedspace, &stat.inodes)
    return &stat, nil
}

func (mc *MasterConn) Access(inode uint32, modemask uint8) (err os.Error) {
    _, err = mc.sendAndReceive(CUTOMA_FUSE_ACCESS, inode, mc.uid, mc.gid, modemask)
    return err
}

func (mc *MasterConn) Lookup(parent uint32, name string) (inode uint32, attr []byte, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_LOOKUP, parent, uint8(len(name)), name, uint32(0), uint32(0))
    if err != nil {
        return 0, nil, err
    }
    if len(ans) == 1 {
        return 0, nil, Error(ans[0])
    }
    if len(ans) != 39 {
        return 0, nil, os.NewError("bad length")
    }
    r := bytes.NewBuffer(ans[:4])
    read(r, &inode)
    attr = ans[4:]
    return
}

func (mc *MasterConn) GetAttr(inode uint32) (fi *os.FileInfo, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_GETATTR, inode, mc.uid, mc.gid)
    if err != nil {
        return nil, err
    }
    return attrToFileInfo(inode, ans), nil
}

func (mc *MasterConn) SetAttr(inode uint32, setmask uint8, mode uint16, attruid, attrgid, atime, mtime uint32) (fi *os.FileInfo, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_SETATTR, inode, mc.uid, mc.gid, setmask, mode, attruid, attrgid, atime, mtime)
    if err != nil {
        return nil, err
    }
    return attrToFileInfo(inode, ans), nil
}

func (mc *MasterConn) Truncate(inode uint32, opened uint8, length int64) (fi *os.FileInfo, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_TRUNCATE, inode, opened, mc.uid, mc.gid, length)
    if err != nil {
        return nil, err
    }
    return attrToFileInfo(inode, ans), nil
}

func (mc *MasterConn) ReadLink(inode uint32) (path string, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_READLINK, inode)
    if err != nil {
        return 
    }
    var length uint32
    read(bytes.NewBuffer(ans[:4]), &length)
    if int(length+4) != len(ans) {
        return "", os.NewError("invalid length")
    }
    return string(ans[4:4+length-1]), nil // path is ending with \000
}

func (mc *MasterConn) Symlink(parent uint32, name, path string) (fi *os.FileInfo, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_SYMLINK, parent, uint8(len(name)), name, 
        uint32(len(path)+1), path, "\000", mc.uid, mc.gid)
    if err != nil {
        return 
    }
    if len(ans) != 39 {
        return nil, os.NewError("invalid length")
    }
    var inode uint32
    read(bytes.NewBuffer(ans[:4]), &inode)
    return attrToFileInfo(inode, ans[4:]), nil
}

func (mc *MasterConn) Mknod(parent uint32, name string, type_ uint8, mode uint16, rdev uint32) (fi *os.FileInfo, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_MKNOD, parent, uint8(len(name)), name, type_, mode, mc.uid, mc.gid, rdev)
    if err != nil {
        return 
    }
    if len(ans) != 39 {
        return nil, os.NewError("invalid length")
    }
    var inode uint32
    read(bytes.NewBuffer(ans[:4]), &inode)
    return attrToFileInfo(inode, ans[4:]), nil
}

func (mc *MasterConn) Mkdir(parent uint32, name string, mode uint16) (fi *os.FileInfo, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_MKDIR, parent, uint8(len(name)), name, mode, mc.uid, mc.gid)
    if err != nil {
        return 
    }
    if len(ans) != 39 {
        return nil, os.NewError("invalid length")
    }
    var inode uint32
    read(bytes.NewBuffer(ans[:4]), &inode)
    return attrToFileInfo(inode, ans[4:]), nil
}

func (mc *MasterConn) Unlink(parent uint32, name string) os.Error {
    _, err := mc.sendAndReceive(CUTOMA_FUSE_UNLINK, parent, uint8(len(name)), name, mc.uid, mc.gid)
    return err
}

func (mc *MasterConn) Rmdir(parent uint32, name string) os.Error {
    _, err := mc.sendAndReceive(CUTOMA_FUSE_RMDIR, parent, uint8(len(name)), name, mc.uid, mc.gid)
    return err
}

func (mc *MasterConn) Rename(parent_src uint32, name_src string, parent_dst uint32, name_dst string) os.Error {
    _, err := mc.sendAndReceive(CUTOMA_FUSE_RENAME, parent_src, uint8(len(name_src)), name_src,
            parent_dst, uint8(len(name_dst)), name_dst, mc.uid, mc.gid)
    return err
}

func (mc *MasterConn) Link(inode_src, parent_dst uint32, name_dst string) (inode uint32, attr []byte, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_LINK, inode_src, parent_dst, uint8(len(name_dst)), name_dst, mc.uid, mc.gid)
    if len(ans) != 39 {
        return 0, nil, os.NewError("invalid length")
    }
    read(bytes.NewBuffer(ans[:4]), &inode)
    attr = ans[4:]
    return 
}

func (mc *MasterConn) GetDir(inode uint32) (names []string, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_GETDIR, inode, mc.uid, mc.gid)
    if err != nil {
        return nil, err
    }
    r := bytes.NewBuffer(ans)
    for r.Len() > 0 {
        var length,type_ uint8
        var inode uint32
        read(r, &length)
        if r.Len() < int(length + 5) {
            break
        }
        name := make([]byte, length)
        r.Read(name)
        read(r, &inode, &type_)
        names = append(names, string(name))
    }
    return
}

func (mc *MasterConn) GetDirPlus(inode uint32) (info []os.FileInfo, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_GETDIR, inode, mc.uid, mc.gid, uint8(GETDIR_FLAG_WITHATTR))
    if err != nil {
        return nil, err
    }
    r := bytes.NewBuffer(ans)
    for r.Len() > 0 {
        var length uint8
        var inode uint32
        read(r, &length)
        if r.Len() < int(length + 39) {
            println("broken getdirplus data")
            break
        }
        name := make([]byte, length)
        attr := make([]byte, 35)
        r.Read(name)
        read(r, &inode)
        r.Read(attr)
        info = append(info, *newFileInfo(string(name), inode, attr))
    }
    return
}

func (mc *MasterConn) OpenCheck(inode uint32, flag uint8) (attr []byte, err os.Error){
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_OPEN, inode, mc.uid, mc.gid, flag)
    if err != nil {
        return nil, err
    }
    return ans, nil
}

func (mc *MasterConn) Release(inode uint32) os.Error {
    return nil
}

func (mc *MasterConn) ReadChunk(inode uint32, indx uint32) (info *Chunk, err os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_READ_CHUNK, inode, indx)
    if err != nil {
        return nil, err
    }
    n := len(ans)
    if n < 20 || (n-20)%6 != 0 {
        return nil, os.NewError("read chunk: invalid length: " + strconv.Itoa(n))
    }
    info = new(Chunk)
    r := bytes.NewBuffer(ans)
    read(r, &info.length, &info.id, &info.version)
    info.csdata = ans[20:]
    return info, nil
}

func (mc *MasterConn) WriteChunk(inode,indx uint32) (*Chunk, os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_WRITE_CHUNK, inode, indx)
    if err != nil {
        return nil, err
    }
    if len(ans) < 20 || (len(ans)-20)%6 != 0 {
        return nil, os.NewError("invalid length")
    }
    var info Chunk
    r := bytes.NewBuffer(ans)
    read(r, &info.length, &info.id, &info.version)
    info.csdata = ans[20:]
    return &info, nil
}

func (mc *MasterConn) WriteEnd(chunkid uint64, inode uint32, length uint64) os.Error {
    _, err := mc.sendAndReceive(CUTOMA_FUSE_WRITE_CHUNK_END, chunkid, inode, length)
    return err
}

type MasterMetaConn struct {
    MasterConn
}

func NewMasterMetaConn(addr string) *MasterMetaConn {
    if !strings.Contains(addr, ":") {
        addr += ":9421"
    }
    mc := new(MasterMetaConn)
    mc.addr = addr
    return mc
}

// connect as meta
func (mc *MasterMetaConn) Connect() (err os.Error) {
    return mc.MasterConn.Connect()
}

func (mc *MasterMetaConn) GetReserved() ([]byte, os.Error) {
    return mc.sendAndReceive(CUTOMA_FUSE_GETRESERVED)
}

func (mc *MasterMetaConn) GetTrash() (map[uint32]string, os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_GETTRASH)
    if err != nil {
        return nil, err
    }
    rs := make(map[uint32]string)
    var inode uint32
    for len(ans) > 0 {
        kl := int(ans[0])
        if len(ans) < kl + 5 {
            break
        }
        name := string(ans[1:kl+1])
        read(bytes.NewBuffer(ans[1+kl:1+kl+4]), &inode)
        rs[inode] = name
        ans = ans[kl + 5:]
    }
    return rs, nil
}

func (mc *MasterMetaConn) GetDetachedAttr(inode uint32) (attr []byte, err os.Error) {
    return mc.sendAndReceive(CUTOMA_FUSE_GETDETACHEDATTR, inode)
}

func (mc *MasterMetaConn) GetTrashPath(inode uint32) (string, os.Error) {
    ans, err := mc.sendAndReceive(CUTOMA_FUSE_GETTRASHPATH, inode)
    if err != nil {
        return "", err
    }
    var l uint32
    read(bytes.NewBuffer(ans[:4]), &l)
    if len(ans) != int(l+4) {
        return "", os.NewError("length not match")
    }
    return string(ans[4:]), nil
}

func (mc *MasterMetaConn) SetTrashPath(inode uint32, path string) os.Error {
    _, err := mc.sendAndReceive(CUTOMA_FUSE_SETTRASHPATH, inode, uint32(len(path)+1), path, "\000")
    return err
}

func (mc *MasterMetaConn) Undel(inode uint32) os.Error {
    _, err := mc.sendAndReceive(CUTOMA_FUSE_UNDEL, inode)
    return err
}

func (mc *MasterMetaConn) Purge(inode uint32) os.Error {
    _, err := mc.sendAndReceive(CUTOMA_FUSE_PURGE, inode)
    return err
}
