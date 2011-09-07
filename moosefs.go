package moosefs

import (
    "os"
    "strconv"
    "strings"
    "path"
    "sync"
)

type Client struct {
    mc *MasterConn
    sync.Mutex //

    cwd string
    curr_inode uint32
}

type File struct {
    path string
    inode uint32
    info *os.FileInfo

    client *Client
    
    offset int64
    rbuf []byte

    woff  int64
    wbuf []byte
}

func NewClient(addr, subdir string) (c *Client) {
    c = &Client{}
    c.mc = NewMasterConn(addr, subdir)
    c.cwd = "/"
    c.curr_inode = MFS_ROOT_ID 
    return c
}

func (c *Client) Close() {
    c.Lock()
    defer c.Unlock()
    c.mc.Close()
    c.mc = nil
}

func (c *Client) Create(name string) (file *File, err os.Error) {
    return c.OpenFile(name, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
}

func (c *Client) Open(name string) (file *File, err os.Error) {
    return c.OpenFile(name, os.O_RDONLY, 0)
}

func (c *Client) lookup(name string, followSymlink bool) (uint32, os.Error) {
    parent := c.curr_inode
    ss := strings.Split(name, "/")
    if ss[0] == "" {
        parent = MFS_ROOT_ID
    }
    for _,n := range ss {
        if len(n) == 0 {
            continue
        }
        inode, attr, err := c.mc.Lookup(parent, n)
        if err != nil {
            return parent, err
        }
        type_ := attr[0]
        if type_ == TYPE_SYMLINK && followSymlink {
            target, err := c.mc.ReadLink(inode)
            if err != nil {
                return 0, os.NewError("read link: " + err.String())
            }
            // TODO
            inode, err = c.lookup(target, true)
            if err != nil {
                return 0, os.NewError("follow :" + err.String())
            }
        }
        parent = inode
    }
    return parent, nil
}

func (c *Client) OpenFile(name string, flag int, perm uint32) (file *File, err os.Error) {
    inode, err := c.lookup(name, true)
    if err != nil {
        if e,ok := err.(Error); ok && e == Error(ERROR_ENOENT) {
            if flag & os.O_CREATE > 0 {
                if !strings.HasPrefix(name, "/") {
                    _,name = path.Split(name)
                }
                fi, err := c.mc.Mknod(inode, name, TYPE_FILE, uint16(perm), 0)
                //println("create ", inode)
                if err != nil {
                    return nil, os.NewError("mknod failed: " + err.String())
                }
                inode = uint32(fi.Ino)
            }else {
                return nil, os.NewError("not exists")
            }
        }else{
            return nil, os.NewError("lookup failed: " + err.String())
        }
    }else{
        if (flag & os.O_TRUNC) > 0 {
            _, err := c.mc.Truncate(inode, 0, 0)
            if err != nil {
                return nil, os.NewError("truncate failed: " + strconv.Itoa(int(inode)) + err.String())
            }
        }
    }

    /*f := uint8(0)
    if flag & os.O_WRONLY > 0 {
        f = WANT_WRITE
    }else if flag & os.O_RDWR > 0 {
        f = WANT_READ | WANT_WRITE
    }
  
    attr, err := c.mc.OpenCheck(inode, f) 
    if err != nil {
        return nil, err
    }*/
    
    file = &File{}
    file.path = name
    file.inode = inode
    file.client = c
//    file.info = attrToFileInfo(inode, attr)
    return file, nil
}

func (c *Client) Link(oldname, newname string) os.Error {
    old_inode, err := c.lookup(oldname, true)
    if err != nil {
        return os.NewError(oldname + " not exists")
    }
    dir := c.cwd
    name := newname
    if strings.HasPrefix(newname, "/") {
        dir,name = path.Split(newname)
    }
    parent_inode, err := c.lookup(dir, true)
    if err != nil {
        return os.NewError("parent not exists")
    }
    _, _, err = c.mc.Link(old_inode, parent_inode, name)
    return err
}

func (c *Client) getParent(name string) (uint32, string, os.Error) {
    parent_inode := c.curr_inode
    if strings.HasPrefix(name, "/") {
        var dir string
        dir,name = path.Split(name)
        var err os.Error
        parent_inode, err = c.lookup(dir, true)
        if err != nil {
            return 0, name, err
        }
    }
    return parent_inode, name, nil
}

func (c *Client) Mkdir(name string, perm uint32) (err os.Error) {
    parent_inode, name, err := c.getParent(name)
    if err != nil {
        return err
    }
    _, err = c.mc.Mkdir(parent_inode, name, uint16(perm))
    return err
}

func (c *Client) MkdirAll(name string, perm uint32) os.Error {
    return os.NewError("no impl")
}

func (c *Client) Remove(name string) os.Error {
    parent_inode, name, err := c.getParent(name)
    if err != nil {
        return err
    }
    return c.mc.Unlink(parent_inode, name)
}

func (c *Client) RemoveAll(path string) os.Error {return os.NewError("no impl")}

func (c *Client) Rename(oldname, newname string) os.Error {
    parent_inode1, name1, err := c.getParent(oldname)
    if err != nil {
        return err
    }
    parent_inode2, name2, err := c.getParent(newname)
    if err != nil {
        return err
    }
    return c.mc.Rename(parent_inode1, name1, parent_inode2, name2)
}

func (c *Client) Symlink(oldname, newname string) os.Error {
    parent_inode, name, err := c.getParent(newname)
    if err != nil {
        return err
    }
    _, err = c.mc.Symlink(parent_inode, name, oldname)
    return err
}

func (c *Client) Truncate(name string, size int64) os.Error {
    _, err := c.OpenFile(name, os.O_TRUNC, 0555)
    return err
}

func (c *Client) Stat(name string) (fi *os.FileInfo, err os.Error) {
    f, err := c.Open(name)
    if err != nil {
        return nil, err
    }
    return f.Stat()
}

func (c *Client) Lstat(name string) (fi *os.FileInfo, err os.Error) {
    inode, err := c.lookup(name, false)
    if err != nil {
        return nil, err
    }
    return c.mc.GetAttr(inode)
}

func (c *Client) Readlink(name string) (string, os.Error) {
    return "", os.NewError("no impl")
}

func (c *Client) Chmod(name string, mode uint32) os.Error {
    // TODO mc.SetAttr()
    return nil
}

func (c *Client) Chown(name string, uid, gid int) os.Error {
    // TODO mc.SetAttr()
    return nil
}

func (c *Client) Lchown(name string, uid, gid int) os.Error {
    return os.NewError("no impl")
}

func (c *Client) Chtimes(name string, atime_ns, mtime_ns uint64) os.Error {
    // TODO mc.SetAttr()
    return nil
}

func (c *Client) Getwd() (string, os.Error) {
    return c.cwd, nil
}

func (c *Client) Chdir(dir string) os.Error {
    inode, err := c.lookup(dir, true)
    if err != nil {
        return err
    }
    if strings.HasPrefix(dir, "/") {
        c.cwd = dir
    }else{
        c.cwd = path.Join(c.cwd, dir)
    }
    c.curr_inode = inode
    return nil
}

// File

func (f *File) Close() os.Error {
    if len(f.wbuf) > 0 {
        return f.Sync()
    }
    return nil
}

func (f *File) Path() string {
    return f.path
}

func (f *File) Read(b []byte) (n int, err os.Error) {
    n, err = f.ReadAt(b, uint64(f.offset))
    if n > 0 {
        f.offset += int64(n)
    }
    return
}

const CHUNK_SIZE = 64*1024*1024

func (f *File) ReadAt(b []byte, offset uint64) (n int, err os.Error) {
    got := 0
    for {
        indx := offset / CHUNK_SIZE
        off := offset % CHUNK_SIZE
        info, err := f.client.mc.ReadChunk(f.inode, uint32(indx))
        if err != nil {
            return got, err
        }
        size := min(len(b)-got, int(info.length-off))
        if size <= 0 {
            return got, os.EOF
        }

        n, err := info.Read(b[got:got+size], uint32(off))
        got += n
        offset += uint64(n)
        if err != nil {
            return got, err
        }
        if got == len(b) {
            return got, nil
        }
        if offset >= info.length {
            return got, os.EOF
        }
    }
    return got, nil
}

func (f *File) Seek(offset int64, whence int) (ret int64, err os.Error) {
    switch whence {
    case os.SEEK_SET:
        f.offset = offset
    case os.SEEK_CUR:
        f.offset += offset
    case os.SEEK_END:
        f.offset = f.info.Size + offset
    }
    return f.offset, nil
}

func (f *File) Stat() (fi *os.FileInfo, err os.Error) {
    if f.info != nil {
        return f.info, nil
    }
    f.info, err = f.client.mc.GetAttr(f.inode)
    return f.info, err
}

func (f *File) Readdir(count int) (fi []os.FileInfo, err os.Error) {
    fi, err = f.client.mc.GetDirPlus(f.inode)
    if err != nil {
        return nil, err
    }
    if len(fi) < int(f.offset) {
        return nil, nil
    }
    fi = fi[f.offset:]
    if count > 0 && len(fi) > count {
        fi = fi[:count]
    }
    f.offset += int64(len(fi))
    return
}

func (f *File) Readdirnames(count int) (names []string, err os.Error) {
    names, err = f.client.mc.GetDir(f.inode)
    if err != nil {
        return nil, err
    }
    if len(names) < int(f.offset) {
        return nil, nil
    }
    names = names[f.offset:]
    if count > 0 && len(names) > count {
        names = names[:count]
    }
    f.offset += int64(len(names))
    return
}

func (f *File) Chmod(mode uint32) os.Error {
    // TODO
    return nil
}

func (f *File) Sync() (err os.Error) {
    for len(f.wbuf) > 0 {
        chindx := f.woff >> 26
        info, err := f.client.mc.WriteChunk(f.inode, uint32(chindx))
        if err != nil {
            return os.NewError("write chunk failed:"+ err.String())
        }
        off := f.woff & 0x3ffffff
        size := min(len(f.wbuf), int(1<<26-off))
        _, err = info.Write(f.wbuf[:size], uint32(off))
        if err != nil {
            return os.NewError("write data to chunk server: " + err.String())
        }

        length := off + int64(size)
        err = f.client.mc.WriteEnd(info.id, f.inode, uint64(length))
        if err != nil {
            return os.NewError("write end to ms: " + err.String())
        }

        f.wbuf = f.wbuf[size:]
        f.woff += int64(size)
    }
    return nil
}

func (f *File) Truncate(size int64) (err os.Error) {
    _, err =  f.client.mc.Truncate(f.inode, 1, size)
    f.woff = 0
    f.wbuf = nil
    return err
}

func (f *File) needSync() bool {
    return len(f.wbuf) > 1024*1024
}

func (f *File) Write(b []byte) (n int, err os.Error) {
    f.wbuf = append(f.wbuf, b...)
    f.offset += int64(len(b))

    if f.needSync() {
        if err := f.Sync(); err != nil {
            return 0, err
        }
    }

    return len(b), nil
}

func (f *File) WriteAt(b []byte, off int64) (n int, err os.Error) {
    if off < f.woff || off > f.woff + int64(len(f.wbuf)) {
        if e := f.Sync(); e != nil {
            return 0, e
        }
        f.woff = off
    }

    return f.Write(b)
}

func (f *File) WriteString(s string) (n int, err os.Error) {
    return f.Write([]byte(s))
}

var client *Client

func init() {
    Init("mfsmaster")
}

func Init(addr string) {
    client = NewClient(addr, "/")
}

func Create(path string) (file *File, err os.Error) {
    return client.Create(path)
}

func Open(path string) (file *File, err os.Error) {
    return client.Open(path)
}

func OpenFile(path string, flag int, perm uint32) (file *File, err os.Error) {
    return client.OpenFile(path, flag, perm)
}

func Lstat(path string) (fi *os.FileInfo, err os.Error) {
    return client.Lstat(path)
}

func Stat(path string) (fi *os.FileInfo, err os.Error) {
    return client.Stat(path)
}

func Readlink(name string) (string, os.Error) {
    return client.Readlink(name)
}

func Link(oldname, newname string) os.Error {
    return client.Link(oldname, newname)
}

func Mkdir(path string, perm uint32) os.Error {
    return client.Mkdir(path, perm)
}

func MkdirAll(path string, perm uint32) os.Error {
    return client.MkdirAll(path, perm)
}

func Truncate(path string, size int64) os.Error {
    return client.Truncate(path, size)
}

func Symlink(oldname, newname string) os.Error {
    return client.Symlink(oldname, newname)
}

func Rename(oldname, newname string) os.Error {
    return client.Rename(oldname, newname)
}

func Remove(name string) os.Error {
    return client.Remove(name)
}

func RemoveAll(path string) os.Error {
    return client.RemoveAll(path)
}

func Chmod(name string, mode uint32) os.Error {
    return client.Chmod(name, mode)
}

func Chown(name string, uid, gid int) os.Error {
    return client.Chown(name, uid, gid)
}

func Lchown(name string, uid, gid int) os.Error {
    return client.Lchown(name, uid, gid)
}

func Chtimes(name string, atime_ns, mtime_ns uint64) os.Error {
    return client.Chtimes(name, atime_ns, mtime_ns)
}

func Getwd() (string, os.Error) {
    return client.Getwd()
}

func Chdir(dir string) os.Error {
    return client.Chdir(dir)
}
