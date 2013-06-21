package moosefs

import (
    "errors"
    "io"
    "os"
    "path"
    "strings"
    "sync"
    "sync/atomic"
    "time"
)

const MASTER_CONNS = 4

type Client struct {
    mcs []*MasterConn
    idx uint64

    enable_cache bool
    cache_mutex  sync.Mutex //
    inode_cache  map[uint32]map[string]*fileStat

    cwd        string
    curr_inode uint32
}

type File struct {
    path  string
    inode uint32
    info  *fileStat

    client *Client

    offset int64
    rbuf   []byte
    roff   int64
    woff   int64
    wbuf   []byte

    dircache     []*fileStat
    dirnamecache []string
    cscache      map[uint64]*Chunk
}

func NewClient(addr, subdir string, enable_cache bool) (c *Client) {
    c = &Client{}
    c.mcs = make([]*MasterConn, MASTER_CONNS)
    for i := 0; i < MASTER_CONNS; i++ {
        c.mcs[i] = NewMasterConn(addr, subdir)
    }
    c.enable_cache = enable_cache
    c.inode_cache = make(map[uint32]map[string]*fileStat)
    c.cwd = "/"
    c.curr_inode = MFS_ROOT_ID
    return
}

func (c *Client) Close() {
    for i := 0; i < MASTER_CONNS; i++ {
        c.mcs[i].Close()
    }
    c.mcs = nil
}

func (c *Client) getMasterConn() *MasterConn {
    idx := atomic.AddUint64(&c.idx, 1)
    return c.mcs[idx%MASTER_CONNS]
}

func (c *Client) Create(name string) (*File, error) {
    return c.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (c *Client) Open(name string) (*File, error) {
    return c.OpenFile(name, os.O_RDONLY, 0)
}

func (c *Client) lookup_inode(parent uint32, name string) (*fileStat, error) {
    c.cache_mutex.Lock()
    cache, ok := c.inode_cache[parent]
    if !ok {
        cache = make(map[string]*fileStat)
        c.inode_cache[parent] = cache
    }
    fi, ok := cache[name]
    c.cache_mutex.Unlock()

    if !ok {
        inode, attr, err := c.getMasterConn().Lookup(parent, name)
        if err != nil {
            return nil, err
        }
        fi = attrToFileInfo(inode, attr)
        fi.name = name

        if c.enable_cache {
            c.cache_mutex.Lock()
            cache[name] = fi
            c.cache_mutex.Unlock()
        }
    }
    return fi, nil
}

func (c *Client) lookup(name string, followSymlink bool) (fi *fileStat, parent uint32, err error) {
    parent = c.curr_inode
    ss := strings.Split(name, "/")
    if ss[0] == "" {
        parent = MFS_ROOT_ID
    }
    for i, n := range ss {
        if len(n) == 0 {
            continue
        }
        fi, err = c.lookup_inode(parent, n)
        if err != nil {
            return nil, parent, err
        }
        if fi.IsSymlink() && (i < len(ss)-1 || followSymlink) {
            target, err := c.getMasterConn().ReadLink(uint32(fi.inode))
            if err != nil {
                return nil, parent, errors.New("read link: " + err.Error())
            }
            if !strings.HasPrefix(target, "/") {
                target = path.Join(strings.Join(ss[:i], "/"), target)
            }
            fi, _, err = c.lookup(target, true)
            if err != nil {
                return nil, parent, errors.New("follow :" + target + err.Error())
            }
        }
        parent = uint32(fi.inode)
    }
    if parent == MFS_ROOT_ID {
        fi, err = c.getMasterConn().GetAttr(parent)
        if err != nil {
            return nil, parent, err
        }
    }
    return fi, parent, nil
}

func (c *Client) OpenFile(name string, flag int, perm os.FileMode) (file *File, err error) {
    fi, parent, err := c.lookup(name, true)
    if err != nil {
        if e, ok := err.(Error); ok && e == Error(ERROR_ENOENT) {
            if flag&os.O_CREATE > 0 {
                if !strings.HasPrefix(name, "/") {
                    _, name = path.Split(name)
                }
                fi, err = c.getMasterConn().Mknod(parent, name, TYPE_FILE, uint16(perm), 0)
                if err != nil {
                    return nil, errors.New("mknod failed: " + err.Error())
                }
            } else {
                return nil, err
            }
        } else {
            return nil, errors.New("lookup failed: " + err.Error())
        }
    } else {
        if (flag & os.O_TRUNC) > 0 {
            fi, err = c.getMasterConn().Truncate(uint32(fi.inode), 0, 0)
            if err != nil {
                return nil, errors.New("truncate failed: " + err.Error())
            }
        }
    }

    /*
       if !fi.IsDirectory() {
           f := uint8(WANT_READ)
           if flag & os.O_WRONLY > 0 {
               f = WANT_WRITE
           } else if flag & os.O_RDWR > 0 {
               f = WANT_READ | WANT_WRITE
           }

           _, err := c.getMasterConn().OpenCheck(uint32(fi.inode), f)
           if err != nil {
               return nil, err
           }
       }*/

    file = &File{}
    file.path = name
    file.inode = uint32(fi.inode)
    file.cscache = make(map[uint64]*Chunk)
    file.client = c
    file.info = fi
    return file, nil
}

func (c *Client) Link(oldname, newname string) error {
    fi, _, err := c.lookup(oldname, true)
    if err != nil {
        return errors.New(oldname + " not exists")
    }
    _, _, err = c.getMasterConn().Link(uint32(fi.inode), c.curr_inode, newname)
    return err
}

func (c *Client) getParent(name string) (uint32, string, error) {
    parent_inode := c.curr_inode
    var dir string
    dir, name = path.Split(name)
    if dir != "" {
        var err error
        fi, _, err := c.lookup(dir, true)
        if err != nil {
            return 0, name, err
        }
        parent_inode = uint32(fi.inode)
    }
    return parent_inode, name, nil
}

func (c *Client) Mkdir(name string, perm os.FileMode) (err error) {
    parent_inode, name, err := c.getParent(name)
    if err != nil {
        return err
    }
    _, err = c.getMasterConn().Mkdir(parent_inode, name, uint16(perm))
    return err
}

func (c *Client) MkdirAll(name string, perm os.FileMode) error {
    return errors.New("no impl")
}

func (c *Client) Remove(name string) error {
    parent_inode, name, err := c.getParent(name)
    if err != nil {
        return err
    }
    return c.getMasterConn().Unlink(parent_inode, name)
}

func (c *Client) Rmdir(name string) error {
    parent_inode, name, err := c.getParent(name)
    if err != nil {
        return err
    }
    return c.getMasterConn().Rmdir(parent_inode, name)
}

func (c *Client) RemoveAll(path string) error {
    return errors.New("no impl")
}

func (c *Client) Rename(oldname, newname string) error {
    parent_inode1, name1, err := c.getParent(oldname)
    if err != nil {
        return err
    }
    parent_inode2, name2, err := c.getParent(newname)
    if err != nil {
        return err
    }
    return c.getMasterConn().Rename(parent_inode1, name1, parent_inode2, name2)
}

func (c *Client) Symlink(oldname, newname string) error {
    parent_inode, name, err := c.getParent(newname)
    if err != nil {
        return err
    }
    _, err = c.getMasterConn().Symlink(parent_inode, name, oldname)
    return err
}

func (c *Client) Truncate(name string, size int64) error {
    _, err := c.OpenFile(name, os.O_TRUNC, 0555)
    return err
}

func (c *Client) Stat(name string) (os.FileInfo, error) {
    f, err := c.Open(name)
    if err != nil {
        return nil, err
    }
    return f.Stat()
}

func (c *Client) Lstat(name string) (os.FileInfo, error) {
    fi, _, err := c.lookup(name, false)
    if err != nil {
        return nil, err
    }
    return fi, nil
}

func (c *Client) Readlink(name string) (string, error) {
    return "", errors.New("no impl")
}

func (c *Client) Chmod(name string, mode os.FileMode) error {
    // TODO mc.SetAttr()
    return nil
}

func (c *Client) Chown(name string, uid, gid int) error {
    // TODO mc.SetAttr()
    return nil
}

func (c *Client) Lchown(name string, uid, gid int) error {
    return errors.New("no impl")
}

func (c *Client) Chtimes(name string, atime, mtime time.Time) error {
    // TODO mc.SetAttr()
    return nil
}

func (c *Client) Getwd() (string, error) {
    return c.cwd, nil
}

func (c *Client) Chdir(dir string) error {
    fi, _, err := c.lookup(dir, true)
    if err != nil {
        return err
    }
    if strings.HasPrefix(dir, "/") {
        c.cwd = dir
    } else {
        c.cwd = path.Join(c.cwd, dir)
    }
    c.curr_inode = uint32(fi.inode)
    return nil
}

func (c *Client) purge_inode_cache_in_dir(fi *fileStat) int {
    n := 0
    if cache, exists := c.inode_cache[uint32(fi.inode)]; exists {
        for _, fi := range cache {
            n += c.purge_inode_cache_in_dir(fi)
        }
        delete(c.inode_cache, uint32(fi.inode))
        n++
    }
    return n
}

func (c *Client) PurgeINodeCache(path string) (n_purged int, err error) {
    fi, parent, err := c.lookup(path, true)
    if err != nil {
        return
    }

    c.cache_mutex.Lock()

    n_purged = c.purge_inode_cache_in_dir(fi)
    if _, exists := c.inode_cache[parent]; exists {
        delete(c.inode_cache[parent], fi.name)
        n_purged++
    }

    c.cache_mutex.Unlock()

    return
}

// File

func (f *File) Close() error {
    if len(f.wbuf) > 0 {
        return f.Sync()
    }
    f.offset = 0
    f.rbuf = nil
    f.wbuf = nil
    return nil
}

func (f *File) Path() string {
    return f.path
}

func (f *File) Len() int64 {
    fi, err := f.Stat()
    if err != nil {
        return 0
    }
    return fi.Size()
}

func (f *File) Read(b []byte) (n int, err error) {
    got := 0
    for got < len(b) {
        if f.offset >= f.roff && f.offset < f.roff+int64(len(f.rbuf)) {
            n := min(len(b)-got, int(f.roff+int64(len(f.rbuf))-f.offset))
            copy(b[got:got+n], f.rbuf[f.offset-f.roff:f.offset-f.roff+int64(n)])
            f.offset += int64(n)
            got += n
            if got == len(b) {
                return got, nil
            }
        }

        f.roff = f.offset
        left := f.Len() - f.offset
        rsize := int64(4 * 1024 * 1024)
        if left < rsize {
            rsize = left
        }
        if len(f.rbuf) < int(rsize) {
            f.rbuf = make([]byte, rsize)
        } else {
            f.rbuf = f.rbuf[:rsize]
        }
        n, err := f.ReadAt(f.rbuf, uint64(f.offset))
        if n == 0 {
            return got, err
        }
        f.rbuf = f.rbuf[:n]
    }
    panic("should not here")
    return
}

const CHUNK_SIZE = 64 * 1024 * 1024

func (f *File) ReadAt(b []byte, offset uint64) (n int, err error) {
    got := 0
    for {
        indx := offset / CHUNK_SIZE
        off := offset % CHUNK_SIZE

        info, ok := f.cscache[indx]
        if !ok {
            info, err = f.client.getMasterConn().ReadChunk(f.inode, uint32(indx))
            if err != nil {
                return got, err
            }
            f.cscache[indx] = info
        }

        size := min(len(b)-got, int(info.length-off))
        if size <= 0 {
            return got, io.EOF
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
            return got, io.EOF
        }
    }
    return got, nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
    switch whence {
    case os.SEEK_SET:
        f.offset = offset
    case os.SEEK_CUR:
        f.offset += offset
    case os.SEEK_END:
        f.offset = f.info.Size() + offset
    }
    return f.offset, nil
}

func (f *File) Stat() (fi os.FileInfo, err error) {
    if f.info != nil {
        return f.info, nil
    }
    f.info, err = f.client.getMasterConn().GetAttr(f.inode)
    return f.info, err
}

func (f *File) Readdir(count int) (fis []os.FileInfo, err error) {
    fi := f.dircache
    if fi == nil {
        fi, err = f.client.getMasterConn().GetDirPlus(f.inode)
        if err != nil {
            return nil, err
        }
        f.dircache = fi
    }

    if len(fi) < int(f.offset) {
        return nil, nil
    }
    fi = fi[f.offset:]
    if count > 0 && len(fi) > count {
        fi = fi[:count]
    }
    f.offset += int64(len(fi))
    for _, i := range fi {
        fis = append(fis, i)
    }
    return
}

func (f *File) Readdirnames(count int) (names []string, err error) {
    if f.dirnamecache == nil {
        names, err = f.client.getMasterConn().GetDir(f.inode)
        if err != nil {
            return nil, err
        }
        f.dirnamecache = names
    } else {
        names = f.dirnamecache
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

func (f *File) Chmod(mode uint32) error {
    // TODO
    return errors.New("Not Impl")
}

func (f *File) Sync() error {
    for len(f.wbuf) > 0 {
        chindx := f.woff >> 26
        info, err := f.client.getMasterConn().WriteChunk(f.inode, uint32(chindx))
        if err != nil {
            return errors.New("write chunk failed:" + err.Error())
        }
        off := f.woff & 0x3ffffff
        size := min(len(f.wbuf), int(1<<26-off))
        _, err = info.Write(f.wbuf[:size], uint32(off))
        if err != nil {
            return errors.New("write data to chunk server: " + err.Error())
        }

        length := off + int64(size)
        err = f.client.getMasterConn().WriteEnd(info.id, f.inode, uint64(length))
        if err != nil {
            return errors.New("write end to ms: " + err.Error())
        }
        delete(f.cscache, info.id)
        f.wbuf = f.wbuf[size:]
        f.woff += int64(size)
    }
    return nil
}

func (f *File) Truncate(size int64) error {
    _, err := f.client.getMasterConn().Truncate(f.inode, 1, size)
    f.woff = 0
    f.wbuf = nil
    return err
}

func (f *File) needSync() bool {
    return len(f.wbuf) > 1024*1024
}

func (f *File) Write(b []byte) (int, error) {
    f.wbuf = append(f.wbuf, b...)
    f.offset += int64(len(b))

    if f.needSync() {
        if err := f.Sync(); err != nil {
            return 0, err
        }
    }

    return len(b), nil
}

func (f *File) WriteAt(b []byte, off int64) (int, error) {
    if off < f.woff || off > f.woff+int64(len(f.wbuf)) {
        if e := f.Sync(); e != nil {
            return 0, e
        }
        f.woff = off
    }

    return f.Write(b)
}

func (f *File) WriteString(s string) (int, error) {
    return f.Write([]byte(s))
}

var client *Client

func init() {
    Init("mfsmaster", false)
}

func Init(master string, enable_cache bool) {
    client = NewClient(master, "/", enable_cache)
}

func Create(path string) (*File, error) {
    return client.Create(path)
}

func Open(path string) (*File, error) {
    return client.Open(path)
}

func OpenFile(path string, flag int, perm os.FileMode) (*File, error) {
    return client.OpenFile(path, flag, perm)
}

func Lstat(path string) (os.FileInfo, error) {
    return client.Lstat(path)
}

func Stat(path string) (os.FileInfo, error) {
    return client.Stat(path)
}

func Readlink(name string) (string, error) {
    return client.Readlink(name)
}

func Link(oldname, newname string) error {
    return client.Link(oldname, newname)
}

func Mkdir(path string, perm os.FileMode) error {
    return client.Mkdir(path, perm)
}

func MkdirAll(path string, perm os.FileMode) error {
    return client.MkdirAll(path, perm)
}

func Truncate(path string, size int64) error {
    return client.Truncate(path, size)
}

func Symlink(oldname, newname string) error {
    return client.Symlink(oldname, newname)
}

func Rename(oldname, newname string) error {
    return client.Rename(oldname, newname)
}

func Remove(name string) error {
    return client.Remove(name)
}

func Rmdir(name string) error {
    return client.Rmdir(name)
}

func RemoveAll(path string) error {
    return client.RemoveAll(path)
}

func Chmod(name string, mode os.FileMode) error {
    return client.Chmod(name, mode)
}

func Chown(name string, uid, gid int) error {
    return client.Chown(name, uid, gid)
}

func Lchown(name string, uid, gid int) error {
    return client.Lchown(name, uid, gid)
}

func Chtimes(name string, atime, mtime time.Time) error {
    return client.Chtimes(name, atime, mtime)
}

func Getwd() (string, error) {
    return client.Getwd()
}

func Chdir(dir string) error {
    return client.Chdir(dir)
}

func PurgeINodeCache(path string) (n_purged int, err error) {
    return client.PurgeINodeCache(path)
}
