package moosefs

import (
    "bytes"
    "encoding/binary"
    "io"
    "os"
    "reflect"
    "time"
)

type Error byte

func (e Error) Error() string {
    return mfs_strerror(int(e))
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func write(w io.Writer, data ...interface{}) {
    for _, d := range data {
        binary.Write(w, binary.BigEndian, d)
    }
}

func read(r io.Reader, data ...interface{}) {
    for _, d := range data {
        binary.Read(r, binary.BigEndian, d)
    }
}

func sizeof(d interface{}) int {
    t := reflect.TypeOf(d)
    switch t.Kind() {
    case reflect.String, reflect.Slice:
        return reflect.ValueOf(d).Len()
    }
    return int(t.Size())
}

func pack(cmd uint32, args ...interface{}) []byte {
    size := 0
    for _, arg := range args {
        size += sizeof(arg)
    }
    w := bytes.NewBuffer(nil)
    write(w, cmd, uint32(size))
    for _, arg := range args {
        switch reflect.TypeOf(arg).Kind() {
        case reflect.String:
            w.Write([]byte(arg.(string)))
        case reflect.Slice:
            w.Write(arg.([]byte))
        default:
            write(w, arg)
        }
    }
    return w.Bytes()
}

// A fileStat is the implementation of FileInfo returned by Stat and Lstat.
type fileStat struct {
    inode   uint64
    uid     int
    gid     int
    name    string
    size    int64
    mode    os.FileMode
    aTime   time.Time
    modTime time.Time
    cTime   time.Time
    sys     interface{}
}

func (fs *fileStat) Name() string       { return fs.name }
func (fs *fileStat) Size() int64        { return fs.size }
func (fs *fileStat) Mode() os.FileMode  { return fs.mode }
func (fs *fileStat) ModTime() time.Time { return fs.modTime }
func (fs *fileStat) IsDir() bool        { return fs.mode.IsDir() }
func (fs *fileStat) IsSymlink() bool    { return fs.mode&os.ModeSymlink != 0 }
func (fs *fileStat) Sys() interface{}   { return fs.sys }

func attrToFileInfo(inode uint32, attr []byte) *fileStat {
    if len(attr) != 35 {
        panic("invalid length")
    }
    var fi fileStat
    r := bytes.NewBuffer(attr)
    var type_ uint8
    var mode uint16
    var uid, gid, atime, mtime, ctime, nlink uint32
    read(r, &type_, &mode, &uid, &gid, &atime, &mtime, &ctime, &nlink)

    fi.inode = uint64(inode)
    fi.mode = os.FileMode(mode & 07777)
    fi.uid = int(uid)
    fi.gid = int(gid)
    fi.aTime = time.Unix(int64(atime), 0)
    fi.modTime = time.Unix(int64(mtime), 0)
    fi.cTime = time.Unix(int64(ctime), 0)

    fi.size = 0
    switch type_ {
    case TYPE_DIRECTORY, TYPE_SYMLINK, TYPE_FILE:
        switch type_ {
        case TYPE_DIRECTORY:
            fi.mode |= os.ModeDir
        case TYPE_SYMLINK:
            fi.mode |= os.ModeSymlink
        case TYPE_FILE:
            var length int64
            read(r, &length)
            fi.size = length
        }
    default:
        fi.mode = 0
    }
    return &fi
}

func newFileInfo(name string, inode uint32, attr []byte) *fileStat {
    fi := attrToFileInfo(inode, attr)
    fi.name = name
    return fi
}
