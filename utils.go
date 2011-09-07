package moosefs

import (
    "io"
    "os"
    "bytes"
    "encoding/binary"
    "reflect"
)

type Error byte

func (e Error) String() string {
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

func attrToFileInfo(inode uint32, attr []byte) *os.FileInfo {
    if len(attr) != 35 {
        panic("invalid length")
    }
    fi := new(os.FileInfo)
    r := bytes.NewBuffer(attr)
    var type_ uint8
    var mode uint16
    var uid, gid, atime, mtime, ctime, nlink uint32
    read(r, &type_, &mode, &uid, &gid, &atime, &mtime, &ctime, &nlink)

    fi.Ino = uint64(inode)
    fi.Nlink = uint64(nlink)
    fi.Mode = uint32(mode)
    fi.Uid = int(uid)
    fi.Gid = int(gid)
    fi.Atime_ns = int64(atime) * 1e9
    fi.Mtime_ns = int64(mtime) * 1e9
    fi.Ctime_ns = int64(ctime) * 1e9

    fi.Size = 0
    fi.Blocks = 0
    fi.Blksize = 0x10000
    switch type_ {
    case TYPE_DIRECTORY, TYPE_SYMLINK, TYPE_FILE:
        switch type_ {
        case TYPE_DIRECTORY:
            fi.Mode = uint32(mode&07777) | S_IFDIR
        case TYPE_SYMLINK:
            fi.Mode = uint32(mode&07777) | S_IFLNK
        case TYPE_FILE:
            fi.Mode = uint32(mode&07777) | S_IFREG
        }
        var length int64
        read(r, &length)
        fi.Size = length
        fi.Blocks = (length + 511) / 512
    case TYPE_FIFO:
        fi.Mode = uint32(mode&07777) | S_IFIFO
    case TYPE_SOCKET:
        fi.Mode = uint32(mode&07777) | S_IFSOCK
    case TYPE_BLOCKDEV:
        fi.Mode = uint32(mode&07777) | S_IFBLK
        var rdev uint32
        read(r, &rdev)
        fi.Rdev = uint64(rdev)
    case TYPE_CHARDEV:
        fi.Mode = uint32(mode&07777) | S_IFCHR
        var rdev uint32
        read(r, &rdev)
        fi.Rdev = uint64(rdev)
    default:
        fi.Mode = 0
    }
    return fi
}

func newFileInfo(name string, inode uint32, attr []byte) *os.FileInfo {
    fi := attrToFileInfo(inode, attr)
    fi.Name = name
    return fi
}
