
package moosefs

import (
    "testing"
    "os"
)

func TestAPI(t *testing.T) {
    if err:=Chdir("/"); err != nil {
        t.Error("chdir fail")
    }
    if cwd, err := Getwd(); err != nil || cwd != "/" {
        t.Error("getwd failed")
    }

    fn := "test67"
    f, err := Create(fn)
    if err != nil {
        t.Error("create failed", err.String())
        return
    }
    n, err := f.WriteString("hello")
    if err != nil || n != 5 {
        t.Error("write faile", err.String())
    }
    err = f.Sync()
    if err != nil {
        t.Error("sync fail",err.String())
    }
    err = f.Close()
    if err != nil {
        t.Error("close fail",err.String())
        return
    }

    f, err = Open("/test67")
    if err != nil {
        t.Error("open fail", err.String())
    }
    fi, err := f.Stat()
    if err != nil {
        t.Error("stat fail", err.String())
    }else {
        t.Log("stat info", *fi)
    }

    b := make([]byte, 1024)
    n, err = f.Read(b)
    if err != nil && err != os.EOF {
        t.Error("read fail", err.String())
    }
    if n!=5 || string(b[:n]) != "hello" {
        t.Error("content error", n, b[:n])
    }
    f.Close()

    fn2 := "test76"
    Remove(fn2)
    err = Symlink("/test67", fn2) 
    if err != nil {
        t.Error("symlink failed", err.String())
    }
    if fi, err = Lstat(fn2); err != nil {
        t.Error("stat fail", err.String())
    }else{
        t.Log("stat of file: ", *fi)
    }
    if fi, err = Stat(fn2); err != nil {
        t.Error("stat fail", err.String())
    }else{
        t.Log("stat of symlink: ", *fi)
    }
}
