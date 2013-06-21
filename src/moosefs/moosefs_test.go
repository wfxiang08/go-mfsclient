package moosefs

import (
    "io"
    "os"
    "testing"
)

func TestAPI(t *testing.T) {
    if err := Chdir("/"); err != nil {
        t.Error("chdir fail")
    }
    if cwd, err := Getwd(); err != nil || cwd != "/" {
        t.Error("getwd failed")
    }

    fn := "test67"
    f, err := Create(fn)
    if err != nil {
        t.Error("create failed", err.Error())
        return
    }
    n, err := f.WriteString("hello")
    if err != nil || n != 5 {
        t.Error("write faile", err.Error())
    }
    err = f.Sync()
    if err != nil {
        t.Error("sync fail", err.Error())
    }
    err = f.Close()
    if err != nil {
        t.Error("close fail", err.Error())
        return
    }

    f, err = Open("/test67")
    if err != nil {
        t.Error("open fail", err.Error())
    }
    fi, err := f.Stat()
    if err != nil {
        t.Error("stat fail", err.Error())
    } else {
        t.Log("stat info", *fi.(*fileStat))
    }

    b := make([]byte, 1024)
    n, err = f.Read(b)
    if err != nil && err != io.EOF {
        t.Error("read fail", err.Error())
    }
    if n != 5 || string(b[:n]) != "hello" {
        t.Error("content error", n, b[:n])
    }
    f.Close()

    fn2 := "test76"
    Remove(fn2)
    err = Symlink("/test67", fn2)
    if err != nil {
        t.Error("symlink failed", err.Error())
    }
    if fi, err = Lstat(fn2); err != nil {
        t.Error("stat fail", err.Error())
    } else {
        t.Log("stat of file: ", *fi.(*fileStat))
    }
    if fi, err = Stat(fn2); err != nil {
        t.Error("stat fail", err.Error())
    } else {
        t.Log("stat of symlink: ", *fi.(*fileStat))
    }
}

func TestPurgeINodeCache(t *testing.T) {
    dir := "go-mfsclient-testing"
    if err := Mkdir(dir, 0777); err != nil {
        t.Error("mkdir failed", err.Error())
    }

    // create an empty file
    path := "go-mfsclient-testing/purge-inode-test"
    f, err := Create(path)
    if err != nil {
        t.Error("create file failed", err.Error())
    }
    f.Close()

    // initialize another client to get stats
    client := NewClient("mfsmaster", "/", true)

    // assert file size == 0
    fi, err := client.Stat(path)
    if err != nil {
        t.Error("stat file failed", err.Error())
    }
    if fi.Size() != 0 {
        t.Error("assert size failed", fi.Size())
    }

    // write something to the file
    f, err = OpenFile(path, os.O_RDWR, 0666)
    if err != nil {
        t.Error("open file failed", err.Error())
    }
    n, err := f.WriteString("hello")
    if err != nil || n != 5 {
        t.Error("write faile", err.Error())
    }
    f.Close()

    // assert file size is still 0 due to the cache
    fi, err = client.Stat(path)
    if err != nil {
        t.Error("stat file failed", err.Error())
    }
    if fi.Size() != 0 {
        t.Error("assert size failed", fi.Size())
    }

    // purge the cache
    n_purged, err := client.PurgeINodeCache(dir)
    if err != nil || n_purged <= 0 {
        t.Error("purge failed", err.Error())
    }

    // assert file size is up to date
    fi, err = client.Stat(path)
    if err != nil {
        t.Error("stat file failed", err.Error())
    }
    if fi.Size() != 5 {
        t.Error("assert size failed", fi.Size())
    }

    err = Remove(path)
    if err != nil {
        t.Error("remove failed", err.Error())
    }
    err = Rmdir(dir)
    if err != nil {
        t.Error("remove failed", err.Error())
    }
}

func TestDisableCache(t *testing.T) {
    dir := "go-mfsclient-testing"
    if err := Mkdir(dir, 0777); err != nil {
        t.Error("mkdir failed", err.Error())
    }

    // create an empty file
    path := "go-mfsclient-testing/disable-cache-test"
    f, err := Create(path)
    if err != nil {
        t.Error("create file failed", err.Error())
    }
    f.Close()

    // initialize another client (cache disabled) to get stats
    client := NewClient("mfsmaster", "/", false)

    // assert file size == 0
    fi, err := client.Stat(path)
    if err != nil {
        t.Error("stat file failed", err.Error())
    }
    if fi.Size() != 0 {
        t.Error("assert size failed", fi.Size())
    }

    // write something to the file
    f, err = OpenFile(path, os.O_RDWR, 0666)
    if err != nil {
        t.Error("open file failed", err.Error())
    }
    n, err := f.WriteString("hello")
    if err != nil || n != 5 {
        t.Error("write faile", err.Error())
    }
    f.Close()

    // assert file size is up to date
    fi, err = client.Stat(path)
    if err != nil {
        t.Error("stat file failed", err.Error())
    }
    if fi.Size() != 5 {
        t.Error("assert size failed", fi.Size())
    }

    err = Remove(path)
    if err != nil {
        t.Error("remove failed", err.Error())
    }
    err = Rmdir(dir)
    if err != nil {
        t.Error("remove failed", err.Error())
    }
}
