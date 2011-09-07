
package moosefs

import "testing"

const testname = "test123"

func TestMasterConn(t *testing.T) {
    mc := NewMasterConn("localhost", "/")
    e := mc.Connect()
    if e != nil {
        t.Error("fs_connect failed", e.String())
        t.FailNow()
    } else {
        mc.Close()
    }

    // re-connect
    e = mc.Connect()
    if e != nil || mc.conn == nil {
        t.Error("reconnect failed", e.String())
        t.FailNow()
    }

    if attr, err := mc.GetAttr(MFS_ROOT_ID); err != nil {
        t.Error("get attr faile", err.String())
    }else if !attr.IsDirectory() {
        t.Error("invalid attr length", *attr)
    }else{
        //t.Log("attr of /", *attr)
    }

//    if _, err := mc.OpenCheck(MFS_ROOT_ID, WANT_READ); err != nil {
  //      t.Error("open check /", err.String())
    //}

    if _, err := mc.GetDir(MFS_ROOT_ID); err != nil {
        t.Error("getdir() failed", err.String())
    }else{
        //t.Log("dir of /", info)
    }
    if _, err := mc.GetDirPlus(MFS_ROOT_ID); err != nil {
        t.Error("getdir() failed", err.String())
    }else{
        //t.Log("dir of /", info)
    }

    if stat, err := mc.StatFS(); err != nil || stat.inodes == 0 {
        t.Error("stats failed", err.String())
    }else{
        //t.Log("stat info", stat) 
    }

    if ino, _, err := mc.Lookup(MFS_ROOT_ID, "shou_not_exists"); 
        err == nil || err.(Error) != Error(ERROR_ENOENT) {
        t.Error("not exists file with error", err.String(), ino)
    }

    mc.Unlink(MFS_ROOT_ID, testname)

    fi, err := mc.Mknod(MFS_ROOT_ID, testname, TYPE_FILE, 0555, 1)
    inode := uint32(fi.Ino)
    if err != nil {
        t.Error("mknod fail", err.String())
        return
    }
    inode2, _, err2 := mc.Lookup(MFS_ROOT_ID, testname)
    if err2 != nil {
        t.Error("lookup failed")
        return
    }
    if inode2 != inode {
        t.Error("lookup inode not match", inode, inode2)
        return
    }
  
    if e := mc.Access(inode, WANT_READ); e != nil {
        t.Error("access failed", e.String())
    }
    _, err = mc.OpenCheck(inode, WANT_READ)
    if err != nil {
        t.Error("opencheck fail", err.String())
    }
    
    info, err := mc.WriteChunk(inode, 0)
    if err != nil || info == nil {
        t.Error("write chunk fail", err.String())
    }else{
        if e := mc.WriteEnd(info.id, inode, 5); e != nil {
            t.Error("write end failed", e.String())
        }
        info2, err := mc.ReadChunk(inode, 0)
        if err != nil || info2 == nil {
            t.Error("read chunk failed", err.String())
        }else if info2.id != info.id || info2.length != 5 || info.version != 1 {
            t.Error("info wrong", info2.id, info2.length, info2.version)
        }
    }

    if fi, err := mc.GetAttr(inode); err != nil {
        t.Error("get attr", err.String())
    }else {
        if fi, err := mc.SetAttr(inode, SET_UID_FLAG | SET_GID_FLAG, uint16(fi.Mode), 1, 1, 
            uint32(fi.Atime_ns/1e9), uint32(fi.Mtime_ns/1e9)); err != nil {
            t.Error("set attr err", err.String())
        } else if fi.Uid != 1 || fi.Gid != 1 {
            t.Error("set attr fail", *fi)
        }
    }

    // symlink
    mc.Unlink(MFS_ROOT_ID, "test_link")
    if fi, err := mc.Symlink(MFS_ROOT_ID, "test_link", "test123"); err != nil {
        t.Error("symlink", err.String())
    } else {
        if path, err := mc.ReadLink(uint32(fi.Ino)); err != nil {
            t.Error("readlink", err.String())
        }else if path != "test123" {
            t.Error("link error", path, []byte(path))
        }
    }

    if fi, err := mc.Truncate(inode, 1, 3); err != nil {
        t.Error("truncate fail", err.String())
    }else if fi.Size != 3 {
        t.Error("truncate fail", fi.Size)
    }

    newtestname := testname+"_new"
    if err := mc.Rename(MFS_ROOT_ID, testname, MFS_ROOT_ID, newtestname); err != nil {
        t.Error("rename", err.String())
    }else{
        if _, _, err := mc.Lookup(MFS_ROOT_ID, newtestname); err != nil {
            t.Error("rename failed", err.String())
        }
    }

    if e := mc.Unlink(MFS_ROOT_ID, newtestname); e != nil {
        t.Error("unlink err", e.String())
    }else{
        if _, _, err := mc.Lookup(MFS_ROOT_ID, newtestname); err == nil {
            t.Error("unlink failed")
        }
    }

    mc.Rmdir(MFS_ROOT_ID, "testdir")
    if fi, err := mc.Mkdir(MFS_ROOT_ID, "testdir", 0777); err != nil {
        t.Error("mkdir", err.String())
    }else if !fi.IsDirectory() {
        t.Error("mkdir failed")
    }else{
        if err := mc.Rmdir(MFS_ROOT_ID, "testdir"); err != nil {
            t.Error("rmdir", err.String())
        }
    }

    mc.Close()
}
