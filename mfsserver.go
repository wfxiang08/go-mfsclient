package main

import (
    "./_obj/moosefs"
    "http"
    "flag"
    "os"
    "log"
)

var addr = flag.String("addr", ":1718", "http service address")
var mfsmaster = flag.String("mfsmaster", "mfsmaster", "the addr of mfsmaster")
var subdir = flag.String("subdir", "/", "subdir in MFS as root")

type mooseFS struct {
    client *moosefs.Client
}

func (fs *mooseFS) Open(name string) (http.File, os.Error) {
    f, err := fs.client.Open(name)
    log.Println(name, f, err)
    return f, err
}

func main() {
    flag.Parse()

    fs := mooseFS{moosefs.NewClient(*mfsmaster, *subdir)}
    http.Handle("/", http.FileServer(&fs))

    err := http.ListenAndServe(*addr, nil)
    if err != nil {
        log.Fatal("ListenAndServe:", err)
    }
}
