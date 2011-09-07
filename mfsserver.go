package main

import (
    "./_obj/moosefs"
    "http"
    "flag"
    "os"
    "log"
    "time"
    "runtime/pprof"
)

var addr = flag.String("addr", ":9500", "http service address")
var mfsmaster = flag.String("mfsmaster", "mfsmaster", "the addr of mfsmaster")
//var subdir = flag.String("subdir", "/", "subdir in MFS as root")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

type mooseFS struct {
    client *moosefs.Client
}

func (fs *mooseFS) Open(name string) (http.File, os.Error) {
    f, err := fs.client.Open(name)
    //    status := 200
    //    if err != nil {
    //        status = 404
    //    }
    //    log.Println(name, status)
    return f, err
}

func main() {
    flag.Parse()

    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)

        go func() {
            time.Sleep(10e9)
            pprof.StopCPUProfile()
        }()
    }

    fs := &mooseFS{moosefs.NewClient(*mfsmaster, "/")}
    //fs := http.Dir("/mfs")
    http.Handle("/", http.FileServer(fs))

    log.Println("Listen on", *addr)
    err := http.ListenAndServe(*addr, nil)
    if err != nil {
        log.Fatal("ListenAndServe:", err)
    }
}
