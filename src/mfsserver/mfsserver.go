package main

import (
    "flag"
    "fmt"
    "log"
    "moosefs"
    "net/http"
    _ "net/http/pprof"
    "os"
    "runtime/pprof"
    "strings"
    "time"
)

var listen = flag.String("listen", ":9500", "http service address")
var mfsmaster = flag.String("mfsmaster", "mfsmaster", "the listen of mfsmaster")
var local = flag.String("local", "", "use local dir instead")

//var subdir = flag.String("subdir", "/", "subdir in MFS as root")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var enable_cache = flag.Bool("cache", false, "enable inode cache")

type mooseFS struct {
    client *moosefs.Client
}

func (fs *mooseFS) Open(name string) (http.File, error) {
    return fs.client.Open(name)
}

type mfsServerController struct {
    client *moosefs.Client
}

func (ctrl *mfsServerController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    var pathsegs = strings.SplitN(r.URL.Path, "/", 4)
    var cmd = pathsegs[2]
    var path = pathsegs[3]
    if cmd == "purge-inode-cache" {
        n, err := ctrl.client.PurgeINodeCache(path)
        if err != nil {
            fmt.Fprintf(w, "Error: %s\n", err)
        } else {
            fmt.Fprintf(w, "Purged %d cache entries\n", n)
        }
    }
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

    var fs http.FileSystem
    if *local != "" {
        fs = http.Dir(*local)
    } else {
        var client = moosefs.NewClient(*mfsmaster, "/", *enable_cache)
        fs = &mooseFS{client}
        mfsserver_ctrl := mfsServerController{client}
        http.Handle("/.mfsserver-ctrl/", &mfsserver_ctrl)
    }
    http.Handle("/", http.FileServer(fs))

    log.Println("Listen on", *listen)
    err := http.ListenAndServe(*listen, nil)
    if err != nil {
        log.Fatal("ListenAndServe:", err)
    }
}
