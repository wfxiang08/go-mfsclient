all: mfsserver

include $(GOROOT)/src/Make.inc

TARG=moosefs
GOFILES=moosefs.go consts.go mastercomm.go cscomm.go utils.go csdb.go\

include $(GOROOT)/src/Make.pkg

mfsserver: _obj/moosefs.a mfsserver.go
	$(GC) mfsserver.go
	$(LD) -o mfsserver mfsserver.$(O)

