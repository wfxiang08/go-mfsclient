all: mfsserver

GOPATH:=$(CURDIR)
export GOPATH

fmt:
	gofmt -w -s=true -l -tabs=false -tabwidth=4 src/*/*.go

moosefs: fmt
	go install moosefs

mfsserver: moosefs
	go build mfsserver

test:
	go test moosefs
