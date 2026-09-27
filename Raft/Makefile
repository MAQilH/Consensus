# The tester launches each Raft peer as a separate process from main/raft1d,
# so the daemon must be rebuilt after every change to raft1/.

.PHONY: build test test-3A test-3B test-3C test-3D clean

build:
	go build -race -o main/raft1d ./main/raft1d.go

test: build
	cd raft1 && go test -race

test-%: build
	cd raft1 && go test -race -run $*

clean:
	rm -f main/raft1d
