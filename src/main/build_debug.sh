go build -race -gcflags="all=-N -l" -o mrcoordinator.so mrcoordinator.go
go build -race -gcflags="all=-N -l" -o mrworker.so mrworker.go
go build -race -gcflags="all=-N -l" -buildmode=plugin ../mrapps/wc.go
