rm mr-out*
rm mr-tmp/*
go run -race mrcoordinator.go pg-*.txt
