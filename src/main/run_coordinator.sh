rm mr-out*
rm mr-tmp-*
rm mr-tmp/*
go run -race mrcoordinator.go pg-*.txt
