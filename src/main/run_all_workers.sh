for i in {1..10}
do
    #echo $i
    go run -race mrworker.go wc.so &
done
