mkdir -p results
for i in {0..100};
 do go test -race > results/log_$i.txt
    echo $i
done
