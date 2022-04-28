mkdir -p results
for i in {36..100};
 do go test -race > results/log_$i.txt
done
