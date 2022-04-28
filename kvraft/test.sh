mkdir -p result
for i in {29..100};
 do go test -race > result/log_$i.txt
done
