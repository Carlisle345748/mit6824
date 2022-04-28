package shardctrler

//func TestJoin(t *testing.T) {
//	cfg := Config{
//		Num:    0,
//		Shards: [10]int{},
//		Groups: map[int][]string{},
//	}
//	for i := 1; i <= 20; i++ {
//		srv := map[int][]string{
//			i: {"x", "y", "z"},
//		}
//		cfg = cfg.Join(srv)
//		fmt.Println(cfg)
//	}
//
//	fmt.Println("#############")
//
//	for i := 20; i >= 1; i-- {
//		cfg = cfg.Leave([]int{i})
//		fmt.Println(cfg)
//	}
//}
