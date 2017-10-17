package main

import "zp_manager"
import "fmt"

func main() {
  op_timeout := 1000
  m1 := zp_manager.ZpNode{IP : "127.0.0.1", Port : 9221}
  m2 := zp_manager.ZpNode{IP : "127.0.0.1", Port : 9222}
  cluster := zp_manager.NewZpCluster(op_timeout, m1, m2)

  cluster.Pull("test")

  suc, _, res := cluster.ListTable()
  if suc {
    for _, v := range *res {
      fmt.Println(v)
    }
  }

  suc, _, master, slaves := cluster.ListMeta()
  if suc {
    fmt.Println(*master)
    for _, v := range *slaves {
      fmt.Println(v)
    }
  }

  suc, _, nodes, status := cluster.ListNode()
  if suc {
    for _, v := range *nodes {
      fmt.Println(v)
    }
    for _, v := range *status {
      fmt.Println(v)
    }
  }

  key := []byte("testkey1")
  value := []byte("testvalue1")
  cluster.Set("test", &key, &value, -1)

  _, _, v := cluster.Get("test", &key)
  str := string(v[:])
  fmt.Println(str)

  keys := make([]string, 1)
  keys = append(keys, "testkey1")
  result := make(map[string]string, 1)
  cluster.Mget("test", keys, &result)
  for k, v := range result {
    fmt.Println(k)
    fmt.Println(v)
  }

  // cluster.CreateTable("test4", 3)
  cluster.Delete("test", "testkey1")

  _, reason, qps, total_query := cluster.InfoQps("test")
  fmt.Println(reason)
  fmt.Printf("qps: %d\n", qps)
  fmt.Printf("total_query: %d\n", total_query)

  views := make(map[int]zp_manager.ZpPartitionView)
  d1 := zp_manager.ZpNode{IP : "127.0.0.1", Port : 13222}
  suc, reason = cluster.InfoRepl(d1, "test", &views)
  if suc {
    for k, v := range views {
      fmt.Printf("%d ", k)
      fmt.Println(v)
    }
  } else {
    fmt.Println(reason)
  }

  space_info := make(map[zp_manager.ZpNode]zp_manager.ZpSpaceInfo)
  suc, reason = cluster.InfoSpace("test", &space_info)
  if suc {
    for k, v := range space_info {
      fmt.Println(k, v)
    }
  } else {
    fmt.Println(reason)
  }

  var state zp_manager.ZpServerState
  suc, reason = cluster.InfoServer(d1, &state)
  if suc {
    fmt.Println(state)
  } else {
    fmt.Println(reason)
  }
}
