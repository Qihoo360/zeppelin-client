package main

import "zp_manager"
import "fmt"

func main() {
  op_timeout := 1000
  m1 := zp_manager.Node{IP : "127.0.0.1", Port : 9221}
  m2 := zp_manager.Node{IP : "127.0.0.1", Port : 9222}
  cluster := zp_manager.NewZpCluster(op_timeout, m1, m2)

  cluster.Pull("test")

  res := cluster.ListTable()
  for _, v := range *res {
    fmt.Println(v)
  }

  master, slaves := cluster.ListMeta()
  fmt.Println(*master)
  for _, v := range *slaves {
    fmt.Println(v)
  }

  nodes, status := cluster.ListNode()
  if nodes != nil {
    for _, v := range *nodes {
      fmt.Println(v)
    }
    for _, v := range *status {
      fmt.Println(v)
    }
  }

  cluster.CreateTable("test4", 3)

}
