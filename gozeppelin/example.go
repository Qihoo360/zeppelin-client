package main

import "zeppelin"
import "fmt"

func main() {
  op_timeout := 1000
  m1 := zeppelin.ZpNode{IP : "127.0.0.1", Port : 9221}
  m2 := zeppelin.ZpNode{IP : "127.0.0.1", Port : 9222}
  m3 := zeppelin.ZpNode{IP : "127.0.0.1", Port : 9223}
  table_name := "test"
  client := zeppelin.NewZpClient(table_name, op_timeout, m1, m2, m3)

  key := []byte("testkey")
  value := []byte("testvalue")
  key1 := []byte("testkey1")
  value1 := []byte("testvalue1")
  key2 := []byte("testkey2")
  value2 := []byte("testvalue2")

  ok, reason := client.Set(&key, &value, -1)
  if (!ok) {
    fmt.Println(reason);
  }

  ok, reason = client.Set(&key1, &value1, -1)
  if (!ok) {
    fmt.Println(reason);
  }

  ok, reason = client.Set(&key2, &value2, -1)
  if (!ok) {
    fmt.Println(reason);
  }

  ok, reason, v := client.Get(&key)
  if (!ok) {
    fmt.Println(reason);
  }
  fmt.Println(string(v[:]));

  mvalues := make(map[string]string)
  keys := []string {string(key1[:]), string(key2[:])}
  ok, reason = client.Mget(keys, &mvalues);
  if (!ok) {
    fmt.Println(reason);
  }
  for mk, mv := range mvalues {
    fmt.Println("key:", mk, "value:", mv);
  }
}
