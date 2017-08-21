package zp_manager

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo LDFLAGS: -L${SRCDIR}/lib -lzp -lpthread -lprotobuf -lstdc++
#include <stdlib.h>
#include <string.h>
#include "libzp/include/zp_cluster_c.h"
*/
import "C"

import "fmt"
import "runtime"
import "unsafe"

type ZpCluster struct {
  zp_cluster *C.struct_zp_cluster_t
}

type Node struct {
  IP string
  Port int
}

func NewZpCluster(op_timeout int, metas ...Node) (cluster *ZpCluster) {
  cluster = &ZpCluster{}

  // Init zp_node_vec_t of metas
  zp_metas := C.zp_nodevec_create()
  defer C.zp_nodevec_destroy(zp_metas)

  for _, v := range metas {
    c_ip_str := C.CString(v.IP)
    zp_meta := C.zp_node_create1(c_ip_str, C.int(v.Port))
    C.zp_nodevec_pushback(zp_metas, zp_meta)
    defer C.free(unsafe.Pointer(c_ip_str))
  }

  // Create zp_option_t
  zp_option := C.zp_option_create(zp_metas, C.int(op_timeout))
  defer C.zp_option_destroy(zp_option)

  // Create zp_cluster
  cluster.zp_cluster = C.zp_cluster_create(zp_option)

  runtime.SetFinalizer(cluster, DeleteZpCluster)
  return cluster
}

func DeleteZpCluster(cluster *ZpCluster) {
  fmt.Println("DeleteZpCluster")
  C.zp_cluster_destroy(cluster.zp_cluster);
}

func (cluster *ZpCluster) Pull(table_name string) (bool, string) {
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))

  status := C.zp_pull(cluster.zp_cluster, c_table_name_str)
  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }
  C.zp_status_destroy(status)

  return true, "OK"
}

func (cluster *ZpCluster) ListTable() (bool, string, *[]string) {
  tables := C.zp_strvec_create()
  defer C.zp_strvec_destroy(tables)

  status := C.zp_list_table(cluster.zp_cluster, tables)
  defer C.zp_status_destroy(status)
  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason, nil;
  }

  res := new([]string)
  for tbl := C.zp_strvec_popback(tables); tbl != nil; tbl = C.zp_strvec_popback(tables) {
  // for int(C.zp_strvec_popback(tables, &C.str_buf[0], C.str_buf_len)) != 0 {
    table := C.GoStringN(C.zp_string_data(tbl), C.zp_string_length(tbl));
    C.zp_string_destroy(tbl)
    *res = append(*res, table)
  }
  return true, "OK", res;
}

func (cluster *ZpCluster) ListMeta() (bool, string, *Node, *[]Node) {
  master := C.zp_node_create()
  slaves := C.zp_nodevec_create()

  defer C.zp_node_destroy(master)
  defer C.zp_nodevec_destroy(slaves)

  status := C.zp_list_meta(cluster.zp_cluster, master, slaves)
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason, nil, nil;
  }

  master_node := new(Node)
  master_node.IP = C.GoString(C.zp_node_ip(master))
  master_node.Port = int(C.zp_node_port(master))

  slaves_res := new([]Node)
  for slave := C.zp_nodevec_popback(slaves); slave != nil; slave = C.zp_nodevec_popback(slaves) {
    slave_node := Node{C.GoString(C.zp_node_ip(slave)), int(C.zp_node_port(slave))}
    *slaves_res = append(*slaves_res, slave_node)
    C.zp_node_destroy(slave)
  }
  return true, "OK", master_node, slaves_res
}

func (cluster *ZpCluster) ListNode() (bool, string, *[]Node, *[]string) {
  nodes := C.zp_nodevec_create()
  node_status := C.zp_strvec_create()

  defer C.zp_nodevec_destroy(nodes)
  defer C.zp_strvec_destroy(node_status)

  status := C.zp_list_node(cluster.zp_cluster, nodes, node_status)
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason, nil, nil;
  }

  node_res := new([]Node)
  node_status_res := new([]string)
  for node := C.zp_nodevec_popback(nodes); node != nil; node = C.zp_nodevec_popback(nodes) {
    n := Node{C.GoString(C.zp_node_ip(node)), int(C.zp_node_port(node))}
    *node_res = append(*node_res, n)
    C.zp_node_destroy(node)

    ns := C.zp_strvec_popback(node_status)
    ns_str := C.GoStringN(C.zp_string_data(ns), C.zp_string_length(ns))
    C.zp_string_destroy(ns)
    *node_status_res = append(*node_status_res, ns_str)
  }

  return true, "OK", node_res, node_status_res
}

func (cluster *ZpCluster) CreateTable(table_name string, partition_num int) (bool, string) {
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))
  status := C.zp_create_table(cluster.zp_cluster, c_table_name_str, C.int(partition_num))
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }
  return true, "OK"
}

func (cluster *ZpCluster) Set(table_name string, key *[]byte, value *[]byte, ttl int) (bool, string) {
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))

  zp_key := C.zp_string_create1((*C.char)(unsafe.Pointer(&(*key)[0])), C.int(len(*key)))
  zp_value := C.zp_string_create1((*C.char)(unsafe.Pointer(&(*value)[0])), C.int(len(*value)))
  defer C.zp_string_destroy(zp_key)
  defer C.zp_string_destroy(zp_value)

  status := C.zp_cluster_set(cluster.zp_cluster, c_table_name_str, zp_key, zp_value, C.int(ttl))
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }
  return true, "OK"
}

func (cluster *ZpCluster) Get(table_name string, key *[]byte) (bool, string, []byte) {
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))

  zp_key := C.zp_string_create1((*C.char)(unsafe.Pointer(&(*key)[0])), C.int(len(*key)))
  zp_value := C.zp_string_create()
  defer C.zp_string_destroy(zp_key)
  defer C.zp_string_destroy(zp_value)

  status := C.zp_cluster_get(cluster.zp_cluster, c_table_name_str, zp_key, zp_value)
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    null := make([]byte, 0)
    return false, reason, null
  }

  v := C.GoBytes(unsafe.Pointer(C.zp_string_data(zp_value)), C.zp_string_length(zp_value))
  return true, "OK", v
}

func (cluster *ZpCluster) Mget(table_name string, keys []string, values *map[string]string) (bool, string) {
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))

  zp_keys := C.zp_strvec_create()
  zp_res_keys := C.zp_strvec_create()
  zp_res_values := C.zp_strvec_create()
  defer C.zp_strvec_destroy(zp_keys)
  defer C.zp_strvec_destroy(zp_res_keys)
  defer C.zp_strvec_destroy(zp_res_values)

  for _, v := range keys {
    zp_key := C.CString(v)
    defer C.free(unsafe.Pointer(zp_key))
    zp_str_key := C.zp_string_create1(zp_key, C.int(len(v)))
    C.zp_strvec_pushback(zp_keys, zp_str_key)
  }

  status := C.zp_cluster_mget(cluster.zp_cluster, c_table_name_str, zp_keys, zp_res_keys, zp_res_values)
  defer C.zp_status_destroy(status)
  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }

  for k := C.zp_strvec_popback(zp_res_keys); k != nil; k =
      C.zp_strvec_popback(zp_res_keys) {
    v := C.zp_strvec_popback(zp_res_values)
    defer C.zp_string_destroy(k);
    defer C.zp_string_destroy(v);
  
    gk := C.GoStringN(C.zp_string_data(k), C.zp_string_length(k))
    gv := C.GoStringN(C.zp_string_data(v), C.zp_string_length(v))
    (*values)[gk] = gv
  }

  return true, "OK"
}

func (cluster *ZpCluster) Delete(table_name string, key string) (bool, string) {
  c_table_name_str := C.CString(table_name)
  c_key_str := C.CString(key)
  defer C.free(unsafe.Pointer(c_table_name_str))
  defer C.free(unsafe.Pointer(c_key_str))

  zp_key := C.zp_string_create1(c_key_str, C.int(len(key)))
  defer C.zp_string_destroy(zp_key)

  status := C.zp_cluster_delete(cluster.zp_cluster, c_table_name_str, zp_key)
  defer C.zp_status_destroy(status)
  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }

  return true, "OK"
}
