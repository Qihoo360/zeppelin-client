package zp_manager

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo LDFLAGS: -L${SRCDIR}/lib -lzp -lpthread -lprotobuf -lstdc++
#include <stdlib.h>
#include "libzp/include/zp_cluster_c.h" 
char str_buf[1024];
int str_buf_len = 1024;
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
    // deleted in node_popback
    zp_meta := C.zp_node_create(c_ip_str, C.int(v.Port))
    C.zp_nodevec_pushback(zp_metas, zp_meta)
    defer C.free(unsafe.Pointer(c_ip_str))
  }

  // Create zp_option_t
  zp_option := C.zp_option_create(zp_metas, C.int(op_timeout))

  // Create zp_cluster
  cluster.zp_cluster = C.zp_cluster_create(zp_option)

  runtime.SetFinalizer(cluster, DeleteZpCluster)
  return cluster
}

func DeleteZpCluster(cluster *ZpCluster) {
  fmt.Println("DeleteZpCluster")
}

func (cluster *ZpCluster) Pull(table_name string) (bool, string) {
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))

  status := C.zp_pull(cluster.zp_cluster, c_table_name_str)
  if int(C.zp_status_ok(status)) != 1 {
    C.zp_status_tostring(status, &C.str_buf[0], C.str_buf_len);
    return false, C.GoString(&C.str_buf[0])
  }
  C.zp_status_destroy(status)

  return true, "OK"
}

func (cluster *ZpCluster) ListTable() (*[]string) {
  tables := C.zp_strvec_create()
  defer C.zp_strvec_destroy(tables)

  status := C.zp_list_table(cluster.zp_cluster, tables)
  defer C.zp_status_destroy(status)
  if int(C.zp_status_ok(status)) != 1 {
    C.zp_status_tostring(status, &C.str_buf[0], C.str_buf_len);
    fmt.Printf("ListTable error happend, %s\n", C.GoString(&C.str_buf[0]))
    return nil;
  }

  res := new([]string)
  for int(C.zp_strvec_popback(tables, &C.str_buf[0], C.str_buf_len)) != 0 {
    *res = append(*res, C.GoString(&C.str_buf[0]))
  }
  return res;
}

func (cluster *ZpCluster) ListMeta() (*Node, *[]Node) {
  dummy_str := C.CString("")
  master := C.zp_node_create(dummy_str, C.int(0))
  slaves := C.zp_nodevec_create()

  defer C.free(unsafe.Pointer(dummy_str))
  defer C.zp_node_destroy(master)
  defer C.zp_nodevec_destroy(slaves)

  status := C.zp_list_meta(cluster.zp_cluster, master, slaves)
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    C.zp_status_tostring(status, &C.str_buf[0], C.str_buf_len);
    fmt.Printf("ListMeta error happend, %s\n", C.GoString(&C.str_buf[0]))
    return nil, nil;
  }

  master_node := new(Node)
  master_node.IP = C.GoString(&master.ip[0])
  master_node.Port = int(master.port)

  slaves_res := new([]Node)
  for slave := C.zp_nodevec_popback(slaves); slave != nil; slave = C.zp_nodevec_popback(slaves) {
    slave_node := Node{C.GoString(&slave.ip[0]), int(slave.port)}
    *slaves_res = append(*slaves_res, slave_node)
    C.zp_node_destroy(slave)
  }
  return master_node, slaves_res
}

func (cluster *ZpCluster) ListNode() (*[]Node, *[]string) {
  nodes := C.zp_nodevec_create()
  node_status := C.zp_strvec_create()

  defer C.zp_nodevec_destroy(nodes)
  defer C.zp_strvec_destroy(node_status)

  status := C.zp_list_node(cluster.zp_cluster, nodes, node_status)
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    C.zp_status_tostring(status, &C.str_buf[0], C.str_buf_len);
    fmt.Printf("ListNode error happend, %s\n", C.GoString(&C.str_buf[0]))
    return nil, nil;
  }

  node_res := new([]Node)
  node_status_res := new([]string)
  for node := C.zp_nodevec_popback(nodes); node != nil; node = C.zp_nodevec_popback(nodes) {
    n := Node{C.GoString(&node.ip[0]), int(node.port)}
    *node_res = append(*node_res, n)
    C.zp_node_destroy(node)
    C.zp_strvec_popback(node_status, &C.str_buf[0], C.str_buf_len)
    *node_status_res = append(*node_status_res, C.GoString(&C.str_buf[0]))
  }

  return node_res, node_status_res
}

func (cluster *ZpCluster) CreateTable(table_name string, partition_num int) (bool) {
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))
  status := C.zp_create_table(cluster.zp_cluster, c_table_name_str, C.int(partition_num))
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    C.zp_status_tostring(status, &C.str_buf[0], C.str_buf_len);
    fmt.Printf("CreateTable error happend, %s\n", C.GoString(&C.str_buf[0]))
    return false
  }
  return true
}
