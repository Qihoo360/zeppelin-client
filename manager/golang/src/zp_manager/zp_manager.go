package zp_manager

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo LDFLAGS: -L${SRCDIR}/lib -lzp -lm -lpthread -lprotobuf -lstdc++
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

type ZpNode struct {
  IP string
  Port int
}

// Copy from libzp/include/zp_cluster_c.h
type ZpBinlogOffset struct {
  Filenum uint32
  Offset uint64
}

type ZpPartitionView struct {
  Role string
  Repl_state string
  Master ZpNode
  Slaves []ZpNode
  Offset ZpBinlogOffset
  FallbackTime uint64
  FallbackBefore ZpBinlogOffset
  FallbackAfter ZpBinlogOffset
}

type ZpSpaceInfo struct {
  Used uint64
  Remain uint64
}

type ZpServerState struct {
  Epoch uint64
  TableNames []string
  CurMeta ZpNode
  MetaRenewing int
}

func NewZpCluster(op_timeout int, metas ...ZpNode) (cluster *ZpCluster) {
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

func (cluster *ZpCluster) ListMeta() (bool, string, *ZpNode, *[]ZpNode) {
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

  master_node := new(ZpNode)
  master_node.IP = C.GoString(C.zp_node_ip(master))
  master_node.Port = int(C.zp_node_port(master))

  slaves_res := new([]ZpNode)
  for slave := C.zp_nodevec_popback(slaves); slave != nil; slave = C.zp_nodevec_popback(slaves) {
    slave_node := ZpNode{C.GoString(C.zp_node_ip(slave)), int(C.zp_node_port(slave))}
    *slaves_res = append(*slaves_res, slave_node)
    C.zp_node_destroy(slave)
  }
  return true, "OK", master_node, slaves_res
}

func (cluster *ZpCluster) ListNode() (bool, string, *[]ZpNode, *[]string) {
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

  node_res := new([]ZpNode)
  node_status_res := new([]string)
  for node := C.zp_nodevec_popback(nodes); node != nil; node = C.zp_nodevec_popback(nodes) {
    n := ZpNode{C.GoString(C.zp_node_ip(node)), int(C.zp_node_port(node))}
    *node_res = append(*node_res, n)
    C.zp_node_destroy(node)

    ns := C.zp_strvec_popback(node_status)
    ns_str := C.GoStringN(C.zp_string_data(ns), C.zp_string_length(ns))
    C.zp_string_destroy(ns)
    *node_status_res = append(*node_status_res, ns_str)
  }

  return true, "OK", node_res, node_status_res
}

// func (cluster *ZpCluster) CreateTable(table_name string, partition_num int) (bool, string) {
//   c_table_name_str := C.CString(table_name)
//   defer C.free(unsafe.Pointer(c_table_name_str))
//   status := C.zp_create_table(cluster.zp_cluster, c_table_name_str, C.int(partition_num))
//   defer C.zp_status_destroy(status)
// 
//   if int(C.zp_status_ok(status)) != 1 {
//     status_str := C.zp_status_tostring(status)
//     reason := C.GoString(C.zp_string_data(status_str))
//     C.zp_string_destroy(status_str)
//     return false, reason
//   }
//   return true, "OK"
// }

func (cluster *ZpCluster) InfoQps(table_name string) (bool, string, int, uint64) {
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))

  var qps C.int = 0
  var total_query C.long = 0
  status := C.zp_info_qps(cluster.zp_cluster, c_table_name_str, &qps, &total_query)
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason, 0, 0
  }
  return true, "OK", int(qps), uint64(total_query)
}

func (cluster *ZpCluster) InfoRepl(node ZpNode, table_name string, repl_info *map[int]ZpPartitionView) (bool, string) {
  c_ip_str := C.CString(node.IP)
  defer C.free(unsafe.Pointer(c_ip_str))
  c_node := C.zp_node_create1(c_ip_str, C.int(node.Port))
  defer C.zp_node_destroy(c_node);

  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))

  var res_count C.int
  var p_ids *C.int
  var views *C.struct_zp_partition_view_t
  status := C.zp_info_repl(cluster.zp_cluster, c_node, c_table_name_str,
                           &res_count, &p_ids, &views)
  defer C.zp_status_destroy(status)
  defer C.free(unsafe.Pointer(p_ids))
  defer C.zp_partition_view_destroy(views)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }

  ids_ptr := uintptr(unsafe.Pointer(p_ids))
  views_ptr := uintptr(unsafe.Pointer(views))
  for i := 0; i < int(res_count); i++ {
    c_id := *(*C.int)(unsafe.Pointer(ids_ptr))
    ids_ptr += unsafe.Sizeof(c_id)
    id := int(c_id)
    view := *(*C.struct_zp_partition_view_t)(unsafe.Pointer(views_ptr))
    zp_v := ZpPartitionView{
      Role : C.GoStringN(C.zp_string_data(view.role),
                         C.zp_string_length(view.role)),
      Repl_state : C.GoStringN(C.zp_string_data(view.repl_state),
                               C.zp_string_length(view.repl_state)),
      Master : ZpNode{IP : C.GoString(C.zp_node_ip(view.master)),
                      Port : int(C.zp_node_port(view.master))},
      Offset : ZpBinlogOffset{uint32(view.offset.filenum),
                              uint64(view.offset.offset)},
      FallbackTime : uint64(view.fallback_time),
      FallbackBefore : ZpBinlogOffset{uint32(view.fallback_before.filenum),
                                      uint64(view.fallback_before.offset)},
      FallbackAfter : ZpBinlogOffset{uint32(view.fallback_after.filenum),
                                     uint64(view.fallback_after.offset)},
    }
    for j := 0; j < int(C.zp_nodevec_length(view.slaves)); j++ {
      slave := C.zp_nodevec_get(view.slaves, C.uint(j))
      zp_v.Slaves = append(zp_v.Slaves, ZpNode{IP : C.GoString(C.zp_node_ip(slave)),
                                               Port : int(C.zp_node_port(slave))})
    }
    views_ptr += unsafe.Sizeof(view)
    (*repl_info)[id] = zp_v
  }

  return true, "OK"
}

func (cluster *ZpCluster) InfoSpace(table_name string, space_info *map[ZpNode]ZpSpaceInfo) (bool, string) {
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))

  var res_count C.int
  nodes := C.zp_nodevec_create()
  var info *C.struct_zp_space_info_t
  status := C.zp_info_space(cluster.zp_cluster, c_table_name_str, &res_count, nodes, &info)
  defer C.zp_status_destroy(status)
  defer C.free(unsafe.Pointer(info))
  defer C.zp_nodevec_destroy(nodes)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }

  info_ptr := uintptr(unsafe.Pointer(info))
  for i := 0; i < int(res_count); i++ {
    _info := *(*C.struct_zp_space_info_t)(unsafe.Pointer(info_ptr))
    info_ptr += unsafe.Sizeof(_info)
    node := C.zp_nodevec_get(nodes, C.uint(i))
    (*space_info)[ZpNode{IP : C.GoString(C.zp_node_ip(node)),
                         Port : int(C.zp_node_port(node))}] =
    ZpSpaceInfo{Used : uint64(_info.used), Remain : uint64(_info.remain)}
  }

  return true, "OK"
}

func (cluster *ZpCluster) InfoServer(node ZpNode, state *ZpServerState) (bool, string) {
  c_ip_str := C.CString(node.IP)
  defer C.free(unsafe.Pointer(c_ip_str))
  c_node := C.zp_node_create1(c_ip_str, C.int(node.Port))
  defer C.zp_node_destroy(c_node);

  var server_state C.struct_zp_server_state_t
  status := C.zp_info_server(cluster.zp_cluster, c_node, &server_state)
  defer C.zp_status_destroy(status)
  defer C.zp_server_state_destroy(&server_state)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }

  state.Epoch = uint64(server_state.epoch)
  state.CurMeta = ZpNode{IP : C.GoString(C.zp_node_ip(server_state.cur_meta)),
                         Port : int(C.zp_node_port(server_state.cur_meta))}
  state.MetaRenewing = int(server_state.meta_renewing)
  for i := 0; i < int(C.zp_strvec_length(server_state.table_names)); i++ {
    tb_name := C.zp_strvec_get(server_state.table_names, C.uint(i))
    state.TableNames = append(state.TableNames,
                              C.GoStringN(C.zp_string_data(tb_name),
                                          C.zp_string_length(tb_name)))
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
