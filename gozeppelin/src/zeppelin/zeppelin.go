package zeppelin

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo LDFLAGS: -L${SRCDIR}/lib -lzp -lm -lpthread -lprotobuf -lstdc++
#include <stdlib.h>
#include <string.h>
#include "libzp/include/zp_client_c.h"
*/
import "C"

import "fmt"
import "runtime"
import "unsafe"

type ZpClient struct {
  zp_client *C.struct_zp_client_t
}

type ZpNode struct {
  IP string
  Port int
}

func NewZpClient(table_name string, op_timeout int, metas ...ZpNode) (client *ZpClient) {
  client = &ZpClient{}

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

  // Create zp_client
  c_table_name_str := C.CString(table_name)
  defer C.free(unsafe.Pointer(c_table_name_str))
  client.zp_client = C.zp_client_create(zp_option, c_table_name_str)

  runtime.SetFinalizer(client, DeleteZpClient)
  return client
}

func DeleteZpClient(client *ZpClient) {
  fmt.Println("DeleteZpClient")
  C.zp_client_destroy(client.zp_client);
}

func (client *ZpClient) Set(key *[]byte, value *[]byte, ttl int) (bool, string) {
  zp_key := C.zp_string_create1((*C.char)(unsafe.Pointer(&(*key)[0])), C.int(len(*key)))
  zp_value := C.zp_string_create1((*C.char)(unsafe.Pointer(&(*value)[0])), C.int(len(*value)))
  defer C.zp_string_destroy(zp_key)
  defer C.zp_string_destroy(zp_value)

  status := C.zp_set(client.zp_client, zp_key, zp_value, C.int(ttl))
  defer C.zp_status_destroy(status)

  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }
  return true, "OK"
}

func (client *ZpClient) Get(key *[]byte) (bool, string, []byte) {
  zp_key := C.zp_string_create1((*C.char)(unsafe.Pointer(&(*key)[0])), C.int(len(*key)))
  zp_value := C.zp_string_create()
  defer C.zp_string_destroy(zp_key)
  defer C.zp_string_destroy(zp_value)

  status := C.zp_get(client.zp_client, zp_key, zp_value)
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

func (client *ZpClient) Mget(keys []string, values *map[string]string) (bool, string) {
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

  status := C.zp_mget(client.zp_client, zp_keys, zp_res_keys, zp_res_values)
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

func (client *ZpClient) Delete(key string) (bool, string) {
  c_key_str := C.CString(key)
  defer C.free(unsafe.Pointer(c_key_str))

  zp_key := C.zp_string_create1(c_key_str, C.int(len(key)))
  defer C.zp_string_destroy(zp_key)

  status := C.zp_delete(client.zp_client, zp_key)
  defer C.zp_status_destroy(status)
  if int(C.zp_status_ok(status)) != 1 {
    status_str := C.zp_status_tostring(status)
    reason := C.GoString(C.zp_string_data(status_str))
    C.zp_string_destroy(status_str)
    return false, reason
  }

  return true, "OK"
}
