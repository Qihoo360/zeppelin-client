#include <string.h>
#include <stdio.h>

#include "libzp/include/zp_cluster_c.h"

int main() {
  zp_node_t* n = zp_node_create("127.0.0.1", 9221);
  zp_node_vec_t* metas = zp_nodevec_create();
  zp_nodevec_pushback(metas, n);
  zp_option_t* option = zp_option_create(metas, 1000);

  zp_cluster_t* cluster = zp_cluster_create(option);

  char buf[1024];

  // Pull
  zp_status_t* s = zp_pull(cluster, "test");
  if (!zp_status_ok(s)) {
    zp_status_tostring(s, buf, sizeof(buf));
    printf("error: %s\n", buf);
  }
  zp_status_destroy(s);

  // ListTable
  zp_string_vec_t* tables = zp_strvec_create();
  s = zp_list_table(cluster, tables);
  if (!zp_status_ok(s)) {
    zp_status_tostring(s, buf, sizeof(buf));
    printf("error: %s\n", buf);
  }
  zp_status_destroy(s);
  printf("ListTable:\n");
  while (zp_strvec_popback(tables, buf, sizeof(buf)) > 0) {
    printf("  --- %s\n", buf);
  }
  zp_strvec_destroy(tables);

  // ListMeta
  zp_node_t* master = zp_node_create("", 0);
  zp_node_vec_t* slaves = zp_nodevec_create();
  s = zp_list_meta(cluster, master, slaves);
  if (!zp_status_ok(s)) {
    zp_status_tostring(s, buf, sizeof(buf));
    printf("error: %s\n", buf);
  }
  zp_status_destroy(s);
  printf("Meta master: %s:%d\n", master->ip, master->port);
  zp_node_t* slave;
  while ((slave = zp_nodevec_popback(slaves)) != NULL) {
    printf("Meta slave: %s:%d\n", slave->ip, slave->port);
    zp_node_destroy(slave);
  }
  zp_node_destroy(master);
  zp_nodevec_destroy(slaves);

  // ListNode
  zp_node_vec_t* nodes = zp_nodevec_create();
  zp_string_vec_t* status = zp_strvec_create();
  s = zp_list_node(cluster, nodes, status);
  if (!zp_status_ok(s)) {
    zp_status_tostring(s, buf, sizeof(buf));
    printf("error: %s\n", buf);
  }
  zp_status_destroy(s);
  zp_node_t* node;
  while ((node = zp_nodevec_popback(nodes)) != NULL) {
    zp_strvec_popback(status, buf, sizeof(buf));
    printf("node: %s:%d, %s\n", node->ip, node->port, buf);
    zp_node_destroy(node);
  }
  zp_nodevec_destroy(nodes);
  zp_strvec_destroy(status);

  // CreateTable
  const char* table_name = "test5";
  int partition_num = 4;
  s = zp_create_table(cluster, table_name, partition_num);
  if (!zp_status_ok(s)) {
    zp_status_tostring(s, buf, sizeof(buf));
    printf("error: %s\n", buf);
  } else {
    printf("create table %s success\n", table_name);
  }
  zp_status_destroy(s);

  zp_nodevec_destroy(metas);
  zp_node_destroy(n);
  zp_option_destroy(option);
  zp_cluster_destroy(cluster);
}
