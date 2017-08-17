#include <vector>
#include <string>

#include "slash/include/slash_status.h"
#include "libzp/include/zp_cluster.h"
#include "libzp/include/zp_option.h"
#include "libzp/include/zp_cluster_c.h"

using slash::Status;
using libzp::Cluster;
using libzp::Options;
using libzp::Table;
using libzp::Node;

extern "C" {

struct zp_status_t      { Status rep; };
struct zp_option_t      { Options rep; };
struct zp_cluster_t     { Cluster* rep; };
struct zp_node_vec_t    { std::vector<zp_node_t*> rep; };
struct zp_string_vec_t  { std::vector<std::string> rep; };

zp_option_t* zp_option_create(zp_node_vec_t* metas, int op_timeout) {
  zp_option_t* option = new zp_option_t;;
  zp_node_t* node;
  while ((node = zp_nodevec_popback(metas)) != nullptr) {
    option->rep.meta_addr.push_back(Node(node->ip, node->port));
    zp_node_destroy(node);
  }
  option->rep.op_timeout = op_timeout;
  return option;
}

void zp_option_destroy(zp_option_t* option) {
  delete option;
}

zp_cluster_t* zp_cluster_create(const zp_option_t* options) {
  zp_cluster_t* cluster = new zp_cluster_t;
  cluster->rep = new Cluster(options->rep);
  return cluster;
}

void zp_cluster_destroy(zp_cluster_t* cluster) {
  delete cluster->rep;
  delete cluster;
}

zp_status_t* zp_create_table(
    zp_cluster_t* cluster, const char* table_name, int partition_num) {
  zp_status_t* s = new zp_status_t;
  std::string tn(table_name);
  s->rep = cluster->rep->CreateTable(tn, partition_num);
  return s;
}

zp_status_t* zp_drop_table(zp_cluster_t* cluster, const char* table_name) {
  zp_status_t* s = new zp_status_t;
  std::string tn(table_name);
  s->rep = cluster->rep->DropTable(tn);
  return s;
}

zp_status_t* zp_pull(zp_cluster_t* cluster, const char* table_name) {
  zp_status_t* s = new zp_status_t;
  std::string tn(table_name);
  s->rep = cluster->rep->Pull(tn);
  return s;
}

// statistical cmd
zp_status_t* zp_list_table(zp_cluster_t* cluster, zp_string_vec_t* tables) {
  zp_status_t* s = new zp_status_t;
  s->rep = cluster->rep->ListTable(&tables->rep);
  return s;
}

zp_status_t* zp_list_meta(
    zp_cluster_t* cluster, zp_node_t* master, zp_node_vec_t* slaves) {
  zp_status_t* s = new zp_status_t;
  Node mastern;
  std::vector<Node> slavesn;
  s->rep = cluster->rep->ListMeta(&mastern, &slavesn);
  if (s->rep.ok()) {
    snprintf(master->ip, 64, "%s", mastern.ip.c_str());
    master->port = mastern.port;
    for (auto& n : slavesn) {
      zp_node_t* slave = new zp_node_t;
      snprintf(slave->ip, 64, "%s", n.ip.c_str());
      slave->port = n.port;
      zp_nodevec_pushback(slaves, slave);
    }
  }
  return s;
}

zp_status_t* zp_metastatus(
    zp_cluster_t* cluster, char* meta_status, int buf_len) {
  zp_status_t* s = new zp_status_t;
  std::string buf;
  s->rep = cluster->rep->MetaStatus(&buf);
  snprintf(meta_status, buf_len, "%s", buf.c_str());
  return s;
}

zp_status_t* zp_list_node(
    zp_cluster_t* cluster, zp_node_vec_t* nodes, zp_string_vec_t* status) {
  zp_status_t* s = new zp_status_t;
  std::vector<Node> all_nodes;
  s->rep = cluster->rep->ListNode(&all_nodes, &status->rep);
  if (s->rep.ok()) {
    for (auto& n : all_nodes) {
      zp_node_t* node = zp_node_create(n.ip.c_str(), n.port);
      zp_nodevec_pushback(nodes, node);
    }
  }
  return s;
}

zp_node_t* zp_node_create(const char* ip, int port) {
  zp_node_t* node = new zp_node_t;
  snprintf(node->ip, 64, "%s", ip);
  node->port = port;
  return node;
}

void zp_node_destroy(zp_node_t* node) {
  delete node;
}

int zp_status_ok(const zp_status_t* s) {
  return s->rep.ok();
}

void zp_status_tostring(const zp_status_t* s, char* buf, int buf_len) {
  snprintf(buf, buf_len, "%s", s->rep.ToString().c_str());
}

void zp_status_destroy(zp_status_t* s) {
  delete s;
}

zp_node_vec_t* zp_nodevec_create() {
  return new zp_node_vec_t;
}

void zp_nodevec_destroy(zp_node_vec_t* vec) {
  delete vec;
}

void zp_nodevec_pushback(zp_node_vec_t* nodevec, const zp_node_t* node) {
  nodevec->rep.push_back(const_cast<zp_node_t*>(node));
}

zp_node_t* zp_nodevec_popback(zp_node_vec_t* nodevec) {
  zp_node_t *node = nullptr;
  if (nodevec == nullptr || nodevec->rep.empty()) {
    return node;
  }
  node = nodevec->rep.back();
  nodevec->rep.pop_back();
  return node;
}

zp_string_vec_t* zp_strvec_create() {
  return new zp_string_vec_t;
}

void zp_strvec_destroy(zp_string_vec_t* vec) {
  delete vec;
}

int zp_strvec_popback(zp_string_vec_t* strvec, char* buf, int buf_len) {
  if (strvec == nullptr || strvec->rep.empty()) {
    return 0;
  }
  snprintf(buf, buf_len, "%s", strvec->rep.back().c_str());
  strvec->rep.pop_back();
  return 1;
}

}  // extern "C"
