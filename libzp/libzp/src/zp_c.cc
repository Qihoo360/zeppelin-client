#include <vector>
#include <string>
#include <string.h>

#include "slash/include/slash_status.h"
#include "libzp/include/zp_cluster.h"
#include "libzp/include/zp_client.h"
#include "libzp/include/zp_option.h"
#include "libzp/include/zp_cluster_c.h"
#include "libzp/include/zp_client_c.h"

using slash::Status;
using libzp::Cluster;
using libzp::Options;
using libzp::Table;
using libzp::Node;
using libzp::PartitionView;
using libzp::SpaceInfo;
using libzp::BinlogOffset;
using libzp::ServerState;

extern "C" {

struct zp_status_t          { Status rep; };
struct zp_option_t          { Options rep; };
struct zp_cluster_t         { Cluster* rep; };
struct zp_client_t          { zp_cluster_t* rep; std::string tb_name; };
struct zp_node_t            { char ip[32]; int port; };
struct zp_node_vec_t        { std::vector<zp_node_t*> rep; };
struct zp_string_t          { char* data; int length; };
struct zp_string_vec_t      { std::vector<zp_string_t*> rep; };

// zp_status_t
int zp_status_ok(const zp_status_t* s) { return s->rep.ok(); }
zp_string_t* zp_status_tostring(const zp_status_t* s) {
  std::string res = s->rep.ToString();
  return zp_string_create1(res.data(), res.length());
}
void zp_status_destroy(zp_status_t* s) { delete s; }

// zp_option_t
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
void zp_option_destroy(zp_option_t* option) { delete option; }

// zp_cluster_t
zp_cluster_t* zp_cluster_create(const zp_option_t* options) {
  zp_cluster_t* cluster = new zp_cluster_t;
  cluster->rep = new Cluster(options->rep);
  return cluster;
}
void zp_cluster_destroy(zp_cluster_t* cluster) {
  delete cluster->rep;
  delete cluster;
}

// zp_client_t
zp_client_t* zp_client_create(const zp_option_t* options, const char* tb_name) {
  zp_client_t* client = new zp_client_t;
  client->rep = zp_cluster_create(options);
  client->tb_name.assign(tb_name);
  return client;
}
void zp_client_destroy(zp_client_t* client) {
  delete client->rep;
  delete client;
}

// zp_node_t
static zp_node_t* zp_node_create2(const Node& node) {
  zp_node_t* n = new zp_node_t;
  snprintf(n->ip, 32, "%s", node.ip.c_str());
  n->port = node.port;
  return n;
}
zp_node_t* zp_node_create1(const char* ip, int port) {
  zp_node_t* node = new zp_node_t;
  snprintf(node->ip, 32, "%s", ip);
  node->port = port;
  return node;
}
zp_node_t* zp_node_create() { return zp_node_create1("", -1); }
void zp_node_destroy(zp_node_t* node) { delete node; }
char* zp_node_ip(zp_node_t* node) { return node->ip; }
int zp_node_port(zp_node_t* node) { return node->port; }

// zp_nodevec_t
zp_node_vec_t* zp_nodevec_create() { return new zp_node_vec_t; }
void zp_nodevec_destroy(zp_node_vec_t* vec) {
  if (vec) {
    for (zp_node_t* node : vec->rep) {
      delete node;
    }
  }
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
int zp_nodevec_length(zp_node_vec_t* vec) { return vec->rep.size(); }
zp_node_t* zp_nodevec_get(zp_node_vec_t* vec, unsigned int index) {
  if (index > vec->rep.size()) {
    return nullptr;
  }
  return vec->rep[index];
}

// zp_string_t
zp_string_t* zp_string_create1(const char* data, int length) {
  zp_string_t* str = new zp_string_t;
  if (length < 0) {
    return str;
  }
  str->data = new char[length];
  memcpy(str->data, data, length);
  str->length = length;
  return str;
}
zp_string_t* zp_string_create() { return zp_string_create1(nullptr, 0); }
void zp_string_destroy(zp_string_t* str) {
  if (str->length > 0) {
    delete[] str->data;
    str->length = 0;
  }
  delete str;
}
char* zp_string_data(zp_string_t* str) { return str->data; }
int zp_string_length(zp_string_t* str) { return str->length; }

// zp_strvec_t
zp_string_vec_t* zp_strvec_create() { return new zp_string_vec_t; }
void zp_strvec_destroy(zp_string_vec_t* vec) {
  if (vec) {
    for (zp_string_t* s : vec->rep) {
      delete s;
    }
  }
  delete vec;
}
void zp_strvec_pushback(zp_string_vec_t* nodevec, zp_string_t* str) {
  nodevec->rep.push_back(str);
}
zp_string_t* zp_strvec_popback(zp_string_vec_t* strvec) {
  zp_string_t* str = nullptr;
  if (strvec == nullptr || strvec->rep.empty()) {
    return str;
  }
  str = strvec->rep.back();
  strvec->rep.pop_back();
  return str;
}
int zp_strvec_length(zp_string_vec_t* vec) { return vec->rep.size(); }
zp_string_t* zp_strvec_get(zp_string_vec_t* vec, unsigned int index) {
  if (index > vec->rep.size()) {
    return nullptr;
  }
  return vec->rep[index];
}

void zp_partition_view_destroy(zp_partition_view_t* var) {
  if (var == nullptr) return;
  zp_string_destroy(var->role);
  zp_string_destroy(var->repl_state);
  zp_node_destroy(var->master);
  zp_node_t* n;
  while ((n = zp_nodevec_popback(var->slaves)) != nullptr) {
    zp_node_destroy(n);
  }
  zp_nodevec_destroy(var->slaves);
}

void zp_server_state_destroy(zp_server_state_t* var) {
  zp_string_t* s;
  while ((s = zp_strvec_popback(var->table_names)) != nullptr) {
    zp_string_destroy(s);
  }
  zp_strvec_destroy(var->table_names);
  zp_node_destroy(var->cur_meta);
}

// Zeppelin cluster interface
zp_status_t* zp_create_table(
    const zp_cluster_t* cluster,
    const char* table_name,
    int partition_num) {
  zp_status_t* s = new zp_status_t;
  std::string tn(table_name);
  s->rep = cluster->rep->CreateTable(tn, partition_num);
  return s;
}

zp_status_t* zp_drop_table(
    const zp_cluster_t* cluster,
    const char* table_name) {
  zp_status_t* s = new zp_status_t;
  std::string tn(table_name);
  s->rep = cluster->rep->DropTable(tn);
  return s;
}

zp_status_t* zp_pull(
    const zp_cluster_t* cluster,
    const char* table_name) {
  zp_status_t* s = new zp_status_t;
  std::string tn(table_name);
  s->rep = cluster->rep->Pull(tn);
  return s;
}

// statistical cmd
zp_status_t* zp_list_table(
    const zp_cluster_t* cluster,
    zp_string_vec_t* tables) {
  zp_status_t* s = new zp_status_t;
  std::vector<std::string> tbls;
  s->rep = cluster->rep->ListTable(&tbls);
  if (s->rep.ok()) {
    for (auto& tbl : tbls) {
      zp_string_t* tbl_str = zp_string_create1(tbl.data(), tbl.length());
      zp_strvec_pushback(tables, tbl_str);
    }
  }
  return s;
}

zp_status_t* zp_list_meta(
    const zp_cluster_t* cluster,
    zp_node_t* master,
    zp_node_vec_t* slaves) {
  zp_status_t* s = new zp_status_t;
  Node mn;
  std::vector<Node> sns;
  s->rep = cluster->rep->ListMeta(&mn, &sns);
  if (s->rep.ok()) {
    snprintf(master->ip, 32, "%s", mn.ip.c_str());
    master->port = mn.port;
    for (auto& s : sns) {
      zp_node_t* slave = zp_node_create1(s.ip.c_str(), s.port);
      zp_nodevec_pushback(slaves, slave);
    }
  }
  return s;
}

zp_status_t* zp_metastatus(
    const zp_cluster_t* cluster,
    zp_string_t* status) {
  zp_status_t* s = new zp_status_t;
  std::string buf;
  s->rep = cluster->rep->MetaStatus(&buf);
  status->data = static_cast<char*>(realloc(status->data, buf.length()));
  memcpy(status->data, buf.data(), buf.length());
  return s;
}

zp_status_t* zp_list_node(
    const zp_cluster_t* cluster,
    zp_node_vec_t* nodes,
    zp_string_vec_t* status) {
  zp_status_t* s = new zp_status_t;
  std::vector<Node> all_nodes;
  std::vector<std::string> sts;
  s->rep = cluster->rep->ListNode(&all_nodes, &sts);
  if (s->rep.ok()) {
    for (size_t i = 0; i < all_nodes.size(); i++) {
      zp_node_t* node = zp_node_create1(all_nodes[i].ip.c_str(), all_nodes[i].port);
      zp_string_t* str = zp_string_create1(sts[i].data(), sts[i].length());
      zp_nodevec_pushback(nodes, node);
      zp_strvec_pushback(status, str);
    }
  }
  return s;
}

// info cmds
zp_status_t* zp_info_qps(
    const zp_cluster_t* cluster,
    const char* table_name,
    int* qps,
    long* total_query) {
  zp_status_t* s = new zp_status_t;
  std::string tb_name(table_name);
  s->rep = cluster->rep->InfoQps(tb_name, qps, total_query);
  return s;
}

zp_status_t* zp_info_repl(
    const zp_cluster_t* cluster,
    const zp_node_t* node,
    const char* table_name,
    int* res_count,
    int** p_ids,
    zp_partition_view_t** views) {
  zp_status_t* s = new zp_status_t;
  Node n(node->ip, node->port);
  std::string tb_name(table_name);
  std::map<int, PartitionView> partitions;
  s->rep = cluster->rep->InfoRepl(n, tb_name, &partitions);
  if (s->rep.ok()) {
    *res_count = partitions.size();
    *p_ids = reinterpret_cast<int*>(malloc((*res_count) * sizeof(int)));
    *views = reinterpret_cast<zp_partition_view_t*>(
        malloc((*res_count) * sizeof(zp_partition_view_t)));
    int index = 0;
    for (auto& item : partitions) {
      (*p_ids)[index] = item.first;
      const PartitionView& view = item.second;
      // role
      (*views)[index].role = zp_string_create1(
          view.role.data(), view.role.length());
      // repl_state
      (*views)[index].repl_state= zp_string_create1(
          view.repl_state.data(), view.repl_state.length());
      // master
      (*views)[index].master = zp_node_create1(
          view.master.ip.c_str(), view.master.port);
      // slaves
      zp_node_vec_t* slaves  = zp_nodevec_create();
      (*views)[index].slaves = slaves;
      for (auto& s : view.slaves) {
        zp_node_t* slave = zp_node_create1(s.ip.c_str(), s.port);
        zp_nodevec_pushback(slaves, slave);
      }
      // offset
      (*views)[index].offset.filenum = view.offset.filenum;
      (*views)[index].offset.offset = view.offset.offset;
      // fallback_time
      (*views)[index].fallback_time = view.fallback_time;
      // fallback_before
      (*views)[index].fallback_before.filenum = view.fallback_before.filenum;
      (*views)[index].fallback_before.offset = view.fallback_before.offset;
      // fallback_after
      (*views)[index].fallback_after.filenum = view.fallback_after.filenum;
      (*views)[index].fallback_after.offset = view.fallback_after.offset;

      index++;
    }
  }
  return s;
}

zp_status_t* zp_info_space(
    const zp_cluster_t* cluster,
    const char* table_name,
    int* res_count,
    zp_node_vec_t* nodes,
    zp_space_info_t** infos) {
  zp_status_t* s = new zp_status_t;
  std::string tb_name(table_name);
  std::vector<std::pair<Node, SpaceInfo>> nodes_info;
  s->rep = cluster->rep->InfoSpace(tb_name, &nodes_info);
  if (s->rep.ok()) {
    *res_count = nodes_info.size();
    *infos = reinterpret_cast<zp_space_info_t*>(
        malloc((*res_count) * sizeof(zp_space_info_t)));
    for (size_t i = 0; i < nodes_info.size(); i++) {
      auto& item = nodes_info[i];
      zp_node_t* node = zp_node_create2(item.first);
      zp_nodevec_pushback(nodes, node);
      (*infos)[i].used = item.second.used;
      (*infos)[i].remain = item.second.remain;
    }
  }
  return s;
}

zp_status_t* zp_info_server(
    const zp_cluster_t* cluster,
    const zp_node_t* node,
    zp_server_state_t* state) {
  zp_status_t* s = new zp_status_t;
  Node n(node->ip, node->port);
  ServerState server_state;
  s->rep = cluster->rep->InfoServer(n, &server_state);
  if (s->rep.ok()) {
    state->epoch = server_state.epoch;  // epoch
    // table_names
    state->table_names = zp_strvec_create();
    for (auto& name : server_state.table_names) {
      zp_string_t* n = zp_string_create1(name.data(), name.length());
      zp_strvec_pushback(state->table_names, n);
    }
    // cur_meta
    state->cur_meta = zp_node_create2(server_state.cur_meta);
    // meta_renewing
    state->meta_renewing = server_state.meta_renewing;
  }
  return s;
}

// Zeppelin cluster interface
zp_status_t* zp_cluster_set(
    const zp_cluster_t* cluster,
    const char* table_name,
    const zp_string_t* key,
    const zp_string_t* value,
    int ttl) {
  zp_status_t* s = new zp_status_t;
  std::string tb_name(table_name);
  std::string k(key->data, key->length);
  std::string v(value->data, value->length);
  s->rep = cluster->rep->Set(tb_name, k, v, ttl);
  return s;
}

zp_status_t* zp_cluster_get(
    const zp_cluster_t* cluster,
    const char* table_name,
    const zp_string_t* key,
    zp_string_t* value) {
  zp_status_t* s = new zp_status_t;
  std::string tb_name(table_name);
  std::string k(key->data, key->length);
  std::string v;
  s->rep = cluster->rep->Get(tb_name, k, &v);
  if (s->rep.ok()) {
    value->data = static_cast<char*>(realloc(value->data, v.length()));
    memcpy(value->data, v.data(), v.length());
    value->length = v.length();
  }
  return s;
}

zp_status_t* zp_cluster_mget(
    const zp_cluster_t* cluster,
    const char* table_name,
    zp_string_vec_t* keys,
    zp_string_vec_t* res_keys,
    zp_string_vec_t* res_values) {
  zp_status_t* s = new zp_status_t;
  std::string tb_name(table_name);
  std::vector<std::string> ks;
  std::map<std::string, std::string> vs;

  zp_string_t* key;
  while ((key = zp_strvec_popback(keys))) {
    ks.push_back(std::string(key->data, key->length));
    delete key;
  }
  s->rep = cluster->rep->Mget(tb_name, ks, &vs);
  if (s->rep.ok()) {
    for (auto& p : vs) {
      zp_string_t* k = zp_string_create1(p.first.data(), p.first.length());
      zp_string_t* v = zp_string_create1(p.second.data(), p.second.length());
      zp_strvec_pushback(res_keys, k);
      zp_strvec_pushback(res_values, v);
    }
  }
  return s;
}

zp_status_t* zp_cluster_delete(
    const zp_cluster_t* cluster,
    const char* table_name,
    const zp_string_t* key) {
  zp_status_t* s = new zp_status_t;
  std::string tb_name(table_name);
  std::string k(key->data, key->length);
  s->rep = cluster->rep->Delete(tb_name, k);
  return s;
}

// Zeppelin client interface
zp_status_t* zp_set(
    const zp_client_t* client,
    const zp_string_t* key,
    const zp_string_t* value,
    int ttl) {
  return zp_cluster_set(client->rep, client->tb_name.c_str(),
                        key, value, ttl);
}

zp_status_t* zp_get(
    const zp_client_t* client,
    const zp_string_t* key,
    zp_string_t* value) {
  return zp_cluster_get(client->rep, client->tb_name.c_str(),
                        key, value);
}

zp_status_t* zp_mget(
    const zp_client_t* client,
    zp_string_vec_t* keys,
    zp_string_vec_t* res_keys,
    zp_string_vec_t* res_values) {
  return zp_cluster_mget(client->rep, client->tb_name.c_str(),
                         keys, res_keys, res_values);
}

zp_status_t* zp_delete(
    const zp_client_t* client,
    const zp_string_t* key) {
  return zp_cluster_delete(client->rep, client->tb_name.c_str(),
                           key);
}

}  // extern "C"
