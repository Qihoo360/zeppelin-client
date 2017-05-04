/*
 * "Copyright [2016] qihoo"
 * "Author <hrxwwd@163.com>"
 */
#ifndef CLIENT_INCLUDE_ZP_CLUSTER_H_
#define CLIENT_INCLUDE_ZP_CLUSTER_H_

#include <map>
#include <string>
#include <vector>
#include <unordered_map>

#include "slash/include/slash_status.h"

#include "libzp/include/zp_table.h"
#include "libzp/include/zp_conn.h"
#include "libzp/include/zp_meta.pb.h"
#include "libzp/include/client.pb.h"

namespace pink {
  class BGThread;
}

namespace libzp {

using slash::Status;

struct Options {
  std::vector<Node> meta_addr;
  Options() {
  }
};

struct CmdRpcArg;

struct PartitionView {
  std::string role;
  std::string repl_state;
  Node master;
  std::vector<Node> slaves;
  int32_t file_num;
  int64_t offset;
  PartitionView(const client::PartitionState& state)
    : role(state.role()),
    repl_state(state.repl_state()),
    master(state.master().ip(), state.master().port()),
    file_num(state.sync_offset().filenum()),
    offset(state.sync_offset().offset()) {
      for (auto& s : state.slaves()) {
        slaves.push_back(Node(s.ip(), s.port()));
      }
  }
};

struct ServerState {
  int64_t epoch;
  std::vector<std::string> table_names;
  Node cur_meta;
  bool meta_renewing;
  ServerState()
    : epoch(-1),
    meta_renewing(false) {
    }

  ServerState(const client::CmdResponse::InfoServer& state)
    : epoch(state.epoch()),
    cur_meta(Node(state.cur_meta().ip(), state.cur_meta().port())),
    meta_renewing(state.meta_renewing()) {
      for (auto& s : state.table_names()) {
        table_names.push_back(s);
      }
    }
};

struct SpaceInfo {
  int64_t used;
  int64_t remain;
};

class Cluster {
public:
  explicit Cluster(const Options& options);
  Cluster(const std::string& ip, int port);
  virtual ~Cluster();

  Status Connect();

  // data cmd
  Status Set(const std::string& table, const std::string& key,
      const std::string& value, int32_t ttl = -1);
  Status Get(const std::string& table, const std::string& key,
      std::string* value);
  Status Delete(const std::string& table, const std::string& key);
  Status Mget(const std::string& table, const std::vector<std::string>& keys,
      std::map<std::string, std::string>* values);

  // meta cmd
  Status CreateTable(const std::string& table_name, int partition_num);
  Status DropTable(const std::string& table_name);
  Status Pull(const std::string& table);
  Status SetMaster(const std::string& table, const int partition,
      const Node& ip_port);
  Status AddSlave(const std::string& table, const int partition,
      const Node& ip_port);
  Status RemoveSlave(const std::string& table, const int partition,
      const Node& ip_port);

  // statistical cmd
  Status ListTable(std::vector<std::string>* tables);
  Status ListMeta(Node* master, std::vector<Node>* nodes);
  Status ListNode(std::vector<Node>* nodes,
      std::vector<std::string>* status);

  Status InfoQps(const std::string& table, int* qps, int* total_query);
  Status InfoRepl(const Node& node, const std::string& table,
      std::map<int, PartitionView>* partitions);
  Status InfoSpace(const std::string& table,
      std::vector<std::pair<Node, SpaceInfo> >* nodes);
  Status InfoServer(const Node& node, ServerState* state);

  // local cmd
  Status DebugDumpTable(const std::string& table);
  const Partition* GetPartition(const std::string& table,
      const std::string& key);

 private:
  static void DoSubmitDataCmd(void* arg);
  void DistributeDataRpc(
      const std::map<Node, CmdRpcArg*>& key_distribute);
  
  ZpCli* GetMetaConnection();
  Status TryGetDataMaster(const std::string& table,
      const std::string& key, Node* master);
  Status GetDataMaster(const std::string& table,
      const std::string& key, Node* master, bool has_pull= false);

  Status SubmitDataCmd(const std::string& table, const std::string& key,
      client::CmdRequest& req, client::CmdResponse *res, bool has_pull = false);
  Status TryDataRpc(const Node& master,
      client::CmdRequest& req, client::CmdResponse *res,
      int attempt = 0);
  Status SubmitMetaCmd(int attempt = 0);
  void ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull);

  // meta info
  int64_t epoch_;
  std::vector<Node> meta_addr_;
  std::unordered_map<std::string, Table*> tables_;
  std::map<Node, pink::BGThread*> cmd_workers_;

  // connection pool
  ConnectionPool* meta_pool_;
  ConnectionPool* data_pool_;

  // Pb command for communication
  ZPMeta::MetaCmd meta_cmd_;
  ZPMeta::MetaCmdResponse meta_res_;
  client::CmdRequest data_cmd_;
  client::CmdResponse data_res_;
};

} // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_CLUSTER_H_
