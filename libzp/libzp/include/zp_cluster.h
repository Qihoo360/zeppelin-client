/*
 * "Copyright [2016] qihoo"
 */
#ifndef CLIENT_INCLUDE_ZP_CLUSTER_H_
#define CLIENT_INCLUDE_ZP_CLUSTER_H_

#include <map>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "libzp/include/zp_meta.pb.h"
#include "libzp/include/client.pb.h"
#include "libzp/include/zp_entity.h"

namespace pink {
  class BGThread;
}

namespace libzp {

class ZpCli;
class ConnectionPool;
struct CmdContext;

class Cluster {
public:
  explicit Cluster(const Options& options);
  Cluster(const std::string& ip, int port);
  virtual ~Cluster();
  Status Connect();

  // data cmd
  Status Set(const std::string& table, const std::string& key,
      const std::string& value, int32_t ttl = -1);
  Status Delete(const std::string& table, const std::string& key);
  Status Get(const std::string& table, const std::string& key,
      std::string* value);
  Status Mget(const std::string& table, const std::vector<std::string>& keys,
      std::map<std::string, std::string>* values);

  // async data cmd
  Status Aset(const std::string& table, const std::string& key,
      const std::string& value, zp_completion_t complietion, void* data,
      int32_t ttl = -1);
  Status Adelete(const std::string& table, const std::string& key,
      zp_completion_t complietion, void* data);
  Status Aget(const std::string& table, const std::string& key,
      zp_completion_t complietion, void* data);
  Status Amget(const std::string& table, const std::vector<std::string>& keys,
      zp_completion_t complietion, void* data);

  // meta cmd
  Status CreateTable(const std::string& table_name, int partition_num);
  Status DropTable(const std::string& table_name);
  Status Pull(const std::string& table);
  Status FetchMetaInfo(const std::string& table, Table* table_meta);
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
  Status InfoRepl(const Node& node,
      const std::string& table, std::map<int, PartitionView>* partitions);
  Status InfoSpace(const std::string& table,
      std::vector<std::pair<Node, SpaceInfo> >* nodes);
  Status InfoServer(const Node& node, ServerState* state);

  // local cmd
  Status DebugDumpPartition(const std::string& table, int partition_id = -1) const;
  int LocateKey(const std::string& table, const std::string& key) const;

 private:
  
  bool DispatchMget(CmdContext* context);
  bool Dispatch(CmdContext* context);
  void DeliverDataCmd(CmdContext* context, bool has_pull = false);
  static void DoAsyncTask(void* arg);
  void AddAsyncTask(CmdContext* context);
  static void DoNodeTask(void* arg);
  void AddNodeTask(const Node& node, CmdContext* context);
  Status SubmitDataCmd(const Node& master,
      client::CmdRequest& req, client::CmdResponse *res,
      int attempt = 0);
  Status SubmitMetaCmd(int attempt = 0);
  std::shared_ptr<ZpCli> GetMetaConnection();

  Status GetTableMasters(const std::string& table_name,
    std::set<Node>* related_nodes);
  Status GetDataMaster(const std::string& table,
      const std::string& key, Node* master);
  void ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull);

  // meta info
  int64_t epoch_;
  std::vector<Node> meta_addr_;
  std::unordered_map<std::string, Table*> tables_;


  // connection pool
  ConnectionPool* meta_pool_;
  ConnectionPool* data_pool_;

  // Pb command for communication
  ZPMeta::MetaCmd meta_cmd_;
  ZPMeta::MetaCmdResponse meta_res_;
  CmdContext* context_;
  
  // BG worker
  std::map<Node, pink::BGThread*> peer_workers_;
  pink::BGThread* async_worker_;
};

} // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_CLUSTER_H_
