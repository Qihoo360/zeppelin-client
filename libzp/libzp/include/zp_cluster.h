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

#include "libzp/include/zp_entity.h"

namespace pink {
  class BGThread;
}

namespace client {
  class CmdRequest;
  class CmdResponse;
}

namespace ZPMeta {
  class MetaCmd;
  class MetaCmdResponse;
  class MetaCmdResponse_Pull;
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

  uint64_t op_timeout() {
    return options_.op_timeout;
  }

  // data cmd
  Status Set(const std::string& table, const std::string& key,
      const std::string& value, int32_t ttl = -1);
  Status Delete(const std::string& table, const std::string& key);
  Status Get(const std::string& table, const std::string& key,
      std::string* value);
  Status Mget(const std::string& table, const std::vector<std::string>& keys,
      std::map<std::string, std::string>* values);
  Status Mset(const std::string& table,
      const std::vector<std::pair<std::string, std::string>>& kvs);
  Status FlushTable(const std::string& table);

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

  // The keys in a batch will be written into same partition hashed by 'tag'
  struct Batch {
    explicit Batch(const std::string& hash_tag) : tag(hash_tag) {}
    void Delete(const std::string& key) {
      keys_tobe_deleted.push_back(tag + key);
    }
    void Write(const std::string& key, const std::string& value) {
      keys_tobe_added.emplace_back(std::make_pair(tag + key, value));
    }

    std::string tag;
    std::vector<std::string> keys_tobe_deleted;
    std::vector<std::pair<std::string, std::string>> keys_tobe_added;
  };

  Status WriteBatch(const std::string& table, const Batch& batch);
  Status ListbyTag(
    const std::string& table,
    const std::string& hash_tag,
    std::map<std::string, std::string>* kvs);
  Status DeletebyTag(const std::string& table, const std::string& hash_tag);

  // meta cmd
  int64_t epoch() { return epoch_; }
  Status AddMetaNode(const Node& node);
  Status RemoveMetaNode(const Node& node);
  Status CreateTable(const std::string& table_name,
                     const std::vector<std::vector<Node>>& distribution);
  Status DropTable(const std::string& table_name);
  Status Pull(const std::string& table);
  Status FetchMetaInfo(const std::string& table, Table* table_meta);
  Status SetMaster(const std::string& table, const int partition,
      const Node& ip_port);
  Status AddSlave(const std::string& table, const int partition,
      const Node& ip_port);
  Status RemoveSlave(const std::string& table, const int partition,
      const Node& ip_port);
  Status RemoveNodes(const std::vector<libzp::Node>& nodes);

  struct MigrateCmd;
  MigrateCmd* GetMigrateCmd();  // Would not lead to memoryleak
  Status Expand(const std::string& table,
                const std::vector<Node>& ip_ports,
                MigrateCmd* cmd);
  Status Migrate(const std::string& table,
                 const Node& src_node,
                 int partition_id,
                 const Node& dst_node,
                 MigrateCmd* cmd);
  Status Shrink(const std::string& table,
                const std::vector<Node>& ip_ports,
                MigrateCmd* cmd);
  Status ReplaceNode(const Node& ori_node,
                     const Node& dst_node,
                     MigrateCmd* cmd);
  void DumpMigrateCmd(const MigrateCmd* cmd);
  Status SubmitMigrateCmd();
  Status CancelMigrate();

  // statistical cmd
  Status ListTable(std::vector<std::string>* tables);
  Status MetaStatus(Node* leader, std::map<Node, std::string>* meta_status,
                    int32_t* version, std::string* consistency_stautus);
  Status MigrateStatus(
      int64_t* migrate_begin_time, int32_t* complete_proportion);
  Status ListNode(std::vector<Node>* nodes,
      std::vector<std::string>* status);

  Status InfoQps(const std::string& table, int32_t* qps, int64_t* total_query);
  Status InfoLatency(
      const std::string& table, std::map<Node, std::string>* latency_info);
  Status InfoRepl(const Node& node,
      const std::string& table, std::map<int, PartitionView>* partitions);
  Status InfoSpace(const std::string& table,
      std::vector<std::pair<Node, SpaceInfo> >* nodes);
  Status InfoServer(const Node& node, ServerState* state);

  // local cmd
  Status DebugDumpPartition(
      const std::string& table, int partition_id = -1, bool dump_nodes = false);
  int LocateKey(const std::string& table, const std::string& key);

  std::unordered_map<std::string, Table*> tables();

 private:
  
  void Init();
  Status PullInternal(const std::string& table, uint64_t deadline);
  Status ListMeta(Node* leader, std::vector<Node>* nodes);
  Status MetaStatusInternal(Node* leader, std::map<Node, std::string>* meta_status,
                            int32_t* version, std::string* consistency_stautus,
                            int64_t* migrate_begin_time, int32_t* complete_proportion);
  /* TODO Delete me
  bool DeliverMget(CmdContext* context);
  */
  bool MultiDeliver(CmdContext* context);
  bool Deliver(CmdContext* context);
  void DeliverAndPull(CmdContext* context);
  static void DoAsyncTask(void* arg);
  void AddAsyncTask(CmdContext* context);
  static void DoNodeTask(void* arg);
  void AddNodeTask(const Node& node, CmdContext* context);
  Status SubmitDataCmd(const Node& master,
      client::CmdRequest& req, client::CmdResponse *res,
      uint64_t deadline, int attempt = 0);
  Status SubmitMetaCmd(ZPMeta::MetaCmd& req, ZPMeta::MetaCmdResponse *res,
      uint64_t deadline, int attempt = 0, const Node* specific_meta = nullptr);
  std::shared_ptr<ZpCli> GetMetaConnection(
      uint64_t deadline, Status* s, const Node* specific_meta = nullptr);

  // options
  Options options_;
 
  // Protect meta info include epoch, tables
  // Which may be changed when meta updated
  pthread_rwlock_t meta_rw_;
  int64_t epoch_;
  std::unordered_map<std::string, Table*> tables_;
  void ResetMetaInfo(const std::string& table_name,
      const ZPMeta::MetaCmdResponse_Pull& pull);
  void PointUpdateMetaInfo(const std::string& table_name,
      const std::string& sample_key, const Node& target);

  Status GetTableMasters(const std::string& table_name,
    std::set<Node>* related_nodes);
  Status GetDataMasterById(const std::string& table,
    int partition_id, Node* master);
  Status GetDataMaster(const std::string& table,
      const std::string& key, Node* master);
  Status UpdateDataMaster(const std::string& table,
      const std::string& key, const Node& target);
  Status UpdateDataMasterById(const std::string& table_name,
      int partition_id, const Node& target);

  // connection pool
  ConnectionPool* meta_pool_;
  ConnectionPool* data_pool_;

  // Pb command for communication
  ZPMeta::MetaCmd *meta_cmd_;
  ZPMeta::MetaCmdResponse *meta_res_;
  CmdContext* context_;
  
  // BG worker
  slash::Mutex peer_mu_;
  std::map<Node, pink::BGThread*> peer_workers_;
  pink::BGThread* async_worker_;
};

} // namespace libzp

#endif  // CLIENT_INCLUDE_ZP_CLUSTER_H_
