/*
 * "Copyright [2016] qihoo"
 */
#include "libzp/include/zp_cluster.h"

#include <unordered_set>
#include <tuple>
#include <deque>
#include <memory>
#include <queue>
#include <string>
#include <algorithm>
#include <sstream>

#include "slash/include/slash_string.h"
#include "slash/include/env.h"
#include "pink/include/bg_thread.h"
#include "pink/include/pink_cli.h"
#include "libzp/src/zp_conn.h"
#include "libzp/src/zp_meta.pb.h"
#include "libzp/src/client.pb.h"

namespace libzp {

const int kMetaAttempt = 10;
const int kDataAttempt = 10;

struct NodeTaskArg {
  CmdContext* context;
  Node target;
  NodeTaskArg(CmdContext* c, const Node& n)
    :context(c), target(n) {}
};

static inline uint64_t CalcDeadline(int timeout) {
  if (timeout == 0) {
    return 0; // no limit
  }
  return slash::NowMicros() / 1000 + timeout;
}

struct CmdContextResult {
  Status status;
  client::StatusCode res_code;
  std::string res_msg;
  bool is_filled;

  CmdContextResult()
    : status(Status::OK()),
    res_code(client::StatusCode::kOk),
    is_filled(false) {}

  inline void Fill(const Status& s,
      const client::StatusCode& code, const std::string& msg) {
    status = s;
    res_code = code;
    res_msg = msg;
    is_filled = true;
  }
  inline bool empty() const {
    return (!is_filled);
  }
};

struct CmdContext {
  Cluster* cluster;
  std::string table;
  
  // key and partition_id as two different type of routing info
  // used by different command
  std::string key;
  int partition_id;
  client::CmdRequest* request;
  client::CmdResponse* response;
  Status result;
  zp_completion_t completion;
  void* user_data;
  uint64_t deadline; //0 means no limit

  CmdContext()
      : cluster(NULL), partition_id(-1),
        result(Status::Incomplete("Not complete")),
        user_data(NULL), cond_(&mu_), done_(false) {
    request = new client::CmdRequest();
    response = new client::CmdResponse();
  }

  // Init with partition id
  void Init(Cluster* c, const std::string& tab, int id,
      zp_completion_t comp = NULL, void* d = NULL) {
    partition_id = id;
    Init(c, tab, std::string(), comp, d);
  }

  // Init with key
  void Init(Cluster* c, const std::string& tab, const std::string& k = std::string(),
      zp_completion_t comp = NULL, void* d = NULL) {
    cluster = c;
    table = tab;
    key = k;
    request->Clear();
    response->Clear();
    result = Status::Incomplete("Not complete");
    completion = comp;
    user_data = d;
    done_ = false;
    deadline = CalcDeadline(c->op_timeout());
  }
  
  inline bool OpTimeout() {
    if (deadline == 0) {
      return false; // no limit
    }
    return (slash::NowMicros() / 1000) >= deadline;
  }

  inline void Reset () {
    response->Clear();
    result = Status::Incomplete("Not complete");
    done_ = false;
  }
  
  inline void SetResult(const CmdContextResult& ccr) {
    result = ccr.status;
    response->set_code(ccr.res_code);
    response->set_msg(ccr.res_msg);
  }
  
  ~CmdContext() {
    delete response;
    delete request;
  }

  void WaitRpcDone() {
    slash::MutexLock l(&mu_);
    while (!done_) {
      cond_.Wait();  
    }
  }
  void RpcDone() {
    slash::MutexLock l(&mu_);
    done_ = true;
    cond_.Signal();
  }

private:
  slash::Mutex mu_;
  slash::CondVar cond_;
  bool done_;
};

static void BuildSetContext(Cluster* cluster, const std::string& table,
    const std::string& key, const std::string& value, int ttl,
    CmdContext* set_context, zp_completion_t completion = NULL, void* data = NULL) {
  set_context->Init(cluster, table, key, completion, data);
  set_context->request->set_type(client::Type::SET);
  client::CmdRequest_Set* set_info = set_context->request->mutable_set();
  set_info->set_table_name(table);
  set_info->set_key(key);
  set_info->set_value(value);
  if (ttl >= 0) {
    set_info->mutable_expire()->set_ttl(ttl);
  }
}

static void BuildGetContext(Cluster*cluster, const std::string& table,
    const std::string& key, CmdContext* get_context,
    zp_completion_t completion = NULL, void* data = NULL) {
  get_context->Init(cluster, table, key, completion, data);
  get_context->request->set_type(client::Type::GET);
  client::CmdRequest_Get* get_cmd = get_context->request->mutable_get();
  get_cmd->set_table_name(table);
  get_cmd->set_key(key);
}

static void BuildDeleteContext(Cluster* cluster, const std::string& table,
    const std::string& key, CmdContext* delete_context,
    zp_completion_t completion = NULL, void* data = NULL) {
  delete_context->Init(cluster, table, key, completion, data);
  delete_context->request->set_type(client::Type::DEL);
  client::CmdRequest_Del* delete_cmd = delete_context->request->mutable_del();
  delete_cmd->set_table_name(table);
  delete_cmd->set_key(key);
}

static void BuildMgetContext(Cluster* cluster, const std::string& table,
    const std::vector<std::string>& keys, CmdContext* mget_context,
    zp_completion_t completion = NULL, void* data = NULL) {
  mget_context->Init(cluster, table, keys[0], completion, data);
  mget_context->request->set_type(client::Type::MGET);
  client::CmdRequest_Mget* mget_cmd = mget_context->request->mutable_mget();
  mget_cmd->set_table_name(table);
  for (auto& key : keys) {
    mget_cmd->add_keys(key);
  }
}

static void BuildFlushTableContext(Cluster*cluster, const std::string& table,
    int partition_id, CmdContext* flush_context) {
  flush_context->Init(cluster, table, partition_id);
  flush_context->request->set_type(client::Type::FLUSHDB);
  client::CmdRequest_FlushDB* flush_cmd = flush_context->request->mutable_flushdb();
  flush_cmd->set_table_name(table);
  flush_cmd->set_partition_id(partition_id);
}

static void BuildInfoContext(Cluster* cluster, const std::string& table,
    client::Type type, CmdContext* info_context) {
  info_context->Init(cluster, table);
  info_context->request->set_type(type);
  if (!table.empty()) {
    info_context->request->mutable_info()->set_table_name(table);
  }
}

static void ClearDistributeMap(std::map<Node, CmdContext*>* key_distribute) {
  for (auto& kd : *key_distribute) {
    delete kd.second;
  }
}

Cluster::Cluster(const Options& options)
  : epoch_(-1) {
    options_ = options;
    Init();
  }

Cluster::Cluster(const std::string& ip, int port)
  : epoch_(-1) {
    Options opt;
    opt.meta_addr.push_back(Node(ip, port));
    options_ = opt;
    Init();
  }

void Cluster::Init() {
    meta_pool_ = new ConnectionPool(8);
    data_pool_ = new ConnectionPool(options_.connection_pool_capacity);
    meta_cmd_ = new ZPMeta::MetaCmd();
    meta_res_ = new ZPMeta::MetaCmdResponse();
    context_ = new CmdContext();
    async_worker_ = new pink::BGThread();

    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&meta_rw_, &attr);
}

Cluster::~Cluster() {
    
  delete async_worker_;
  {
    slash::MutexLock l(&peer_mu_);
    for (auto& bg : peer_workers_) {
      delete bg.second;
      }
  }

  {
    slash::RWLock l(&meta_rw_, true);
    std::unordered_map<std::string, Table*>::iterator iter = tables_.begin();
    while (iter != tables_.end()) {
      delete iter->second;
      iter++;
    }
  }
  
  pthread_rwlock_destroy(&meta_rw_);
  delete context_;
  delete meta_res_;
  delete meta_cmd_;
  delete data_pool_;
  delete meta_pool_;
}

Status Cluster::Connect() {
  Status s;
  std::shared_ptr<ZpCli> meta_cli =
    GetMetaConnection(CalcDeadline(options_.op_timeout), &s);
  if (meta_cli == NULL) {
    return s;
  }
  return Status::OK();
}

Status Cluster::Set(const std::string& table, const std::string& key,
    const std::string& value, int32_t ttl) {
  BuildSetContext(this, table, key, value, ttl, context_);
  DeliverAndPull(context_);

  if (!context_->result.ok()) {
    return context_->result;
  }
  if (context_->response->code() == client::StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::Corruption(context_->response->msg());
  }
}

Status Cluster::Delete(const std::string& table, const std::string& key) {
  BuildDeleteContext(this, table, key, context_);
  DeliverAndPull(context_);

  if (!context_->result.ok()) {
    return context_->result;
  }
  if (context_->response->code() == client::StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::Corruption(context_->response->msg());
  }
}

Status Cluster::Get(const std::string& table, const std::string& key,
    std::string* value) {
  BuildGetContext(this, table, key, context_);
  DeliverAndPull(context_);
  
  if (!context_->result.ok()) {
    return context_->result;
  }
  if (context_->response->code() == client::StatusCode::kOk) {
    client::CmdResponse_Get info = context_->response->get();
    value->assign(info.value().data(), info.value().size());
    return Status::OK();
  } else if (context_->response->code() == client::StatusCode::kNotFound) {
    return Status::NotFound("key do not exist");
  } else {
    return Status::Corruption(context_->response->msg());
  }
}

Status Cluster::Mget(const std::string& table,
    const std::vector<std::string>& keys,
    std::map<std::string, std::string>* values) {
  if (keys.empty() || values == NULL) {
    return Status::InvalidArgument("Null pointer");
  }

  BuildMgetContext(this, table, keys, context_);
  DeliverAndPull(context_);
  

  if (!context_->result.ok()) {
    return context_->result;
  }
  if (context_->response->code() == client::StatusCode::kOk) {
    for (auto& kv : context_->response->mget()) {
      values->insert(std::pair<std::string, std::string>(kv.key(), kv.value()));
    }
    return Status::OK();
  } else {
    return Status::Corruption(context_->response->msg());
  }
}

Status Cluster::Aset(const std::string& table, const std::string& key,
    const std::string& value, zp_completion_t complietion, void* data,
    int32_t ttl) {
  CmdContext* context = new CmdContext();
  BuildSetContext(this, table, key, value, ttl, context, complietion, data);
  AddAsyncTask(context);
  return Status::OK();
}

Status Cluster::Aget(const std::string& table, const std::string& key,
    zp_completion_t complietion, void* data) {
  CmdContext* context = new CmdContext();
  BuildGetContext(this, table, key, context, complietion, data);
  AddAsyncTask(context);
  return Status::OK();
}

Status Cluster::Adelete(const std::string& table, const std::string& key,
    zp_completion_t complietion, void* data) {
  CmdContext* context = new CmdContext();
  BuildDeleteContext(this, table, key, context, complietion, data);
  AddAsyncTask(context);
  return Status::OK();
}

Status Cluster::Amget(const std::string& table, const std::vector<std::string>& keys,
    zp_completion_t complietion, void* data) {
  if (keys.empty()) {
    return Status::InvalidArgument("Empty keys");
  }
  CmdContext* context = new CmdContext();
  BuildMgetContext(this, table, keys, context, complietion, data);
  AddAsyncTask(context);
  return Status::OK();
}

bool Cluster::DeliverMget(CmdContext* context) {
  // Prepare Request
  Node master;
  std::map<Node, CmdContext*> key_distribute;
  for (auto& k : context->request->mget().keys()) {
    context->result = GetDataMaster(context->table, k, &master);
    if (!context->result.ok()) {
      ClearDistributeMap(&key_distribute);
      return false;
    }

    if (key_distribute.find(master) == key_distribute.end()) {
      CmdContext* sub_context = new CmdContext();
      sub_context->Init(this, context->table, k);
      sub_context->deadline = context->deadline;
      sub_context->request->set_type(client::Type::MGET);
      client::CmdRequest_Mget* new_mget_cmd = sub_context->request->mutable_mget();
      new_mget_cmd->set_table_name(context->table);
      key_distribute.insert(std::pair<Node, CmdContext*>(
            master, sub_context));
    }
    key_distribute[master]->request->mutable_mget()->add_keys(k);
  }

  // Dispatch
  for (auto& kd : key_distribute) {
    AddNodeTask(kd.first, kd.second);
  }

  for (auto& kd : key_distribute) {
    kd.second->WaitRpcDone();
  }

  // Wait peer_workers process and merge result
  context->response->set_type(client::Type::MGET);
  for (auto& kd : key_distribute) {
    context->key = kd.second->key;
    context->result = kd.second->result;
    context->response->set_code(kd.second->response->code());
    context->response->set_msg(kd.second->response->msg());
    if (kd.second->response->has_redirect()) {
      client::Node* node =  context->response->mutable_redirect();
      *node = kd.second->response->redirect();
    }
    if (!context->result.ok()
        || context->response->code() != client::StatusCode::kOk) { // no NOTFOUND in mget response
      ClearDistributeMap(&key_distribute);
      return false;
    }
    for (auto& kv : kd.second->response->mget()) {
      client::CmdResponse_Mget* res_mget= context->response->add_mget();
      res_mget->set_key(kv.key());
      res_mget->set_value(kv.value());
    }
  }
  ClearDistributeMap(&key_distribute);
  return true;
}

bool Cluster::Deliver(CmdContext* context) {
  if (context->request->type() == client::Type::MGET) {
    return DeliverMget(context); 
  }

  // Prepare Request
  Node master;
  if (context->partition_id >= 0) {
    // specified partition_id
    context->result = GetDataMasterById(context->table,
        context->partition_id, &master);
  } else {
    context->result = GetDataMaster(context->table, context->key, &master);
  }
  if (!context->result.ok()) {
    return false;
  }
  
  context->result = SubmitDataCmd(master,
      *(context->request), context->response, context->deadline);

  if (context->result.ok()
        && (context->response->code() == client::StatusCode::kOk
          || context->response->code() == client::StatusCode::kNotFound)) {
    return true; //succ
  }
  return false;
}

void Cluster::DeliverAndPull(CmdContext* context) {
  CmdContextResult initiator;  
  while (!Deliver(context)) {
    if (initiator.empty()) {
      initiator.Fill(context->result,
          context->response->code(), context->response->msg());
    }

    if (context->result.IsTimeout()) {
      // recover initiator result
      context->SetResult(initiator);
      return;
    }
    
    bool need_pull = false;
    if (context->response->code() == client::StatusCode::kMove
        && context->response->has_redirect()) {
      // Update meta info with redirect message
      if (context->partition_id >= 0) {
        need_pull = !(UpdateDataMasterById(context->table, context->partition_id,
              Node(context->response->redirect().ip(),
                context->response->redirect().port())).ok());
      } else {
        need_pull = !(UpdateDataMaster(context->table, context->key,
              Node(context->response->redirect().ip(),
                context->response->redirect().port())).ok());
      }
    } else if (context->response->code() == client::StatusCode::kWait) {
      // might be solved by just wait
    } else {
      need_pull = true;
    }
    
    if (need_pull) {
      // Refresh meta info on error except kWait
      context->result = PullInternal(context->table, context->deadline);

      if (context->result.IsTimeout()) {
        context->SetResult(initiator);
        return;
      } else if (!context->result.ok()) {
        // No need to retry when pull failed
        return;
      }
    }

    context->Reset();
  }
}

void Cluster::DoAsyncTask(void* arg) {
  CmdContext *carg = static_cast<CmdContext*>(arg);
  carg->cluster->DeliverAndPull(carg);

  // Callback zp_completion_t
  std::string value;
  std::map<std::string, std::string> kvs;
  switch (carg->request->type()) {
    case client::Type::SET:
    case client::Type::DEL:
      carg->completion(Result(carg->result, carg->key), carg->user_data);
      break;
    case client::Type::GET:
      value = carg->response->get().value();
      carg->completion(Result(carg->result, carg->key, &value),
          carg->user_data);
      break;
    case client::Type::MGET:
      kvs.clear();
      for (auto& kv : carg->response->mget()) {
        kvs.insert(std::pair<std::string, std::string>(kv.key(), kv.value()));
      }
      carg->completion(Result(carg->result, &kvs),
          carg->user_data);
      break;
    default:
      break;
  }
  delete carg;
}

void Cluster::AddAsyncTask(CmdContext* context) {
  async_worker_->StartThread();
  async_worker_->Schedule(DoAsyncTask, context);
}

void Cluster::DoNodeTask(void* arg) {
  NodeTaskArg* task_arg = static_cast<NodeTaskArg*>(arg);
  CmdContext *carg = task_arg->context;
  carg->result = carg->cluster->SubmitDataCmd(task_arg->target,
      *(carg->request), carg->response, carg->deadline);
  carg->RpcDone();
  delete task_arg;
}

void Cluster::AddNodeTask(const Node& node, CmdContext* context) {
  slash::MutexLock l(&peer_mu_);
  if (peer_workers_.find(node) == peer_workers_.end()) {
    pink::BGThread* bg = new pink::BGThread();
    bg->StartThread();
    peer_workers_.insert(std::pair<Node, pink::BGThread*>(node, bg));
  }
  NodeTaskArg* arg = new NodeTaskArg(context, node);
  peer_workers_[node]->Schedule(DoNodeTask, arg);
}

Status Cluster::FlushTable(const std::string& table_name) {
  Table table;
  Status s = FetchMetaInfo(table_name, &table);
  if (!s.ok()) {
    return s;
  }

  for (int id = 0; id < table.partition_num(); ++id) {
    BuildFlushTableContext(this, table_name, id, context_);
    DeliverAndPull(context_);
    if (!context_->result.ok()) {
      return context_->result;
    }
    if (context_->response->code() != client::StatusCode::kOk) {
      return Status::Corruption(context_->response->msg());
    }
  }
  return Status::OK();
}

Status Cluster::CreateTable(const std::string& table_name,
    const std::vector<std::vector<Node>>& distribution) {
  if (distribution.empty()) {
    return Status::InvalidArgument("Empty distribution");
  }

  std::vector<Node> nodes;
  std::vector<std::string> status;
  Status s = ListNode(&nodes, &status);
  if (!s.ok()) {
    return s;
  }
  // For check nodes aliveness
  std::set<Node> alive_nodes;
  assert(nodes.size() == status.size());
  for (size_t i = 0; i < nodes.size(); i++) {
    if (status[i] == "up") {
      alive_nodes.insert(nodes[i]);
    }
  }

  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::INIT);
  ZPMeta::Table* table = meta_cmd_->mutable_init()->mutable_table();
  table->set_name(table_name);
  size_t master_index = 0;
  for (size_t i = 0; i < distribution.size(); i++) {
    const std::vector<Node>& partition = distribution[i];
    master_index = (master_index + 1) % partition.size();
    ZPMeta::Partitions* p = table->add_partitions();
    p->set_id(i);
    p->set_state(ZPMeta::PState::ACTIVE);

    if (partition.empty()) {
      return Status::Corruption("Empty partition");
    }

    for (size_t j = 0; j < partition.size(); j++) {
      const Node& node = partition[j];
      if (alive_nodes.find(node) == alive_nodes.end()) {
        return Status::Corruption("Node " + node.ToString() + " is down");
      }
      ZPMeta::Node* n = j == master_index ?
        p->mutable_master() : p->add_slaves();
      n->set_ip(node.ip);
      n->set_port(node.port);
    }
  }

  // // Debug dump
  // for (int i = 0; i < table->partitions_size(); i++) {
  //   const ZPMeta::Partitions& p = table->partitions(i);
  //   const ZPMeta::Node& master = p.master();
  //   printf("Partition: %d, master %s:%d ", p.id(), master.ip().c_str(), master.port());
  //   for (int j = 0; j < p.slaves_size(); j++) {
  //     const ZPMeta::Node& slave = p.slaves(j);
  //     printf("slave%d: %s:%d ", j, slave.ip().c_str(), slave.port());
  //   }
  //   printf("\n");
  // }

  Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::DropTable(const std::string& table_name) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::DROPTABLE);
  ZPMeta::MetaCmd_DropTable* drop_table = meta_cmd_->mutable_drop_table();
  drop_table->set_name(table_name);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::Pull(const std::string& table) {
  return PullInternal(table, CalcDeadline(options_.op_timeout));
}

Status Cluster::PullInternal(const std::string& table, uint64_t deadline) {
  // Pull is different with other meta command
  // Since it may be called both by user thread and async thread
  ZPMeta::MetaCmd meta_cmd;
  ZPMeta::MetaCmdResponse meta_res;
  meta_cmd.set_type(ZPMeta::Type::PULL);
  ZPMeta::MetaCmd_Pull* pull = meta_cmd.mutable_pull();
  pull->set_name(table);

  slash::Status ret = SubmitMetaCmd(meta_cmd, &meta_res, deadline);
  if (!ret.ok()) {
    return ret;
  }

  if (meta_res.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res.msg());
  }

  // Update clustermap now
  ResetMetaInfo(table, meta_res.pull());
  return Status::OK();
}

Status Cluster::FetchMetaInfo(const std::string& table, Table* table_meta) {
  slash::Status s = PullInternal(table, CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }
  slash::RWLock l(&meta_rw_, false);
  if (tables_.find(table) == tables_.end()) {
    return Status::InvalidArgument("table not found");
  }
  *table_meta = *(tables_[table]);
  return Status::OK();
}

Status Cluster::SetMaster(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::SETMASTER);
  ZPMeta::MetaCmd_SetMaster* set_master_cmd = meta_cmd_->mutable_set_master();
  ZPMeta::BasicCmdUnit* set_master_entity = set_master_cmd->mutable_basic();
  set_master_entity->set_name(table_name);
  set_master_entity->set_partition(partition_num);
  ZPMeta::Node* node = set_master_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::AddSlave(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::ADDSLAVE);
  ZPMeta::MetaCmd_AddSlave* add_slave_cmd = meta_cmd_->mutable_add_slave();
  ZPMeta::BasicCmdUnit* add_slave_entity = add_slave_cmd->mutable_basic();
  add_slave_entity->set_name(table_name);
  add_slave_entity->set_partition(partition_num);
  ZPMeta::Node* node = add_slave_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::RemoveSlave(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::REMOVESLAVE);
  ZPMeta::MetaCmd_RemoveSlave* remove_slave_cmd =
    meta_cmd_->mutable_remove_slave();
  ZPMeta::BasicCmdUnit* remove_slave_entity = remove_slave_cmd->mutable_basic();
  remove_slave_entity->set_name(table_name);
  remove_slave_entity->set_partition(partition_num);
  ZPMeta::Node* node = remove_slave_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::RemoveNodes(const std::vector<Node>& nodes) {
  // Check node load before send remove command to meta
  std::vector<std::string> tables;
  Status s = ListTable(&tables);
  if (!s.ok()) {
    return s;
  }
  for (auto& table : tables) {
    s = Pull(table);
    if (!s.ok()) {
      return s;
    }
    Table* table_ptr = tables_[table];
    if (table_ptr == nullptr) {
      return Status::Corruption("Can not find table_ptr");
    }
    std::map<Node, std::vector<const Partition*>> nodes_loads;
    table_ptr->GetNodesLoads(&nodes_loads);
    for (auto& node : nodes) {
      auto node_iter = nodes_loads.find(node);
      if (node_iter != nodes_loads.end() &&
          !node_iter->second.empty()) {
        return Status::Corruption("Can not remove nonempty node: " + node.ToString());
      }
    }
  }

  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::REMOVENODES);
  ZPMeta::MetaCmd_RemoveNodes* remove_nodes_cmd =
    meta_cmd_->mutable_remove_nodes();

  for (const auto& n : nodes) {
    ZPMeta::Node* node = remove_nodes_cmd->add_nodes();
    node->set_ip(n.ip);
    node->set_port(n.port);
  }

  printf("Remove nodes:\n");
  for (int i = 0; i < remove_nodes_cmd->nodes_size(); i++) {
    auto node = remove_nodes_cmd->nodes(i);
    printf(" --- %s:%d\n", node.ip().c_str(), node.port());
  }

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));
  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

namespace {

struct NodeWithLoad {
  Node node;
  std::vector<const Partition*> partitions;
};

int NodeLoad(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& n)  {
  auto iter = nodes_map.find(n.ip);
  if (iter != nodes_map.end()) {
    for (auto& nwl : iter->second) {
      if (nwl->node == n) {
        return nwl->partitions.size();
      }
    }
  }
  return 0;
}

std::shared_ptr<NodeWithLoad> FindNodeWithLoad(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& node) {
  assert(nodes_map.find(node.ip) != nodes_map.end());
  for (auto nwl : nodes_map.at(node.ip)) {
    if (nwl->node == node) {
      return nwl;
    }
  }
  return nullptr;
}

int ParCountOnHost(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& src_node, int par_id) {
  assert(nodes_map.find(src_node.ip) != nodes_map.end());
  const std::vector<std::shared_ptr<NodeWithLoad>>& n_vec = nodes_map.at(src_node.ip);
  int count = 0;
  for (auto& nwl : n_vec) {
    for (auto p : nwl->partitions) {
      if (p->id() == par_id) {
        count++;
      }
    }
  }
  return count;
}

bool ParExistOnNode(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& node, int par_id) {
  const auto nwl = FindNodeWithLoad(nodes_map, node);
  for (const auto p : nwl->partitions) {
    if (p->id() == par_id) {
      return true;
    }
  }
  return false;
}

const Partition* SelectOnePartition(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& src_node,
    const Node& dst_node) {
  const auto nwl = FindNodeWithLoad(nodes_map, src_node);
  if (nodes_map.size() == 1) {
    assert(nwl->partitions.size() > 0);
    for (const auto p : nwl->partitions) {
      if (ParExistOnNode(nodes_map, dst_node, p->id())) {
        continue;
      }
      return p;
    }
  } else if (nodes_map.size() == 2) {
    for (const auto p : nwl->partitions) {
      if (ParExistOnNode(nodes_map, dst_node, p->id())) {
        continue;
      }
      int load = ParCountOnHost(nodes_map, dst_node, p->id());
      if (load == 2) {
        continue;
      }
      return p;
    }
  } else {
    for (const auto p : nwl->partitions) {
      if (ParExistOnNode(nodes_map, dst_node, p->id())) {
        continue;
      }
      int load = ParCountOnHost(nodes_map, dst_node, p->id());
      if (load == 1) {
        continue;
      }
      return p;
    }
  }
  return nullptr;
}

bool IsValidDstNode(
    const std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>& nodes_map,
    const Node& dst_node,
    int par_id) {
  if (ParExistOnNode(nodes_map, dst_node, par_id)) {
    return false;
  }
  if (nodes_map.size() == 1) {
    return true;
  } else if (nodes_map.size() == 2) {
    int load = ParCountOnHost(nodes_map, dst_node, par_id);
    if (load <= 1) {
      return true;
    }
  } else {
    int load = ParCountOnHost(nodes_map, dst_node, par_id);
    if (load == 0) {
      return true;
    }
  }
  return false;
}

}

Status Cluster::Expand(
    const std::string& table,
    const std::vector<Node>& new_nodes) {
  Status s;
  if (new_nodes.empty()) {
    return s;  // OK
  }
  s = PullInternal(table, CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }
  if (tables_.find(table) == tables_.end()) {
    return Status::InvalidArgument("table not fount");
  }

  auto cmp = [](std::shared_ptr<NodeWithLoad> left,
                std::shared_ptr<NodeWithLoad> right) {
    return left->partitions.size() < right->partitions.size();
  };
  std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>
    nodes_map;  // new nodes and origin nodes
  std::priority_queue<std::shared_ptr<NodeWithLoad>,
                      std::vector<std::shared_ptr<NodeWithLoad>>,
                      decltype(cmp)> src_nodes_queue(cmp);

  // Get origin and new nodes loads
  Table* table_ptr = tables_[table];
  std::map<Node, std::vector<const Partition*>> nodes_loads;
  table_ptr->GetNodesLoads(&nodes_loads);
  for (auto& nn : new_nodes) {
    if (nodes_loads.find(nn) != nodes_loads.end()) {
      continue;  // Exist node
    }
    std::shared_ptr<NodeWithLoad>
      item(new NodeWithLoad{nn, std::vector<const Partition*>{}});
    if (nodes_map.find(nn.ip) != nodes_map.end()) {
      nodes_map[nn.ip].push_back(item);
    } else {
      nodes_map.insert(
        std::make_pair(nn.ip, std::vector<std::shared_ptr<NodeWithLoad>>{item}));
    }
  }

  // Calculate average load and restriction
  int total_load = 0;
  for (auto& n : nodes_loads) {
    total_load += n.second.size();
    std::shared_ptr<NodeWithLoad> nwl(new NodeWithLoad{n.first, n.second});
    if (nodes_map.find(n.first.ip) != nodes_map.end()) {
      nodes_map[n.first.ip].push_back(nwl);
    } else {
      nodes_map.insert(std::make_pair(
        n.first.ip, std::vector<std::shared_ptr<NodeWithLoad>>{nwl}));
    }
    src_nodes_queue.push(nwl);
  }
  double average = static_cast<double>(total_load) /
    (nodes_loads.size() + new_nodes.size());
  int ceil_of_load = static_cast<int>(std::ceil(average));
  int floor_of_load = static_cast<int>(std::floor(average));

  // Debug: dump nodes_map
  for (auto& host : nodes_map) {
    printf("Host: %s\n", host.first.c_str());
    for (auto& node_with_load : host.second) {
      printf("\tNode: %s --- ", node_with_load->node.ToString().c_str());
      if (nodes_loads.find(node_with_load->node) == nodes_loads.end()) {
        printf("{}\n");
        continue;
      }
      printf("{");
      size_t i = 0;
      for (; i < node_with_load->partitions.size() - 1; i++) {
        printf("%d, ", node_with_load->partitions[i]->id());
      }
      printf("%d}\n", node_with_load->partitions[i]->id());
    }
  }

  // Init protobuf
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::MIGRATE);
  ZPMeta::MetaCmd_Migrate* migrate_cmd = meta_cmd_->mutable_migrate();
  migrate_cmd->set_origin_epoch(epoch_);

  for (auto& dst_node : new_nodes) {
    std::vector<std::shared_ptr<NodeWithLoad>> nodes_buffer;
    while (!src_nodes_queue.empty()) {
      auto nwl = src_nodes_queue.top();
      src_nodes_queue.pop();
      const Node& src_node = nwl->node;
      int load1 = NodeLoad(nodes_map, dst_node);
      int load2 = NodeLoad(nodes_map, src_node);
      if (load1 >= ceil_of_load) {
        src_nodes_queue.push(nwl);
        break;
      }
      if (load2 <= floor_of_load) {
        continue;
      }
      const Partition* p =
        SelectOnePartition(nodes_map, src_node, dst_node);
      if (p != nullptr) {
        // Build cmd proto
        auto cmd_unit = migrate_cmd->add_diff();
        cmd_unit->set_table(table);
        cmd_unit->set_partition(p->id());
        auto l = cmd_unit->mutable_left();
        l->set_ip(src_node.ip);
        l->set_port(src_node.port);
        auto r = cmd_unit->mutable_right();
        r->set_ip(dst_node.ip);
        r->set_port(dst_node.port);

        auto iter = nwl->partitions.begin();
        while (iter != nwl->partitions.end()) {
          if ((*iter)->id() == p->id()) {
            nwl->partitions.erase(iter);
            break;
          }
          iter++;
        }
        auto nwl1 = FindNodeWithLoad(nodes_map, dst_node);
        nwl1->partitions.push_back(p);

        src_nodes_queue.push(nwl);
      } else {
        nodes_buffer.push_back(nwl);
      }
    }
    for (auto nb : nodes_buffer) {
      src_nodes_queue.push(nb);
    }
  }

  // Dump migrate result
  printf("Move numbers: %d\n", migrate_cmd->diff_size());
  for (int i = 0; i < migrate_cmd->diff_size(); i++) {
    auto diff = migrate_cmd->diff(i);
    auto l = diff.left();
    auto r = diff.right();
    printf("Move table(%s) %s:%d - %d => %s:%d\n", diff.table().c_str(),
           l.ip().c_str(), l.port(), diff.partition(),
           r.ip().c_str(), r.port());
  }

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                    CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::Migrate(const std::string& table,
    const Node& src_node, int partition_id, const Node& dst_node) {
  Status s = PullInternal(table, CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }
  if (tables_.find(table) == tables_.end()) {
    return Status::InvalidArgument("table not fount");
  }

  Table* table_ptr = tables_[table];
  std::map<Node, std::vector<const Partition*>> nodes_loads;
  table_ptr->GetNodesLoads(&nodes_loads);

  if (nodes_loads.find(src_node) == nodes_loads.end() ||
      nodes_loads.find(dst_node) == nodes_loads.end()) {
    return Status::Corruption("Invalid node");
  }
  for (const auto p : nodes_loads[dst_node]) {
    if (p->id() == partition_id) {
      return Status::Corruption("Invalid Move");
    }
  }

  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::MIGRATE);
  ZPMeta::MetaCmd_Migrate* migrate_cmd = meta_cmd_->mutable_migrate();
  migrate_cmd->set_origin_epoch(epoch_);

  // Build cmd proto
  auto cmd_unit = migrate_cmd->add_diff();
  cmd_unit->set_table(table);
  cmd_unit->set_partition(partition_id);
  auto l = cmd_unit->mutable_left();
  l->set_ip(src_node.ip);
  l->set_port(src_node.port);
  auto r = cmd_unit->mutable_right();
  r->set_ip(dst_node.ip);
  r->set_port(dst_node.port);

  auto diff = migrate_cmd->diff(0);
  printf("Move %s:%d - %d => %s:%d\n",
         diff.left().ip().c_str(), diff.left().port(),
         diff.partition(),
         diff.right().ip().c_str(), diff.right().port());

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                    CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::Shrink(const std::string& table, const std::vector<Node>& deleting) {
  Status s;
  if (deleting.empty()) {
    return s; // OK
  }
  s = PullInternal(table, CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }
  if (tables_.find(table) == tables_.end()) {
    return Status::InvalidArgument("table not fount");
  }

  auto cmp = [](std::shared_ptr<NodeWithLoad> left,
                std::shared_ptr<NodeWithLoad> right) {
    return left->partitions.size() > right->partitions.size();
  };
  std::map<std::string, std::vector<std::shared_ptr<NodeWithLoad>>>
    nodes_map;  // new nodes and origin nodes
  std::priority_queue<std::shared_ptr<NodeWithLoad>,
                      std::vector<std::shared_ptr<NodeWithLoad>>,
                      decltype(cmp)> dst_nodes_queue(cmp);

  // Get origin and new nodes loads
  Table* table_ptr = tables_[table];
  std::map<Node, std::vector<const Partition*>> nodes_loads;
  std::map<Node, std::vector<const Partition*>> nodes_tobe_deleted;
  table_ptr->GetNodesLoads(&nodes_loads);

  // Checking
  for (auto& n : deleting) {
    auto iter = nodes_loads.find(n);
    if (iter == nodes_loads.end()) {
      return Status::NotFound(n.ToString());
    }
    nodes_tobe_deleted.insert(*iter);
    nodes_loads.erase(iter);
  }

  // Calculate average load and restriction
  for (auto& n : nodes_loads) {
    std::shared_ptr<NodeWithLoad> nwl(new NodeWithLoad{n.first, n.second});
    if (nodes_map.find(n.first.ip) != nodes_map.end()) {
      nodes_map[n.first.ip].push_back(nwl);
    } else {
      nodes_map.insert(std::make_pair(
        n.first.ip, std::vector<std::shared_ptr<NodeWithLoad>>{nwl}));
    }
    dst_nodes_queue.push(nwl);
  }

  // Debug: dump nodes_map
#if 0
  for (auto& host : nodes_map) {
    printf("Host: %s\n", host.first.c_str());
    for (auto& node_with_load : host.second) {
      printf("\tNode: %s --- ", node_with_load->node.ToString().c_str());
      if (nodes_loads.find(node_with_load->node) == nodes_loads.end()) {
        printf("{}\n");
        continue;
      }
      printf("{");
      size_t i = 0;
      for (; i < node_with_load->partitions.size() - 1; i++) {
        printf("%d, ", node_with_load->partitions[i]->id());
      }
      printf("%d}\n", node_with_load->partitions[i]->id());
    }
  }
#endif

  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::MIGRATE);
  ZPMeta::MetaCmd_Migrate* migrate_cmd = meta_cmd_->mutable_migrate();
  migrate_cmd->set_origin_epoch(epoch_);

  for (auto& n : nodes_tobe_deleted) {
    const Node& src_node = n.first;
    for (auto p : n.second) {
      std::vector<std::shared_ptr<NodeWithLoad>> nodes_buffer;
      while (true && !dst_nodes_queue.empty()) {
        auto nwl = dst_nodes_queue.top();
        const Node& dst_node = nwl->node;
        dst_nodes_queue.pop();
        if (IsValidDstNode(nodes_map, dst_node, p->id())) {
          // Build cmd proto
          auto cmd_unit = migrate_cmd->add_diff();
          cmd_unit->set_table(table);
          cmd_unit->set_partition(p->id());
          auto l = cmd_unit->mutable_left();
          l->set_ip(src_node.ip);
          l->set_port(src_node.port);
          auto r = cmd_unit->mutable_right();
          r->set_ip(dst_node.ip);
          r->set_port(dst_node.port);

          nwl->partitions.push_back(p);

          dst_nodes_queue.push(nwl);
          break;
        } else {
          nodes_buffer.push_back(nwl);
        }
      }
      for (auto nb : nodes_buffer) {
        dst_nodes_queue.push(nb);
      }
    }
  }

  // Dump migrate result
  for (int i = 0; i < migrate_cmd->diff_size(); i++) {
    auto diff = migrate_cmd->diff(i);
    auto l = diff.left();
    auto r = diff.right();
    printf("Move table(%s) %s:%d - %d => %s:%d\n",diff.table().c_str(),
           l.ip().c_str(), l.port(), diff.partition(),
           r.ip().c_str(), r.port());
  }
  if (migrate_cmd->diff_size() == 0) {
    return Status::Corruption("There is no reasonable way");
  }

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                    CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::ReplaceNode(const Node& ori_node, const Node& dst_node) {
  // Check dst_node
  std::vector<Node> nodes;
  std::vector<std::string> status;
  Status s = ListNode(&nodes, &status);
  if (!s.ok()) {
    return s;
  }
  assert(nodes.size() == status.size());
  size_t i = 0;
  for (; i < nodes.size(); i++) {
    if (nodes[i] == dst_node) {
      break;
    }
  }
  if (i >= nodes.size()) {
    return Status::Corruption("Cannot find dst_node: " + dst_node.ToString());
  } else if (status[i] != "up") {
    return Status::Corruption("dst_node is offline");
  }

  // Acquire origin nodes' infomation
  std::vector<std::string> tables;
  s = ListTable(&tables);
  if (!s.ok()) {
    return s;
  }
  for (auto& table : tables) {
    s = PullInternal(table, CalcDeadline(options_.op_timeout));
    if (!s.ok()) {
      return s;
    }
  }

  // Init protobuf
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::MIGRATE);
  ZPMeta::MetaCmd_Migrate* migrate_cmd = meta_cmd_->mutable_migrate();
  migrate_cmd->set_origin_epoch(epoch_);

  for (auto& table : tables) {
    Table* tbl_ptr = tables_[table];
    std::map<Node, std::vector<const Partition*>> nodes_loads;
    tbl_ptr->GetNodesLoads(&nodes_loads);
    auto item = nodes_loads.find(ori_node);
    if (item != nodes_loads.end()) {
      for (auto par : item->second) {
        auto cmd_unit = migrate_cmd->add_diff();
        cmd_unit->set_table(table);
        cmd_unit->set_partition(par->id());
        auto l = cmd_unit->mutable_left();
        l->set_ip(ori_node.ip);
        l->set_port(ori_node.port);
        auto r = cmd_unit->mutable_right();
        r->set_ip(dst_node.ip);
        r->set_port(dst_node.port);
      }
    }
  }

  // Dump migrate result
  printf("Move numbers: %d\n", migrate_cmd->diff_size());
  for (int i = 0; i < migrate_cmd->diff_size(); i++) {
    auto diff = migrate_cmd->diff(i);
    auto l = diff.left();
    auto r = diff.right();
    printf("Move table(%s) %s:%d - %d => %s:%d\n", diff.table().c_str(),
           l.ip().c_str(), l.port(), diff.partition(),
           r.ip().c_str(), r.port());
  }

  printf("Continue? (Y/N)\n");
  char confirm = getchar(); getchar();  // ignore \n
  if (std::tolower(confirm) != 'y') {
    return Status::Incomplete("Abort");
  }

  s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                    CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::CancelMigrate() {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::CANCELMIGRATE);

  Status s = SubmitMetaCmd(*meta_cmd_, meta_res_,
                           CalcDeadline(options_.op_timeout));
  if (!s.ok()) {
    return s;
  }

  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  return s;
}

Status Cluster::ListMeta(Node* master, std::vector<Node>* nodes) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::LISTMETA);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  ZPMeta::MetaNodes info = meta_res_->list_meta().nodes();
  master->ip = info.leader().ip();
  master->port = info.leader().port();
  for (int i = 0; i < info.followers_size(); i++) {
    Node slave_node;
    slave_node.ip = info.followers(i).ip();
    slave_node.port = info.followers(i).port();
    nodes->push_back(slave_node);
  }
  return Status::OK();
}

Status Cluster::MetaStatus(Node* leader, std::map<Node, std::string>* meta_status) {
  std::vector<libzp::Node> followers;
  Status ret = ListMeta(leader, &followers);
  if (!ret.ok()) {
    return ret;
  }

  (*meta_status)[*leader] = "Unknow";
  for (auto& follower : followers) {
    (*meta_status)[follower] = "Unknow";
  }

  int32_t version;
  int64_t begin_time;
  int32_t complete_proportion;
  std::string consistency_stautus;
  ret = MetaStatus(&version, &consistency_stautus,
                   &begin_time, &complete_proportion);
  if (!ret.ok()) {
    return ret;
  }

  std::istringstream input(consistency_stautus);
  for (std::string line; std::getline(input, line); ) {
    if (line.find(":") == std::string::npos) {
      continue;  // Skip header
    }
    std::string ip;
    int port;
    std::istringstream line_s(line);
    for (std::string word; std::getline(line_s, word, ' '); ) {
      if (word.find(":") != std::string::npos) {
        if (slash::ParseIpPortString(word, ip, port)) {
          Node n(ip, port - 100);
          (*meta_status)[n] = "Up";
          break;
        }
      }
    }
  }

  for (auto& item : *meta_status) {
    if (item.second != "Up") {
      item.second = "Down";  // Change 'Unknow' to 'Down'
    }
  }

  return ret;
}

Status Cluster::MetaStatus(std::string* consistency_stautus) {
  int32_t version;
  int64_t begin_time;
  int32_t complete_proportion;
  return MetaStatus(&version, consistency_stautus,
                    &begin_time, &complete_proportion);
}

Status Cluster::MetaStatus(int32_t* version,
                           std::string* consistency_stautus,
                           int64_t* begin_time,
                           int32_t* complete_proportion) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::METASTATUS);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  const ZPMeta::MetaCmdResponse_MetaStatus& mstatus = meta_res_->meta_status();
  *version = mstatus.version();
  *consistency_stautus = mstatus.consistency_stautus();

  const ZPMeta::MigrateStatus& migrate_status = mstatus.migrate_status();
  *begin_time = migrate_status.begin_time();
  *complete_proportion = migrate_status.complete_proportion();

  return Status::OK();
}

Status Cluster::ListNode(std::vector<Node>* nodes,
    std::vector<std::string>* status) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::LISTNODE);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  ZPMeta::Nodes info = meta_res_->list_node().nodes();
  for (int i = 0; i < info.nodes_size(); i++) {
    Node data_node;
    data_node.ip = info.nodes(i).node().ip();
    data_node.port = info.nodes(i).node().port();
    nodes->push_back(data_node);
    if (info.nodes(i).status() == 1) {
      status->push_back("down");
    } else {
      status->push_back("up");
    }
  }
  return Status::OK();
}

Status Cluster::ListTable(std::vector<std::string>* tables) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::LISTTABLE);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_,
      CalcDeadline(options_.op_timeout));

  if (!ret.ok()) {
    return ret;
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  }

  ZPMeta::TableName info = meta_res_->list_table().tables();
  for (int i = 0; i < info.name_size(); i++) {
    tables->push_back(info.name(i));
  }
  return Status::OK();
}

Status Cluster::InfoQps(const std::string& table,
    int32_t* qps, int64_t* total_query) {
  PullInternal(table, CalcDeadline(options_.op_timeout));
  std::set<Node> related_nodes;
  Status s = GetTableMasters(table, &related_nodes);
  if (!s.ok()) {
    return s;
  }

  *qps = *total_query = 0;
  for (auto& node : related_nodes) {
    BuildInfoContext(this, table, client::Type::INFOSTATS, context_);
    context_->result = SubmitDataCmd(node,
        *(context_->request), context_->response, context_->deadline);
    if (!context_->result.ok()) {
      continue;
    }
    *qps += context_->response->info_stats(0).qps();
    *total_query += context_->response->info_stats(0).total_querys();
  }
  return Status::OK();
}

Status Cluster::InfoLatency(
    const std::string& table, std::map<Node, std::string>* latency_info) {
  PullInternal(table, CalcDeadline(options_.op_timeout));
  std::set<Node> related_nodes;
  Status s = GetTableMasters(table, &related_nodes);
  if (!s.ok()) {
    return s;
  }

  for (auto& node : related_nodes) {
    BuildInfoContext(this, table, client::Type::INFOSTATS, context_);
    context_->result = SubmitDataCmd(node,
        *(context_->request), context_->response, context_->deadline);
    if (!context_->result.ok()) {
      continue;
    }
    const std::string& lat_info =
      context_->response->info_stats(0).latency_info();
    latency_info->insert(std::make_pair(node, lat_info));
  }
  return Status::OK();
}

Status Cluster::InfoRepl(const Node& node, const std::string& table,
    std::map<int, PartitionView>* view) {
  BuildInfoContext(this, table, client::Type::INFOREPL, context_);
  context_->result = SubmitDataCmd(node,
      *(context_->request), context_->response, context_->deadline);
  if (!context_->result.ok()) {
    return context_->result;
  }
  if (context_->response->info_repl_size() <= 0) {
    return Status::NotFound("there is no repl info");
  }
  for (int i = 0; i < context_->response->info_repl(0).partition_state_size(); ++i) {
    client::PartitionState pstate = context_->response->info_repl(0).partition_state(i);
    view->insert(std::pair<int, PartitionView>(pstate.partition_id(),
          PartitionView(pstate)));
  }
  return Status::OK();
}

Status Cluster::InfoServer(const Node& node, ServerState* state) {
  BuildInfoContext(this, "", client::Type::INFOSERVER, context_);
  context_->result = SubmitDataCmd(node,
      *(context_->request), context_->response, context_->deadline);
  if (!context_->result.ok()) {
    return context_->result;
  }
  *state = ServerState(context_->response->info_server());
  return Status::OK();
}

Status Cluster::InfoSpace(const std::string& table,
    std::vector<std::pair<Node, SpaceInfo>>* nodes) {
  PullInternal(table, CalcDeadline(options_.op_timeout));
  std::set<Node> related_nodes;
  Status s = GetTableMasters(table, &related_nodes);
  if (!s.ok()) {
    return s;
  }

  nodes->clear();
  for (auto node : related_nodes) {
    BuildInfoContext(this, table, client::Type::INFOCAPACITY, context_);
    context_->result = SubmitDataCmd(node,
        *(context_->request), context_->response, context_->deadline);
    if (!context_->result.ok()) {
      continue;
    }
    SpaceInfo sinfo;
    sinfo.used = context_->response->info_capacity(0).used();
    sinfo.remain = context_->response->info_capacity(0).remain();
    nodes->push_back(std::pair<Node, SpaceInfo>(node, sinfo));
  }

  return Status::OK();
}

/*
 * deadline is 0 means no deadline
 */
Status Cluster::SubmitDataCmd(const Node& master,
    client::CmdRequest& req, client::CmdResponse *res,
    uint64_t deadline, int attempt) {
  Status s;
  res->Clear();
  std::shared_ptr<ZpCli> data_cli = data_pool_->GetConnection(master,
      deadline, &s);
  if (!data_cli) {
    return s;
  }

  {
    slash::MutexLock l(&data_cli->cli_mu);
    s = data_cli->SetTimeout(deadline, TimeoutOptType::SEND);
    if (s.ok()) {
      s = data_cli->cli->Send(&req);
    }
    if (s.ok()) {
      s = data_cli->SetTimeout(deadline, TimeoutOptType::RECV);
    }
    if (s.ok()) {
      s = data_cli->cli->Recv(res);
    }
  }

  if (!s.ok()) {
    data_pool_->RemoveConnection(data_cli);
    if (s.IsTimeout()) {
      return s;
    }
    if (attempt <= kDataAttempt) {
      return SubmitDataCmd(master, req, res, deadline, attempt + 1);
    }
  }
  return s;
}

/*
 * deadline is 0 means no deadline
 */
Status Cluster::SubmitMetaCmd(ZPMeta::MetaCmd& req,
    ZPMeta::MetaCmdResponse *res, uint64_t deadline, int attempt) {
  Status s;
  res->Clear();
  std::shared_ptr<ZpCli> meta_cli = GetMetaConnection(deadline, &s);
  if (!meta_cli) {
    return s;
  }

  {
    slash::MutexLock l(&meta_cli->cli_mu);
    s = meta_cli->SetTimeout(deadline, TimeoutOptType::SEND);
    if (s.ok()) {
      s = meta_cli->cli->Send(&req);
    }
    if (s.ok()) {
      s = meta_cli->SetTimeout(deadline, TimeoutOptType::RECV);
    }
    if (s.ok()) {
      s = meta_cli->cli->Recv(res);
    }
  }

  if (!s.ok()) {
    meta_pool_->RemoveConnection(meta_cli);
    if (s.IsTimeout()) {
      return s;
    }
    if (attempt <= kMetaAttempt) {
      return SubmitMetaCmd(req, res, deadline, attempt + 1);
    }
  }
  return s;
}

Status Cluster::DebugDumpPartition(const std::string& table,
    int partition_id, bool dump_nodes) {
  slash::RWLock l(&meta_rw_, false);
  auto it = tables_.find(table);
  if (it == tables_.end()) {
    return Status::InvalidArgument("don't have this table's info");
  }
  std::cout << "-epoch: " << epoch_ << std::endl;
  it->second->DebugDump(partition_id);

  if (dump_nodes) {
    std::map<Node, std::vector<const Partition*>> nodes_loads;
    it->second->GetNodesLoads(&nodes_loads);
    for (auto& node : nodes_loads) {
      std::cout << node.first << ": [";
      const std::vector<const Partition*>& p_vec = node.second;
      const Partition* p;
      size_t i = 0;
      for (i = 0; i < p_vec.size() - 1; i++) {
        p = p_vec.at(i);
        printf("%d%s, ", p->id(), p->master() == node.first ? "*" : "");
      }
      p = p_vec.at(i);
      printf("%d%s]\n", p->id(), p->master() == node.first ? "*" : "");
    }
  }

  return Status::OK();
}

int Cluster::LocateKey(const std::string& table,
    const std::string& key) {
  slash::RWLock l(&meta_rw_, false);
  auto it = tables_.find(table);
  if (it == tables_.end()) {
    return -1;
  }
  const Partition* part = it->second->GetPartition(key);
  if (!part) {
    return -1;
  }
  return part->id();
}

std::unordered_map<std::string, Table*> Cluster::tables() {
  slash::RWLock l(&meta_rw_, false);
  return tables_;
}

static int RandomIndex(int floor, int ceil) {
  assert(ceil >= floor);
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> di(floor, ceil);
  return di(mt);
}

std::shared_ptr<ZpCli> Cluster::GetMetaConnection(uint64_t deadline, Status* sptr) {
  std::shared_ptr<ZpCli> meta_cli = meta_pool_->GetExistConnection();
  if (meta_cli) {
    return meta_cli;
  }

  // No Exist one, try to connect any
  int cur = RandomIndex(0, options_.meta_addr.size() - 1);
  int count = 0;
  while (static_cast<size_t>(count++) < options_.meta_addr.size()) {
    meta_cli = meta_pool_->GetConnection(options_.meta_addr[cur], deadline, sptr);
    if (meta_cli) {
      break;
    }
    cur++;
    if (static_cast<size_t>(cur) == options_.meta_addr.size()) {
      cur = 0;
    }
  }
  return meta_cli;
}

Status Cluster::GetTableMasters(const std::string& table,
    std::set<Node>* related_nodes) {
  slash::RWLock l(&meta_rw_, false);
  auto table_iter = tables_.find(table);
  if (table_iter == tables_.end()) {
    return Status::InvalidArgument("this table does not exist");
  }
  table_iter->second->GetAllMasters(related_nodes);
  return Status::OK();
}


Status Cluster::GetDataMaster(const std::string& table,
    const std::string& key, Node* master) {
  slash::RWLock l(&meta_rw_, false);
  auto it = tables_.find(table);
  if (it != tables_.end()) {
    const Partition* part = it->second->GetPartition(key);
    if (!part) {
      return Status::Incomplete("no partitions yet");
    }
    if (part->master().port == 0 || part->master().ip == ""){ 
      return Status::Incomplete("no master yet");
    }
    *master = part->master();
    return Status::OK();
  } else {
    return Status::InvalidArgument("table does not exist");
  }
}

Status Cluster::GetDataMasterById(const std::string& table,
    int partition_id, Node* master) {
  slash::RWLock l(&meta_rw_, false);
  auto it = tables_.find(table);
  if (it != tables_.end()) {
    const Partition* part = it->second->GetPartitionById(partition_id);
    if (!part) {
      return Status::Incomplete("no partitions yet");
    }
    if (part->master().port == 0 || part->master().ip == ""){ 
      return Status::Incomplete("no master yet");
    }
    *master = part->master();
    return Status::OK();
  } else {
    return Status::InvalidArgument("table does not exist");
  }
}

Status Cluster::UpdateDataMaster(const std::string& table_name,
    const std::string& sample_key, const Node& target) {
  slash::RWLock l(&meta_rw_, true);
  auto it = tables_.find(table_name);
  if (it == tables_.end()) {
    return Status::InvalidArgument("table does not exist");
  }
  return it->second->UpdatePartitionMaster(sample_key, target);
}

Status Cluster::UpdateDataMasterById(const std::string& table_name,
    int partition_id, const Node& target) {
  slash::RWLock l(&meta_rw_, true);
  auto it = tables_.find(table_name);
  if (it == tables_.end()) {
    return Status::InvalidArgument("table does not exist");
  }
  return it->second->UpdatePartitionMasterById(partition_id, target);
}

void Cluster::ResetMetaInfo(const std::string& table_name,
    const ZPMeta::MetaCmdResponse_Pull& pull) {
  slash::RWLock l(&meta_rw_, true);
  epoch_ = pull.version();
  auto table_ptr = tables_.find(table_name);
  if (table_ptr != tables_.end()) {
    delete table_ptr->second;
    tables_.erase(table_ptr);
  }

  if (pull.info_size() == 0
      || pull.info(0).name() != table_name) { // no meta for table_name
    return;
  }
  Table* new_table = new Table(pull.info(0));
  tables_.insert(std::make_pair(pull.info(0).name(), new_table));
}


}  // namespace libzp
