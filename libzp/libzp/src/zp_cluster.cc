/*
 * "Copyright [2016] qihoo"
 */
#include "libzp/include/zp_cluster.h"

#include <string>
#include <google/protobuf/text_format.h>

#include "slash/include/slash_string.h"
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

struct CmdContext {
  Cluster* cluster;
  std::string table;
  std::string key;
  client::CmdRequest* request;
  client::CmdResponse* response;
  Status result;
  zp_completion_t completion;
  void* user_data;

  CmdContext()
    : cluster(NULL), result(Status::Incomplete("Not complete")),
    user_data(NULL), cond_(&mu_), done_(false) {
      request = new client::CmdRequest();
      response = new client::CmdResponse();
    }

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
  }

  void Reset () {
    response->Clear();
    result = Status::Incomplete("Not complete");
    done_ = false;
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
    meta_addr_ = options.meta_addr;
    Init();
  }

Cluster::Cluster(const std::string& ip, int port)
  : epoch_(-1) {
    meta_addr_.push_back(Node(ip, port));
    Init();
  }

void Cluster::Init() {
    meta_pool_ = new ConnectionPool();
    data_pool_ = new ConnectionPool();
    meta_cmd_ = new ZPMeta::MetaCmd();
    meta_res_ = new ZPMeta::MetaCmdResponse();
    context_ = new CmdContext();
    async_worker_ = new pink::BGThread();

    pthread_rwlock_init(&meta_rw_, NULL);
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_rwlock_init(&meta_rw_, &attr);
}

Cluster::~Cluster() {
    
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
  
  delete async_worker_;
  pthread_rwlock_destroy(&meta_rw_);
  delete context_;
  delete meta_res_;
  delete meta_cmd_;
  delete data_pool_;
  delete meta_pool_;
}

Status Cluster::Connect() {
  std::shared_ptr<ZpCli> meta_cli = GetMetaConnection();
  if (meta_cli == NULL) {
    return Status::IOError("can't connect meta server");
  }
  return Status::OK();
}

Status Cluster::Set(const std::string& table, const std::string& key,
    const std::string& value, int32_t ttl) {
  BuildSetContext(this, table, key, value, ttl, context_);
  DeliverAndPull(context_);

  if (!context_->result.ok()) {
    return Status::IOError(context_->result.ToString());
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
    return Status::IOError(context_->result.ToString());
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
    return Status::IOError(context_->result.ToString());
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
    return Status::IOError(context_->result.ToString());
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

  // Wait peer_workers process and merge result
  context->response->set_type(client::Type::MGET);
  for (auto& kd : key_distribute) {
    kd.second->WaitRpcDone();
    
    context->result = kd.second->result;
    context->response->set_code(kd.second->response->code());
    context->response->set_msg(kd.second->response->msg());
    if (!kd.second->result.ok()
        || kd.second->response->code() != client::StatusCode::kOk) { // no NOTFOUND in mget response
      ClearDistributeMap(&key_distribute);
      return false;
    }
    for (auto& kv : kd.second->response->mget()) {
      client::CmdResponse_Mget* res_mget= context->response->add_mget();
      res_mget->set_key(kv.key());
      res_mget->set_value(kv.value());
    }
    delete kd.second;
  }

  return true;
}

bool Cluster::Deliver(CmdContext* context) {
  if (context->request->type() == client::Type::MGET) {
    return DeliverMget(context); 
  }

  // Prepare Request
  Node master;
  context->result = GetDataMaster(context->table, context->key, &master);
  if (!context->result.ok()) {
    return false;
  }
  
  context->result = SubmitDataCmd(master,
      *(context->request), context->response);

  if (!context->result.ok()
        || (context->response->code() != client::StatusCode::kOk
          && context_->response->code() != client::StatusCode::kNotFound)) { // Error
    return false;
  }
  return true;
}

void Cluster::DeliverAndPull(CmdContext* context, bool has_pull) {
  bool succ = Deliver(context);
  
  if (succ || has_pull){
    return; 
  }

  // Failed, then try to update meta
  context->result = Pull(context->table);
  if (!context->result.ok()) {
    return;
  }
  context->Reset();
  return DeliverAndPull(context, true);
}

void Cluster::DoAsyncTask(void* arg) {
  CmdContext *carg = static_cast<CmdContext*>(arg);
  carg->cluster->DeliverAndPull(carg);

  // Callback zp_completion_t
  std::string value;
  std::map<std::string, std::string> kvs;
  switch (carg->response->type()) {
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
      *(carg->request), carg->response);
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

Status Cluster::CreateTable(const std::string& table_name,
    const int partition_num) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::INIT);
  ZPMeta::MetaCmd_Init* init = meta_cmd_->mutable_init();
  init->set_name(table_name);
  init->set_num(partition_num);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_);

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
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

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_);

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::Pull(const std::string& table) {
  // Pull is different with other meta command
  // Since it may be called both by user thread and async thread
  ZPMeta::MetaCmd meta_cmd;
  ZPMeta::MetaCmdResponse meta_res;
  meta_cmd.set_type(ZPMeta::Type::PULL);
  ZPMeta::MetaCmd_Pull* pull = meta_cmd.mutable_pull();
  pull->set_name(table);

  slash::Status ret = SubmitMetaCmd(meta_cmd, &meta_res);
  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }

  if (meta_res.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res.msg());
  }

  // Update clustermap now
  ResetTableMeta(table, meta_res.pull());
  return Status::OK();
}

Status Cluster::FetchMetaInfo(const std::string& table, Table* table_meta) {
  slash::Status s = Pull(table);
  if (!s.ok()) {
    return s;
  }
  slash::RWLock l(&meta_rw_, false);
  if (tables_.find(table) == tables_.end()) {
    return Status::NotFound("table not found");
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

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_);
  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
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

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_);
  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
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

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_);
  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }
  if (meta_res_->code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_->msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::ListMeta(Node* master, std::vector<Node>* nodes) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::LISTMETA);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_);

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
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

Status Cluster::ListNode(std::vector<Node>* nodes,
    std::vector<std::string>* status) {
  meta_cmd_->Clear();
  meta_cmd_->set_type(ZPMeta::Type::LISTNODE);

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_);

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
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

  slash::Status ret = SubmitMetaCmd(*meta_cmd_, meta_res_);

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
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

Status Cluster::GetTableMasters(const std::string& table,
    std::set<Node>* related_nodes) {
  slash::RWLock l(&meta_rw_, false);
  auto table_iter = tables_.find(table);
  if (table_iter == tables_.end()) {
    return Status::NotFound("this table does not exist");
  }
  table_iter->second->GetAllMasters(related_nodes);
  return Status::OK();
}

Status Cluster::InfoQps(const std::string& table, int* qps, int* total_query) {
  Pull(table);
  std::set<Node> related_nodes;
  Status s = GetTableMasters(table, &related_nodes);
  if (!s.ok()) {
    return s;
  }

  *qps = *total_query = 0;
  auto node_iter = related_nodes.begin();
  while (node_iter != related_nodes.end()) {
    BuildInfoContext(this, table, client::Type::INFOSTATS, context_);
    context_->result = SubmitDataCmd(*node_iter,
        *(context_->request), context_->response);
    node_iter++;
    if (!context_->result.ok()) {
      continue;
    }
    *qps += context_->response->info_stats(0).qps();
    *total_query += context_->response->info_stats(0).total_querys();
  }
  return Status::OK();
}

Status Cluster::InfoRepl(const Node& node, const std::string& table,
    std::map<int, PartitionView>* view) {
  BuildInfoContext(this, table, client::Type::INFOREPL, context_);
  context_->result = SubmitDataCmd(node,
      *(context_->request), context_->response);
  if (!context_->result.ok()) {
    return context_->result;
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
      *(context_->request), context_->response);
  if (!context_->result.ok()) {
    return context_->result;
  }
  *state = ServerState(context_->response->info_server());
  return Status::OK();
}

Status Cluster::InfoSpace(const std::string& table,
    std::vector<std::pair<Node, SpaceInfo>>* nodes) {
  Pull(table);
  std::set<Node> related_nodes;
  Status s = GetTableMasters(table, &related_nodes);
  if (!s.ok()) {
    return s;
  }

  nodes->clear();
  for (auto node : related_nodes) {
    BuildInfoContext(this, table, client::Type::INFOCAPACITY, context_);
    context_->result = SubmitDataCmd(node,
        *(context_->request), context_->response);
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

Status Cluster::SubmitDataCmd(const Node& master,
    client::CmdRequest& req, client::CmdResponse *res,
    int attempt) {
  res->Clear();

  Status s;
  std::shared_ptr<ZpCli> data_cli = data_pool_->GetConnection(master);
  if (!data_cli) {
    return Status::Corruption("Failed to get data cli");
  }

  {
    slash::MutexLock l(&data_cli->cli_mu);
    s = data_cli->cli->Send(&req);
    if (s.ok()) {
      s = data_cli->cli->Recv(res);
    }
  }

  if (!s.ok()) {
    data_pool_->RemoveConnection(data_cli);
    if (attempt <= kDataAttempt) {
      return SubmitDataCmd(master, req, res, attempt + 1);
    }
  }
  return s;
}

Status Cluster::SubmitMetaCmd(ZPMeta::MetaCmd& req,
    ZPMeta::MetaCmdResponse *res, int attempt) {
  res->Clear();

  Status s;
  std::shared_ptr<ZpCli> meta_cli = GetMetaConnection();
  if (!meta_cli) {
    return Status::IOError("Failed to get meta cli");
  }

  {
    slash::MutexLock l(&meta_cli->cli_mu);
    s = meta_cli->cli->Send(&req);
    if (s.ok()) {
      s = meta_cli->cli->Recv(res);
    }
  }

  if (!s.ok()) {
    meta_pool_->RemoveConnection(meta_cli);
    if (attempt <= kMetaAttempt) {
      return SubmitMetaCmd(req, res, attempt + 1);
    }
  }
  return s;
}

Status Cluster::DebugDumpPartition(const std::string& table,
    int partition_id) {
  slash::RWLock l(&meta_rw_, false);
  auto it = tables_.find(table);
  if (it == tables_.end()) {
    return Status::NotFound("don't have this table's info");
  }
  it->second->DebugDump(partition_id);
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

static int RandomIndex(int floor, int ceil) {
  assert(ceil >= floor);
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> di(floor, ceil);
  return di(mt);
}

std::shared_ptr<ZpCli> Cluster::GetMetaConnection() {
  std::shared_ptr<ZpCli> meta_cli = meta_pool_->GetExistConnection();
  if (meta_cli) {
    return meta_cli;
  }

  // No Exist one, try to connect any
  int cur = RandomIndex(0, meta_addr_.size() - 1);
  int count = 0;
  while (count++ < meta_addr_.size()) {
    meta_cli = meta_pool_->GetConnection(meta_addr_[cur]);
    if (meta_cli) {
      break;
    }
    cur++;
    if (cur == meta_addr_.size()) {
      cur = 0;
    }
  }
  return meta_cli;
}

Status Cluster::GetDataMaster(const std::string& table,
    const std::string& key, Node* master) {
  slash::RWLock l(&meta_rw_, false);
  std::unordered_map<std::string, Table*>::iterator it =
    tables_.find(table);
  if (it != tables_.end()) {
    *master = it->second->GetPartition(key)->master();
    return Status::OK();
  } else {
    return Status::NotFound("table does not exist");
  }
}

void Cluster::ResetTableMeta(const std::string& table_name,
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
