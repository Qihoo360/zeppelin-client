/*
 * "Copyright [2016] qihoo"
 */
#include "libzp/include/zp_cluster.h"

#include <google/protobuf/text_format.h>
#include <iostream>
#include <string>

#include "slash/include/slash_string.h"
#include "pink/include/bg_thread.h"
#include "pink/include/pink_cli.h"
#include "libzp/src/zp_conn.h"

namespace libzp {

const int kMetaAttempt = 10;
const int kDataAttempt = 10;

struct CmdContext;
static void BuildSetContext(Cluster* cluster, const std::string& table
    const std::string& key, const std::string& value, int ttl = -1, CmdContext* set_context,
    zp_completion_t completion = NULL, void* data = NULL) {
  set_context->Init(cluster, table, key, completion, data);
  set_context->set_type(client::Type::SET);
  client::CmdRequest_Set* set_info = set_context->mutable_set();
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
  get_context->set_type(client::Type::GET);
  client::CmdRequest_Get* get_cmd = get_context->mutable_get();
  get_cmd->set_table_name(table);
  get_cmd->set_key(key);
}

static void BuildDeleteContext(Cluster* cluster, const std::string& table,
    const std::string& key, CmdContext* delete_context,
    zp_completion_t completion = NULL, void* data = NULL) {
  delete_context->Init(cluster, table, key, completion, data);
  delete_context->set_type(client::Type::DELETE);
  client::CmdRequest_Delete* delete_cmd = delete_context->mutable_delete();
  delete_cmd->set_table_name(table);
  delete_cmd->set_key(key);
}

static void BuildMgetContext(Cluster* cluster, const std::string& table,
    const std::vector<std::string>& keys, CmdContext* mget_context,
    zp_completion_t completion = NULL, void* data = NULL) {
  mget_context->Init(cluster, table, key, completion, data);
  mget_context->set_type(client::Type::MGET);
  client::CmdRequest_Mget* mget_cmd = mget_context->mutable_mget();
  mget_cmd->set_table_name(table);
  for (auto& key : keys) {
    mget_cmd->add_keys(key);
  }
}

static void BuildInfoContext(Cluster* cluster, const std::string& table,
    client::Type type, CmdContext* info_context) {
  info_context->Init(cluster, table);
  info_context->set_type(type);
  if (!table.empty()) {
    info_context->mutable_info()->set_table_name(table);
  }
}

static void ClearDistributeMap(std::map<Node, CmdContext*>* key_distribute) {
  for (auto& kd : *key_distribute) {
    delete kd.second;
  }
}

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
    cond_(&mu_), done_(false), user_data(NULL) {
      request = new client::CmdRequest();
      response = new client::CmdResponse();
    }

  void Init(Cluster* c, const std::string& tab, const std::string& k = string(),
      zp_completion_t comp = NULL, void* d = NULL) {
    request->Clear();
    response->Clear();
    cluster = c;
    table = tab;
    key = k;
    user_data = d;
    complete = comp;
  }
  
  ~CmdContext() {
    delete request;
    delete response;
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

Cluster::Cluster(const Options& options)
  : epoch_(0) {
    meta_addr_ = options.meta_addr;
    assert(!meta_addr_.empty());
    meta_pool_ = new ConnectionPool();
    data_pool_ = new ConnectionPool();
  }

Cluster::Cluster(const std::string& ip, int port)
  : epoch_(0) {
    meta_addr_.push_back(Node(ip, port));
    meta_pool_ = new ConnectionPool();
    data_pool_ = new ConnectionPool();
  }

Cluster::~Cluster() {
  for (auto& bg : peer_workers_) {
    delete bg.second;
  }
  delete async_worker_;

  std::unordered_map<std::string, Table*>::iterator iter = tables_.begin();
  while (iter != tables_.end()) {
    delete iter->second;
    iter++;
  }
  delete data_pool_;
  delete meta_pool_;
}

Status Cluster::Connect() {
  ZpCli* meta_cli = GetMetaConnection();
  if (meta_cli == NULL) {
    return Status::IOError("can't connect meta server");
  }
  return Status::OK();
}

Status Cluster::Set(const std::string& table, const std::string& key,
    const std::string& value, int32_t ttl) {
  BuildSetContext(this, table, key, value, context_);
  DeliverDataCmd(context_);

  if (!context_->res.ok()) {
    return Status::IOError(context_->res.ToString());
  }
  if (context_->response->code() == client::StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::Corruption(context_->response->msg());
  }
}

Status Cluster::Delete(const std::string& table, const std::string& key) {
  BuildDeleteContext(this, table, key, context_);
  DeliverDataCmd(context_);

  if (!context_->res.ok()) {
    return Status::IOError(context_->res.ToString());
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
  DeliverDataCmd(context_);
  
  if (!context_->res.ok()) {
    return Status::IOError(context_->res.ToString());
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
  DeliverDataCmd(context_);

  if (!context_->res.ok()) {
    return Status::IOError(context_->res.ToString());
  }
  if (context_->response->code() == client::StatusCode::kOk) {
    for (auto& kv : context_->response->mget()) {
      values.insert(std::part<std::string, std::string>(kv.key(), kv.value()));
    }
  } else {
    return Status::Corruption(context_->response->msg());
  }
}

Status Cluster::Aset(const std::string& table, const std::string& key,
    const std::string& value, int32_t ttl = -1,
    zp_completion_t complietion, void* data) {
  CmdContext* context = new CmdContext();
  BuildSetContext(this, table, key, value, ttl, context, complietion, data);
  AddAsyncTask(context);
  return Status::OK();
}

Status Cluster::Aget(const std::string& table, const std::string& key,
    zp_completion_t complietion, void* data) {
  CmdContext* context = new CmdContext();
  BuildGetContext(this, table, key, context_, complietion, data);
  AddAsyncTask(context);
  return Status::OK();
}

Status Cluster::Adelete(const std::string& table, const std::string& key,
    zp_completion_t complietion, void* data) {
  CmdContext* context = new CmdContext();
  BuildDeleteContext(this, table, key, context_, complietion, data);
  AddAsyncTask(context);
  return Status::OK();
}

Status Cluster::Amget(const std::string& table, const std::vector<std::string>& keys,
    zp_completion_t complietion, void* data) {
  CmdContext* context = new CmdContext();
  BuildMgetContext(this, table, keys, context_, complietion, data);
  AddAsyncTask(context);
  return Status::OK();
}

bool Cluster::DispatchMget(CmdContext* context) {
  // Prepare Request
  std::map<Node, CmdContext*> key_distribute;
  for (auto& k : context->request->keys) {
    context->res = GetDataMaster(table, k, &master);
    if (!context->res.ok()) {
      ClearDistributeMap(&key_distribute);
      return false;
    }

    if (key_distribute.find(master) == key_distribute.end()) {
      CmdContext* sub_context = new CmdContext();
      sub_context->Init(this, context->table, k);
      sub_context->request->set_type(client::Type::MGET);
      client::CmdRequest_Mget* new_mget_cmd = sub_context->request->mutable_mget();
      new_mget_cmd->set_table_name(table);
      key_distribute.insert(std::pair<Node, CmdContext*>(
            master, sub_context));
    }
    key_distribute[master]->request->mutable_mget()->add_keys(k);
  }

  // Dispatch
  for (auto& kd : key_distribute) {
    AddPeerTask(kd.first, kd.second);
  }

  // Wait peer_workers process and merge result
  for (auto& kd : key_distribute) {
    kd.second->WaitRpcDone();
    
    context->res = kd.second->res;
    context->response->set_code(kd.second->response->code());
    context->response->set_msg(kd.second->response->msg());
    if (!kd.second->res.ok()
        || kd.second->response->code() != client::StatusCode::kOk ) {
      ClearDistributeMap(&key_distribute);
      return false;
    }
    for (auto& kv : kd.second->response->mget()) {
      client::CmdResponse_Mget* res_mget= context->response->add_mget();
      res_mget->set_key(kv.key(), kv.value());
    }
    delete kd.second;
  }

  return true;
}

bool Cluster::Dispatch(CmdContext* context) {
  if (context->request->type == client::Type::MGET) {
    return DispatchMget(context); 
  }

  // Prepare Request
  Node master;
  context->res = GetDataMaster(context->table, context->key, &master);
  if (!(context->res.ok()) {
    return false;
  }
  
  // Dispatch
  AddPeerTask(master, context);

  // Wait peer_workers process
  context->WaitRpcDone();
  if (!context->res.ok()
        || context->response->code() != client::StatusCode::kOk) { // Success
    return false;
  }
  return true;
}

void Cluster::DeliverDataCmd(CmdContext* context, bool has_pull) {
  context->response->Clear();
  bool succ = Dispatch(context);
  
  if (succ || has_pull){
    return; 
  }

  // Failed, then try to update meta
  context->res = Pull(table);
  if (!context->res.ok() {
    return;
  }
  return DeliverDataCmd(context, true);
}

void Cluster::DoAsyncTask(void* arg) {
  CmdContext *carg = static_cast<CmdContext*>(arg);
  carg->result = carg->cluster->DeliverDataCmd(carg->table,
      carg->key, carg);
  // Callback zp_completion_t
  carg->completion(Result(carg->result), carg->data);
}

void Cluster::AddAsyncTask(CmdContext* context) {
  async_worker_->StartThread();
  async_worker_->Schedule(DoAsyncTask, context);
}

void Cluster::DoPeerTask(void* arg) {
  CmdContext *carg = static_cast<CmdContext*>(arg);
  carg->result = carg->cluster->SubmitDataCmd(carg->table,
      *(carg->request), carg->response);
  carg->RpcDone();
}

void Cluster::AddPeerTask(const Node& node, CmdContext* context) {
    if (peer_workers_.find(node) == peer_workers_.end()) {
      pink::BGThread* bg = new pink::BGThread();
      bg->StartThread();
      peer_workers_.insert(std::pair<Node, pink::BGThread*>(node, bg));
    }
    peer_workers_[node]->Schedule(DoPeerTask, context);
}

Status Cluster::CreateTable(const std::string& table_name,
    const int partition_num) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::INIT);
  ZPMeta::MetaCmd_Init* init = meta_cmd_.mutable_init();
  init->set_name(table_name);
  init->set_num(partition_num);

  slash::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }

  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_.msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::DropTable(const std::string& table_name) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::DROPTABLE);
  ZPMeta::MetaCmd_DropTable* drop_table = meta_cmd_.mutable_drop_table();
  drop_table->set_name(table_name);

  slash::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_.msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::Pull(const std::string& table) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::PULL);
  ZPMeta::MetaCmd_Pull* pull = meta_cmd_.mutable_pull();
  pull->set_name(table);

  slash::Status ret = SubmitMetaCmd();
  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }

  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_.msg());
  }

  // Update clustermap now
  ResetClusterMap(meta_res_.pull());
  return Status::OK();
}

Status Cluster::FetchMetaInfo(const std::string& table, Table* table_meta) {
  slash::Status s = Pull(table);
  if (!s.ok()) {
    return s;
  }
  if (tables_.find(table) == tables_.end()) {
    return Status::NotFound("table not found");
  }
  *table_meta = *(tables_[table]);
  return Status::OK();
}

Status Cluster::SetMaster(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::SETMASTER);
  ZPMeta::MetaCmd_SetMaster* set_master_cmd = meta_cmd_.mutable_set_master();
  ZPMeta::BasicCmdUnit* set_master_entity = set_master_cmd->mutable_basic();
  set_master_entity->set_name(table_name);
  set_master_entity->set_partition(partition_num);
  ZPMeta::Node* node = set_master_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  slash::Status ret = SubmitMetaCmd();
  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }

  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_.msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::AddSlave(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::ADDSLAVE);
  ZPMeta::MetaCmd_AddSlave* add_slave_cmd = meta_cmd_.mutable_add_slave();
  ZPMeta::BasicCmdUnit* add_slave_entity = add_slave_cmd->mutable_basic();
  add_slave_entity->set_name(table_name);
  add_slave_entity->set_partition(partition_num);
  ZPMeta::Node* node = add_slave_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  slash::Status ret = SubmitMetaCmd();
  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }

  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_.msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::RemoveSlave(const std::string& table_name,
    const int partition_num, const Node& ip_port) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::REMOVESLAVE);
  ZPMeta::MetaCmd_RemoveSlave* remove_slave_cmd =
    meta_cmd_.mutable_remove_slave();
  ZPMeta::BasicCmdUnit* remove_slave_entity = remove_slave_cmd->mutable_basic();
  remove_slave_entity->set_name(table_name);
  remove_slave_entity->set_partition(partition_num);
  ZPMeta::Node* node = remove_slave_entity->mutable_node();
  node->set_ip(ip_port.ip);
  node->set_port(ip_port.port);

  slash::Status ret = SubmitMetaCmd();
  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_.msg());
  } else {
    return Status::OK();
  }
}

Status Cluster::ListMeta(Node* master, std::vector<Node>* nodes) {
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::LISTMETA);

  slash::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_.msg());
  }

  ZPMeta::MetaNodes info = meta_res_.list_meta().nodes();
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
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::LISTNODE);

  slash::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_.msg());
  }

  ZPMeta::Nodes info = meta_res_.list_node().nodes();
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
  meta_cmd_.Clear();
  meta_cmd_.set_type(ZPMeta::Type::LISTTABLE);

  slash::Status ret = SubmitMetaCmd();

  if (!ret.ok()) {
    return Status::IOError(ret.ToString());
  }
  if (meta_res_.code() != ZPMeta::StatusCode::OK) {
    return Status::Corruption(meta_res_.msg());
  }

  ZPMeta::TableName info = meta_res_.list_table().tables();
  for (int i = 0; i < info.name_size(); i++) {
    tables->push_back(info.name(i));
  }
  return Status::OK();
}

Status Cluster::GetTableMasters(const std::string& table_name,
    std::set<Node>* related_nodes) {
  auto table_iter = tables_.find(table);
  if (table_iter == tables_.end()) {
    return Status::NotFound("this table does not exist");
  }
  table_iter->second->GetAllMasters(&related_nodes);
  return Status::OK();
}

Status Cluster::InfoQps(const std::string& table, int* qps, int* total_query) {
  
  Pull(table);

  std::set<Node> related_nodes;
  Status s = GetTableMasters(table, &related_nodes);
  if (!s.ok()) {
    return s;
  }

  auto node_iter = related_nodes.begin();
  while (node_iter++ != related_nodes.end()) {
    BuildInfoContext(this, table, client::Type::INFOSTATS, context_);
  
    AddPeerTask(node, context_);
    context_->WaitRpcDone();
    if (!context_->res.ok()) {
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
  AddPeerTask(node, context_);
  context_->WaitRpcDone();
  if (!context_->res.ok()) {
    return context_->res;
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
  AddPeerTask(node, context_);
  context_->WaitRpcDone();
  if (!context_->res.ok()) {
    return context_->res;
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

  for (auto node : related_nodes) {
    BuildInfoContext(this, table, client::Type::INFOCAPACITY, context_);
    AddPeerTask(node, context_);
    context_->WaitRpcDone();
    if (!context_->res.ok()) {
      continue;
    }

    SpaceInfo sinfo;
    sinfo.used = context_->response->info_capacity(0).used();
    sinfo.remain = context_->response->info_capacity(0).remain();
    nodes->push_back(std::pari<Node, SpaceInfo>(node, sinfo));
  }

  return Status::OK();
}

Status Cluster::SubmitDataCmd(const Node& master,
    client::CmdRequest& req, client::CmdResponse *res,
    int attempt) {
  ZpCli* data_cli = data_pool_->GetConnection(master);
  if (!data_cli) {
    return Status::Corruption("Failed to get data cli");
  }

  Status s = data_cli->cli->Send(&req);
  if (s.ok()) {
    s = data_cli->cli->Recv(res);
  }
  if (!s.ok()) {
    data_pool_->RemoveConnection(data_cli);
    if (attempt <= kDataAttempt) {
      return SubmitDataCmd(master, req, res, attempt + 1);
    }
  }
  return s;
}

Status Cluster::SubmitMetaCmd(int attempt) {
  ZpCli* meta_cli = GetMetaConnection();
  if (!meta_cli) {
    return Status::IOError("Failed to get meta cli");
  }

  {
    slash::MutexLock l(&meta_cli->cli_mu);
    Status s = meta_cli->cli->Send(&meta_cmd_);
    if (s.ok()) {
      s = meta_cli->cli->Recv(&meta_res_);
    }
  }

  if (!s.ok()) {
    meta_pool_->RemoveConnection(meta_cli);
    if (attempt <= kMetaAttempt) {
      return SubmitMetaCmd(attempt + 1);
    }
  }

  return s;
}

Status Cluster::DebugDumpPartition(const std::string& table,
    int partition_id) const {
  auto it = tables_.find(table);
  if (it == tables_.end()) {
    return Status::NotFound("don't have this table's info");
  }
  it->second->DebugDump(partition_id);
  return Status::OK();
}

int Cluster::LocateKey(const std::string& table,
    const std::string& key) const {
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

ZpCli* Cluster::GetMetaConnection() {
  ZpCli* meta_cli = meta_pool_->GetExistConnection();
  if (meta_cli != NULL) {
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
  std::unordered_map<std::string, Table*>::iterator it =
    tables_.find(table);
  if (it != tables_.end()) {
    *master = it->second->GetPartition(key)->master();
    return Status::OK();
  } else {
    return Status::NotFound("table does not exist");
  }
}

void Cluster::ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull) {
  epoch_ = pull.version();
  for (auto& table : tables_) {
    delete table.second;
  }
  tables_.clear();

  // Record all peer
  std::set<Node> miss_peer;
  for (auto& thread : peer_workers_) {
    miss_peer.insert(thread.first);
  }

  for (int i = 0; i < pull.info_size(); i++) {
    std::cout << "reset table:" << pull.info(i).name() << std::endl;
    auto it = tables_.find(pull.info(i).name());
    if (it != tables_.end()) {
      delete it->second;
      tables_.erase(it);
    }
    Table* new_table = new Table(pull.info(i));
    tables_.insert(std::make_pair(pull.info(i).name(), new_table));
  
    // Remove node still used from miss_peer
    for (auto& node : new_table->GetAllNodes()) {
      if (miss_peer.find(node) != miss_peer.end()) {
        miss_peer.erase(node);
      }
    }
  }

  for (auto& miss : miss_peer) {
    peer_workers_.erase(miss); 
  }
}
}  // namespace libzp
