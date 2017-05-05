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

struct CmdRpcArg {
  Cluster* cluster;
  std::string table;
  std::string key;
  client::CmdRequest* request;
  client::CmdResponse* response;
  Status result;

  CmdRpcArg(Cluster* c, const std::string& t, const std::string& k)
    : cluster(c), table(t), key(k),
    result(Status::Incomplete("Not complete")),
    cond_(&mu_), done_(false) {
      request = new client::CmdRequest();
      response = new client::CmdResponse();
    }
  
  ~CmdRpcArg() {
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
  for (auto& bg : cmd_workers_) {
    delete bg.second;
  }

  std::unordered_map<std::string, Table*>::iterator iter = tables_.begin();
  while (iter != tables_.end()) {
    delete iter->second;
    iter++;
  }
  delete data_pool_;
  delete meta_pool_;
}

Status Cluster::Set(const std::string& table, const std::string& key,
    const std::string& value, int32_t ttl) {
  Status s;
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::SET);
  client::CmdRequest_Set* set_info = data_cmd_.mutable_set();
  set_info->set_table_name(table);
  set_info->set_key(key);
  set_info->set_value(value);
  if (ttl >= 0) {
    set_info->mutable_expire()->set_ttl(ttl);
  }

  s = SubmitDataCmd(table, key, data_cmd_, &data_res_);
  if (!s.ok()) {
    return Status::IOError(s.ToString());
  }
  if (data_res_.code() == client::StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::Corruption(data_res_.msg());
  }
}

Status Cluster::Delete(const std::string& table, const std::string& key) {
  Status s;
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::DEL);
  client::CmdRequest_Del* del_info = data_cmd_.mutable_del();
  del_info->set_table_name(table);
  del_info->set_key(key);

  s = SubmitDataCmd(table, key, data_cmd_, &data_res_);
  if (!s.ok()) {
    return Status::IOError(s.ToString());
  }
  if (data_res_.code() == client::StatusCode::kOk) {
    return Status::OK();
  } else {
    return Status::Corruption(data_res_.msg());
  }
}

Status Cluster::Get(const std::string& table, const std::string& key,
    std::string* value) {
  Status s;
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::GET);
  client::CmdRequest_Get* get_cmd = data_cmd_.mutable_get();
  get_cmd->set_table_name(table);
  get_cmd->set_key(key);

  s = SubmitDataCmd(table, key, data_cmd_, &data_res_);
  if (!s.ok()) {
    return Status::IOError(s.ToString());
  }
  if (data_res_.code() == client::StatusCode::kOk) {
    client::CmdResponse_Get info = data_res_.get();
    value->assign(info.value().data(), info.value().size());
    return Status::OK();
  } else if (data_res_.code() == client::StatusCode::kNotFound) {
    return Status::NotFound("key do not exist");
  } else {
    return Status::Corruption(data_res_.msg());
  }
}

Status Cluster::Mget(const std::string& table,
    const std::vector<std::string>& keys,
    std::map<std::string, std::string>* values) {
  if (values == NULL) {
    return Status::InvalidArgument("Null pointer");
  }

  Status s;
  Node master;
  // Build multi command to nodes: master node -> Command
  std::map<Node, CmdRpcArg*> key_distribute;
  for (auto& k : keys) {
    s = GetDataMaster(table, k, &master);
    if (!s.ok()) {
      return s;
    }

    if (key_distribute.find(master) == key_distribute.end()) {
      CmdRpcArg* arg = new CmdRpcArg(this, table, k);
      arg->request->set_type(client::Type::MGET);
      client::CmdRequest_Mget* new_mget_cmd = arg->request->mutable_mget();
      new_mget_cmd->set_table_name(table);
      key_distribute.insert(std::pair<Node, CmdRpcArg*>(
            master, arg));
    }
    key_distribute[master]->request->mutable_mget()->add_keys(k);
  }

  // Distribute command
  DistributeDataRpc(key_distribute);

  bool has_error = false;
  for (auto& kd : key_distribute) {
    client::CmdResponse *res = kd.second->response;
    if (!kd.second->result.ok()
        || res->code() != client::StatusCode::kOk ) {
      has_error = true;
    }
    for (auto& kv : res->mget()) {
      values->insert(std::pair<std::string, std::string>(kv.key(),
            kv.value()));
    }
    delete kd.second;
  }
  if (has_error) {
    return Status::Corruption("mget error happened");
  }
  return Status::OK();
}

void Cluster::DoSubmitDataCmd(void* arg) {
  CmdRpcArg *carg = static_cast<CmdRpcArg*>(arg);
  carg->result = carg->cluster->SubmitDataCmd(carg->table, carg->key,
      *(carg->request), carg->response);
  carg->RpcDone();
}

void Cluster::DistributeDataRpc(
    const std::map<Node, CmdRpcArg*>& key_distribute) {
  for (auto& kd : key_distribute) {
    if (cmd_workers_.find(kd.first) == cmd_workers_.end()) {
      pink::BGThread* bg = new pink::BGThread();
      bg->StartThread();
      cmd_workers_.insert(std::pair<Node, pink::BGThread*>(kd.first, bg));
    }
    cmd_workers_[kd.first]->Schedule(DoSubmitDataCmd, kd.second);
  }

  for (auto& kd : key_distribute) {
    kd.second->WaitRpcDone();
  }
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

// TODO(all) pull meta_info from data
Status Cluster::Connect() {
  ZpCli* meta_cli = GetMetaConnection();
  if (meta_cli == NULL) {
    return Status::IOError("can't connect meta server");
  }
  return Status::OK();
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

Status Cluster::InfoQps(const std::string& table, int* qps, int* total_query) {
  Status s;

  Pull(table);
  auto table_iter = tables_.find(table);
  if (table_iter == tables_.end()) {
    return Status::NotFound("this table does not exist");
  }
  Table* table_map = table_iter->second;

  std::set<Node> related_nodes;
  table_map->GetAllMasters(&related_nodes);

  auto node_iter = related_nodes.begin();
  while (node_iter != related_nodes.end()) {
    data_cmd_.Clear();
    data_cmd_.set_type(client::Type::INFOSTATS);
    s = TryDataRpc(*node_iter, data_cmd_, &data_res_);
    node_iter++;
    if (s.IsIOError() || s.IsCorruption()) {
      continue;
    }
    for (int i = 0; i < data_res_.info_stats_size(); i++) {
      std::string name = data_res_.info_stats(i).table_name();
      if (name == table) {
        *qps += data_res_.info_stats(i).qps();
        *total_query += data_res_.info_stats(i).total_querys();
        break;
      }
    }
  }
  return Status::OK();
}

Status Cluster::InfoRepl(const Node& node, const std::string& table,
    std::map<int, PartitionView>* view) {
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::INFOREPL);
  data_cmd_.mutable_info()->set_table_name(table);
  Status s = TryDataRpc(node, data_cmd_, &data_res_);
  if (!s.ok()) {
    return s;
  }
  for (int i = 0; i < data_res_.info_repl(0).partition_state_size(); ++i) {
    client::PartitionState pstate = data_res_.info_repl(0).partition_state(i);
    view->insert(std::pair<int, PartitionView>(pstate.partition_id(),
          PartitionView(pstate)));
  }
  return Status::OK();
}

Status Cluster::InfoServer(const Node& node, ServerState* state) {
  data_cmd_.Clear();
  data_cmd_.set_type(client::Type::INFOSERVER);
  Status s = TryDataRpc(node, data_cmd_, &data_res_);
  if (!s.ok()) {
    return s;
  }
  *state = ServerState(data_res_.info_server());
  return Status::OK();
}

Status Cluster::InfoSpace(const std::string& table,
    std::vector<std::pair<Node, SpaceInfo>>* nodes) {
  Status s;

  Pull(table);
  auto table_iter = tables_.find(table);
  if (table_iter == tables_.end()) {
    return Status::NotFound("this table does not exist");
  }
  Table* table_map = table_iter->second;

  std::set<Node> related_nodes;
  table_map->GetAllMasters(&related_nodes);

  auto node_iter = related_nodes.begin();
  while (node_iter != related_nodes.end()) {
    data_cmd_.Clear();
    data_cmd_.set_type(client::Type::INFOCAPACITY);
    s = TryDataRpc(*node_iter, data_cmd_, &data_res_);
    if (s.IsIOError() || s.IsCorruption()) {
      node_iter++;
      continue;
    }
    for (int i = 0; i < data_res_.info_capacity_size(); i++) {
      std::string name = data_res_.info_capacity(i).table_name();
      if (name == table) {
        std::pair<Node, SpaceInfo> info;
        info.first = *node_iter;
        info.second.used = data_res_.info_capacity(i).used();
        info.second.remain = data_res_.info_capacity(i).remain();
        nodes->push_back(info);
        break;
      }
    }
    node_iter++;
  }
  return Status::OK();
}

Status Cluster::SubmitDataCmd(const std::string& table, const std::string& key,
    client::CmdRequest& req, client::CmdResponse *res,
    bool has_pull) {
  Node master;
  Status s = TryGetDataMaster(table, key, &master);
  if (s.ok()) {
    s = TryDataRpc(master, req, res);
  }
  if ((s.ok() && data_res_.code() == client::StatusCode::kOk) // Success
      || has_pull) { // Already pull once
    return s;
  }
  
  // Failed, then try to update meta
  s = Pull(table);
  if (!s.ok()) {
    return s;
  }
  return SubmitDataCmd(table, key, req, res, true);
}

Status Cluster::TryDataRpc(const Node& master,
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
      return TryDataRpc(master, req, res, attempt + 1);
    }
  }
  return s;
}

Status Cluster::SubmitMetaCmd(int attempt) {
  ZpCli* meta_cli = GetMetaConnection();
  if (!meta_cli) {
    return Status::IOError("Failed to get meta cli");
  }

  Status s = meta_cli->cli->Send(&meta_cmd_);
  if (s.ok()) {
    s = meta_cli->cli->Recv(&meta_res_);
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

Status Cluster::TryGetDataMaster(const std::string& table,
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

Status Cluster::GetDataMaster(const std::string& table,
    const std::string& key, Node* master, bool has_pull) {
  Status s = TryGetDataMaster(table, key, master);
  if (s.ok()
      || has_pull) {
    return s;
  }
  
  s = Pull(table);
  if (!s.ok()) {
    return s;
  }
  return GetDataMaster(table, key, master, true);
}

void Cluster::ResetClusterMap(const ZPMeta::MetaCmdResponse_Pull& pull) {
  epoch_ = pull.version();
  for (auto& table : tables_) {
    delete table.second;
  }
  tables_.clear();
  for (int i = 0; i < pull.info_size(); i++) {
    std::cout << "reset table:" << pull.info(i).name() << std::endl;
    auto it = tables_.find(pull.info(i).name());
    if (it != tables_.end()) {
      delete it->second;
      tables_.erase(it);
    }
    Table* new_table = new Table(pull.info(i));
    tables_.insert(std::make_pair(pull.info(i).name(), new_table));
  }
}
}  // namespace libzp
