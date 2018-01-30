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

const int kDataAttempt = 10;

struct NodeTaskArg {
  CmdContext* context;
  Node target;
  NodeTaskArg(CmdContext* c, const Node& n)
    :context(c), target(n) {}
};

uint64_t CalcDeadline(int timeout_ms) {
  if (timeout_ms == 0) {
    return 0; // no limit
  }
  return slash::NowMicros() / 1000 + timeout_ms;
}

struct CmdContextResult {
  Status status;
  client::StatusCode res_code;
  std::string res_msg;
  bool is_filled;

  CmdContextResult()
      : status(Status::OK()),
        res_code(client::StatusCode::kOk),
        is_filled(false) {
  }

  void Fill(const Status& s,
      const client::StatusCode& code, const std::string& msg) {
    status = s;
    res_code = code;
    res_msg = msg;
    is_filled = true;
  }

  bool empty() const {
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

  bool OpTimeout() {
    if (deadline == 0) {
      return false; // no limit
    }
    return (slash::NowMicros() / 1000) >= deadline;
  }

  void Reset () {
    response->Clear();
    result = Status::Incomplete("Not complete");
    done_ = false;
  }

  void SetResult(const CmdContextResult& ccr) {
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

static std::string TryBuildKeyWithHashtag(const std::string& key) {
  std::string hashtag_with_brace;
  size_t l_brace = key.find(kLBrace);
  if (l_brace != std::string::npos) {
    // key := ...{hash_tag}...
    size_t r_brace = key.find(kRBrace, l_brace + 1);
    if (r_brace != std::string::npos) {
      hashtag_with_brace.assign(key.begin() + l_brace, key.begin() + r_brace + 1);
    }
  }

  return hashtag_with_brace.empty() ? key : hashtag_with_brace + key;
}

static void BuildSetContext(Cluster* cluster, const std::string& table,
    const std::string& key, const std::string& value, int ttl,
    CmdContext* set_context, zp_completion_t completion = NULL, void* data = NULL) {
  set_context->Init(cluster, table, key, completion, data);
  set_context->request->set_type(client::Type::SET);
  client::CmdRequest_Set* set_info = set_context->request->mutable_set();
  set_info->set_table_name(table);
  set_info->set_key(TryBuildKeyWithHashtag(key));
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
  get_cmd->set_key(TryBuildKeyWithHashtag(key));
}

static void BuildDeleteContext(Cluster* cluster, const std::string& table,
    const std::string& key, CmdContext* delete_context,
    zp_completion_t completion = NULL, void* data = NULL) {
  delete_context->Init(cluster, table, key, completion, data);
  delete_context->request->set_type(client::Type::DEL);
  client::CmdRequest_Del* delete_cmd = delete_context->request->mutable_del();
  delete_cmd->set_table_name(table);
  delete_cmd->set_key(TryBuildKeyWithHashtag(key));
}

static void BuildMgetContext(Cluster* cluster, const std::string& table,
    const std::vector<std::string>& keys, CmdContext* mget_context,
    zp_completion_t completion = NULL, void* data = NULL) {
  mget_context->Init(cluster, table, keys[0], completion, data);
  mget_context->request->set_type(client::Type::MGET);
  client::CmdRequest_Mget* mget_cmd = mget_context->request->mutable_mget();
  mget_cmd->set_table_name(table);
  for (auto& key : keys) {
    mget_cmd->add_keys(TryBuildKeyWithHashtag(key));
  }
}

static void BuildMsetContext(Cluster* cluster, const std::string& table,
    const std::vector<std::pair<std::string, std::string>>& kvs,
    CmdContext* mset_context) {
  mset_context->Init(cluster, table);
  mset_context->request->set_type(client::Type::MSET);
  for (const auto& kv : kvs) {
    client::CmdRequest_Set* set_cmd = mset_context->request->add_mset();
    set_cmd->set_table_name(table);
    set_cmd->set_key(kv.first);
    set_cmd->set_value(kv.second);
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

static void BuildWriteBatchContext(Cluster*cluster, const std::string& table,
    const Cluster::Batch& batch, CmdContext* context) {
  context->Init(cluster, table, batch.tag);
  context->request->set_type(client::Type::WRITEBATCH);
  client::CmdRequest_WriteBatch* write_batch_cmd =
    context->request->mutable_write_batch();
  write_batch_cmd->set_table_name(table);
  write_batch_cmd->set_hash_tag(batch.tag);
  for (auto& k : batch.keys_tobe_deleted) {
    write_batch_cmd->add_keys_to_delete(TryBuildKeyWithHashtag(k));
  }
  for (auto& item : batch.keys_tobe_added) {
    write_batch_cmd->add_keys_to_add(TryBuildKeyWithHashtag(item.first));
    write_batch_cmd->add_values_to_add(item.second);
  }
}

static void BuildListbyTagContext(Cluster*cluster, const std::string& table,
    const std::string& hash_tag, CmdContext* context) {
  context->Init(cluster, table, hash_tag);
  context->request->set_type(client::Type::LISTBYTAG);
  client::CmdRequest_ListbyTag* listby_tag_cmd =
    context->request->mutable_listby_tag();
  listby_tag_cmd->set_table_name(table);
  listby_tag_cmd->set_hash_tag(hash_tag);
}

static void BuildDeletebyTagContext(Cluster*cluster, const std::string& table,
    const std::string& hash_tag, CmdContext* context) {
  context->Init(cluster, table, hash_tag);
  context->request->set_type(client::Type::DELETEBYTAG);
  client::CmdRequest_DeletebyTag* deleteby_tag_cmd =
    context->request->mutable_deleteby_tag();
  deleteby_tag_cmd->set_table_name(table);
  deleteby_tag_cmd->set_hash_tag(hash_tag);
}

static void BuildInfoContext(Cluster* cluster, const std::string& table,
    client::Type type, CmdContext* info_context) {
  info_context->Init(cluster, table);
  info_context->request->set_type(type);
  if (!table.empty()) {
    info_context->request->mutable_info()->set_table_name(table);
  }
}

/* TODO Delete me
static void ClearDistributeMap(std::map<Node, CmdContext*>* key_distribute) {
  for (auto& kd : *key_distribute) {
    delete kd.second;
  }
}
*/

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

static std::string TryTrimHashtag(const std::string& key) {
  if (key.empty() || key.at(0) != '{') {
    return key;
  }

  size_t r_brace = key.find(kRBrace, 1);
  if (r_brace != std::string::npos) {
    return key.substr(r_brace + 1);
  }
  return key;
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
      values->insert(std::make_pair(TryTrimHashtag(kv.key()), kv.value()));
    }
    return Status::OK();
  } else {
    return Status::Corruption(context_->response->msg());
  }
}

Status Cluster::Mset(const std::string& table_name,
    const std::vector<std::pair<std::string, std::string>>& kvs) {
  BuildMsetContext(this, table_name, kvs, context_);
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

/* TODO Delete me
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
*/

bool Cluster::MultiDeliver(CmdContext* context) {
  // Prepare Request
  Node master;
  std::map<Node, std::unique_ptr<CmdContext>> key_distribute;
  client::Type type = context->request->type();
  if (type == client::Type::MGET) {
    for (const auto& k : context->request->mget().keys()) {
      context->result = GetDataMaster(context->table, k, &master);
      if (!context->result.ok()) {
        return false;
      }

      if (key_distribute.find(master) == key_distribute.end()) {
        std::unique_ptr<CmdContext> sub_context(new CmdContext());
        sub_context->Init(this, context->table, k);
        sub_context->deadline = context->deadline;
        sub_context->request->set_type(type);

        client::CmdRequest_Mget* new_mget_cmd =
          sub_context->request->mutable_mget();
        new_mget_cmd->set_table_name(context->table);
        key_distribute.insert(std::make_pair(master, std::move(sub_context)));
      }
      key_distribute[master]->request->mutable_mget()->add_keys(k);
    }
  } else if (type == client::Type::MSET) {
    for (const auto& set_cmd : context->request->mset()) {
      const std::string& k = set_cmd.key();
      context->result = GetDataMaster(context->table, k, &master);
      if (!context->result.ok()) {
        return false;
      }

      std::map<Node, std::unique_ptr<CmdContext>>::iterator iter =
        key_distribute.find(master);

      CmdContext* sub_ctx_ptr;
      client::CmdRequest_Set* sub_set_cmd;

      if (iter == key_distribute.end()) {
        std::unique_ptr<CmdContext> sub_context(new CmdContext());
        sub_ctx_ptr = sub_context.get();

        sub_context->Init(this, context->table, k);
        sub_context->deadline = context->deadline;
        sub_context->request->set_type(type);

        key_distribute.insert(std::make_pair(master, std::move(sub_context)));
      } else {
        sub_ctx_ptr = iter->second.get();
      }

      sub_set_cmd = sub_ctx_ptr->request->add_mset();
      sub_set_cmd->CopyFrom(set_cmd);
    }
  }

  // Dispatch
  for (auto& kd : key_distribute) {
    AddNodeTask(kd.first, kd.second.get());
  }

  for (auto& kd : key_distribute) {
    kd.second->WaitRpcDone();
  }

  // Wait peer_workers process and merge result
  context->response->set_type(type);
  for (const auto& kd : key_distribute) {
    const std::unique_ptr<CmdContext>& sub_ctx = kd.second;
    context->key = sub_ctx->key;
    context->result = sub_ctx->result;
    context->response->set_code(sub_ctx->response->code());
    context->response->set_msg(sub_ctx->response->msg());
    if (sub_ctx->response->has_redirect()) {
      client::Node* node =  context->response->mutable_redirect();
      *node = sub_ctx->response->redirect();
    }
    if (!context->result.ok() ||
        context->response->code() != client::StatusCode::kOk) { // no NOTFOUND in mget response
      return false;
    }
    if (type == client::Type::MGET) {
      for (auto& kv : kd.second->response->mget()) {
        client::CmdResponse_Mget* res_mget= context->response->add_mget();
        res_mget->set_key(kv.key());
        res_mget->set_value(kv.value());
      }
    }
  }
  return true;
}

bool Cluster::Deliver(CmdContext* context) {
  if (context->request->type() == client::Type::MGET ||
      context->request->type() == client::Type::MSET) {
    return MultiDeliver(context);
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
      usleep(1000);  // 1ms
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
        std::string key = TryTrimHashtag(kv.key());
        kvs.insert(std::pair<std::string, std::string>(key, kv.value()));
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

Status Cluster::WriteBatch(const std::string& table, const Batch& batch) {
  BuildWriteBatchContext(this, table, batch, context_);
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

Status Cluster::ListbyTag(
    const std::string& table,
    const std::string& hash_tag,
    std::map<std::string, std::string>* kvs) {
  BuildListbyTagContext(this, table, hash_tag, context_);
  DeliverAndPull(context_);

  if (!context_->result.ok()) {
    return context_->result;
  }
  if (context_->response->code() == client::StatusCode::kOk) {
    for (auto& kv : context_->response->listby_tag()) {
      kvs->insert(std::make_pair(TryTrimHashtag(kv.key()), kv.value()));
    }
    return Status::OK();
  } else {
    return Status::Corruption(context_->response->msg());
  }
}

Status Cluster::DeletebyTag(const std::string& table,
                            const std::string& hash_tag) {
  BuildDeletebyTagContext(this, table, hash_tag, context_);
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

std::shared_ptr<ZpCli> Cluster::GetMetaConnection(
    uint64_t deadline, Status* sptr, const Node* specific_meta) {
  if (specific_meta != nullptr) {
    return meta_pool_->GetConnection(*specific_meta, deadline, sptr);
  }

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
    if (part->master().port == 0 || part->master().ip == "") {
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
    if (part->master().port == 0 || part->master().ip == "") {
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

  if (pull.meta_members_size() > 0) {
    options_.meta_addr.clear();
    for (auto& node : pull.meta_members()) {
      options_.meta_addr.push_back(Node(node.ip(), node.port()));
    }
    delete meta_pool_;
    meta_pool_ = new ConnectionPool(8);
  }

  if (pull.info_size() == 0
      || pull.info(0).name() != table_name) { // no meta for table_name
    return;
  }
  Table* new_table = new Table(pull.info(0));
  tables_.insert(std::make_pair(pull.info(0).name(), new_table));
}

}  // namespace libzp
