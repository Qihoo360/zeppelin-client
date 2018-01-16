#include "dprd_type.h"

DprdRuleStep::DprdRuleStep(const int op, const int arg1, const int arg2)
  : op_(op), arg1_(arg1), arg2_(arg2) {
  
}

DprdRuleStep::~DprdRuleStep() {}

DprdRule::DprdRule(const int id): id_(id) {
}

DprdRule::~DprdRule() {
  std::vector<DprdRuleStep*>::iterator step_itr = steps_.begin();
  while (step_itr != steps_.end()) {
    DprdRuleStep* to_delete = *step_itr;
    delete(to_delete);
    steps_.erase(step_itr);
  }
}

DprdBucket::DprdBucket(const int id, const int type, const int weight,
    const int parent) : id_(id), type_(type), weight_(weight), 
  parent_(parent), port_(0) {}

DprdBucket::DprdBucket(const int id, const int type, const int weight)
  : id_(id), type_(type), weight_(weight), parent_(0), port_(0) {}

DprdBucket::DprdBucket(const int id, const int type, const int weight, 
    const std::string& name) : id_(id), type_(type), weight_(weight), 
    parent_(0), port_(0), name_(name) {}

DprdBucket::~DprdBucket() {
}

void DprdBucket::AddChild(int id) {
  children_.push_back(id);
}

void DprdBucket::RemoveChild(int id) {
  for (size_t i = 0; i < children_.size(); ++i) {
    if (children_[i] == id) {
      children_.erase(children_.begin() + i);
    }
  }
}

DprdMap::DprdMap() : max_bucket_(0), sum_weight_(0) {  
}

DprdMap::~DprdMap() {
  std::map<int, DprdBucket*>::iterator bucket_itr = buckets_.begin();
  while (bucket_itr != buckets_.end()) {
    DprdBucket* to_delete = bucket_itr->second;
    delete(to_delete);
    buckets_.erase(bucket_itr++);
  }
  std::map<int, DprdRule*>::iterator rule_itr = rules_.begin();
  while (rule_itr != rules_.end()) {
    DprdRule* to_delete = rule_itr->second;
    delete(to_delete);
    rules_.erase(rule_itr++);
  }
}

bool DprdMap::InsertBucket(const int id, DprdBucket* bucket) {
  std::pair<std::map<int, DprdBucket*>::iterator, bool> res_buckets;
  res_buckets = buckets_.insert(std::make_pair(id, bucket));
  if (!res_buckets.second) {
    return false;
  }
  const std::string& name = bucket->name();
  // bucket dont have a name
  if (name.empty()) {
    return true;
  }
  std::pair<std::map<std::string, int>::iterator, bool> res_name;
  res_name = name_id_.insert(std::make_pair(name, id));
  if (!res_name.second) {
    return false;
  }
  return true; 
}

bool DprdMap::RemoveBucket(int id) {
  DprdBucket* target = FindBucket(id);
  if (target == NULL) {
    return false;
  }
  const std::string name = target->name();
  std::map<std::string, int>::iterator iter = name_id_.find(name);
  if (iter != name_id_.end()) {
    name_id_.erase(iter);
  }
  buckets_.erase(id);
  return true;
}

bool DprdMap::InsertRule(const int id, DprdRule* rule) {
  std::pair<std::map<int, DprdRule*>::iterator, bool> res;
  res = rules_.insert(std::make_pair(id, rule));
  return res.second;
}

DprdBucket* DprdMap::FindBucket(const int bucket_id) {
  std::map<int, DprdBucket*>::iterator itr  = buckets_.find(bucket_id);
  if (itr != buckets_.end()) {
    return itr->second;
  }
  else {
    return NULL;
  }
}

DprdRule* DprdMap::FindRule(const int rule_id) {
  std::map<int, DprdRule*>::iterator itr = rules_.find(rule_id);
  if (itr != rules_.end()) {
    return itr->second;
  }
  else {
    return NULL;
  }
}

bool DprdMap::FindId(const std::string& name, int& id) {
  std::map<std::string, int>::iterator iter = name_id_.find(name);
  if (iter != name_id_.end()) {
    id = iter->second;
    return true;
  }
  else {
    return false;
  }
}

void DprdMap::PrintAll() {
  std::map<int, DprdBucket*>::iterator bucket_itr = buckets_.begin();
  for (; bucket_itr != buckets_.end(); bucket_itr++) {
    DprdBucket* bucket = bucket_itr->second;
    std::cout<< "id:" << bucket->id() << " type:" << bucket->type() <<
      " weight:" << bucket->weight() << " parent:" <<bucket->parent() << std::endl;
    if (bucket->type() == BUCKET_TYPE_NODE) {
      std::cout<< "ip:" << bucket->ip() << " port:" << bucket->port() << std::endl;
    }
    const std::vector<int>& children = bucket->children();
    std::cout<< "Children: ";
    for (size_t i = 0; i < children.size(); i++) {
      std::cout<< " " << children[i];
    }
    const std::set<int>& partitions = bucket->partitions();
    std::cout<< std::endl << "partition: ";
    std::set<int>::const_iterator partitions_iter = partitions.begin();
    for (; partitions_iter != partitions.end(); ++partitions_iter) {
      std::cout<< " " << *partitions_iter;
    }
    std::cout<< std::endl;
    std::cout<<"Partition size: " << partitions.size() << std::endl;
    std::cout<< std::endl;
  }
  std::map<int, DprdRule*>::iterator rule_itr = rules_.begin();
  for (; rule_itr != rules_.end(); rule_itr++) {
    DprdRule* rule = rule_itr->second;
    std::cout<< "id: " <<rule->id() << std::endl;
    const std::vector<DprdRuleStep*>& steps = rule->steps();
    for (size_t i = 0; i != steps.size(); ++i) {
      DprdRuleStep* step = steps[i];
      std::cout<< "op: " << step->op() << "arg1: "<< step->arg1() << "arg2: " << step->arg2() << std::endl;
    }
  }
}

