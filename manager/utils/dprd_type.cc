// Copyright 2017 Qihoo
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http:// www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
#include "dprd_type.h"

DprdRuleStep::DprdRuleStep(const int op, const int arg1, const int arg2)
  : op_(op), arg1_(arg1), arg2_(arg2) {
}

DprdRuleStep::~DprdRuleStep() {}

DprdRule::DprdRule(const int id): id_(id) {
}

DprdRule::~DprdRule() {
  for (size_t i = 0; i < steps_.size(); ++i) {
    delete (steps_[i]);
  }
}

DprdBucket::DprdBucket(const int id, const int type, const int weight,
    const int parent, const std::string& name, const std::string& ip,
    const int port)
  : id_(id), type_(type), weight_(weight), parent_(parent), ip_(ip),
  port_(port), name_(name) {}

DprdBucket::~DprdBucket() {
}

DprdMap::DprdMap() : max_bucket_(0), max_pos_id_(0), min_neg_id_(0),
  sum_weight_(0) {
}

DprdMap::~DprdMap() {
  std::map<int, DprdBucket*>::iterator bucket_iter = buckets_.begin();
  for (; bucket_iter != buckets_.end(); ++bucket_iter) {
    DprdBucket* to_delete = bucket_iter->second;
    delete(to_delete);
  }
  std::map<int, DprdRule*>::iterator rule_iter = rules_.begin();
  for (; rule_iter != rules_.end(); ++rule_iter) {
    DprdRule* to_delete = rule_iter->second;
    delete(to_delete);
  }
}

bool DprdMap::InsertBucket(const int id, DprdBucket* bucket) {
  std::pair<std::map<int, DprdBucket*>::iterator, bool> res_buckets;
  res_buckets = buckets_.insert(std::make_pair(id, bucket));
  if (!res_buckets.second) {
    return false;
  }
  const std::string& name = bucket->name_;
  // bucket doesnt have a name
  if (name.empty()) {
    return true;
  }
  std::pair<std::map<std::string, int>::iterator, bool> res_name;
  res_name = name_id_.insert(std::make_pair(name, id));
  if (!res_name.second) {
    return false;
  }
  if (bucket->ip_.empty() || !bucket->port_) {
    return true;
  }
  std::string ip_port = bucket->ip_ + ":" + std::to_string(bucket->port_);
  std::pair<std::map<std::string, int>::iterator, bool> res_ip_port;
  res_ip_port = ip_port_id_.insert(std::make_pair(ip_port, id));
  if (!res_ip_port.second) {
    return false;
  }
  return true;
}

bool DprdMap::RemoveBucket(int id) {
  DprdBucket* target = FindBucket(id);
  if (target == NULL) {
    return false;
  }

  buckets_.erase(id);
  const std::string name = target->name_;
  std::map<std::string, int>::iterator name_iter = name_id_.find(name);
  if (name_iter != name_id_.end()) {
    name_id_.erase(name_iter);
  }

  std::string ip_port = target->ip_ + ":" + std::to_string(target->port_);
  std::map<std::string, int>::iterator ip_port_iter = ip_port_id_.find(ip_port);
  if (ip_port_iter != ip_port_id_.end()) {
    ip_port_id_.erase(ip_port_iter);
  }

  delete target;
  return true;
}

DprdBucket* DprdMap::FindBucket(const int bucket_id) {
  std::map<int, DprdBucket*>::iterator itr  = buckets_.find(bucket_id);
  if (itr != buckets_.end()) {
    return itr->second;
  } else {
    return NULL;
  }
}

DprdRule* DprdMap::FindRule(const int rule_id) {
  std::map<int, DprdRule*>::iterator itr = rules_.find(rule_id);
  if (itr != rules_.end()) {
    return itr->second;
  } else {
    return NULL;
  }
}

bool DprdMap::FindId(const std::string& target, int* id) {
  std::map<std::string, int>::iterator iter = name_id_.find(target);
  if (iter != name_id_.end()) {
    *id = iter->second;
    return true;
  }
  std::map<std::string, int>::iterator ip_port_iter = ip_port_id_.find(target);
  if (ip_port_iter != ip_port_id_.end()) {
    *id = ip_port_iter->second;
    return true;
  }
  return false;
}

void DprdMap::PrintAll() {
  std::map<int, DprdBucket*>::iterator bucket_itr = buckets_.begin();
  for (; bucket_itr != buckets_.end(); bucket_itr++) {
    DprdBucket* bucket = bucket_itr->second;
    std::cout<< "id:" << bucket->id_ << " type:" << bucket->type_ <<
      " weight:" << bucket->weight_ << " parent:" <<bucket->parent_ <<
      std::endl;
    if (bucket->type_ == kBucketTypeNode) {
      std::cout<< "ip:" << bucket->ip_ << " port:" << bucket->port_ <<
        std::endl;
    }
    const std::vector<int>& children = bucket->children_;
    std::cout<< "Children: ";
    for (size_t i = 0; i < children.size(); i++) {
      std::cout<< " " << children[i];
    }
    const std::set<int>& partitions = bucket->partitions_;
    std::cout<< std::endl << "partition: ";
    std::set<int>::const_iterator partitions_iter = partitions.begin();
    for (; partitions_iter != partitions.end(); ++partitions_iter) {
      std::cout<< " " << *partitions_iter;
    }
    std::cout<< std::endl;
    std::cout << "Partition size: " << partitions.size() << std::endl;
    std::cout << std::endl;
  }
  std::map<std::string, int>::iterator ip_port_iter = ip_port_id_.begin();
  std::cout<< "ip_port_id: size " << ip_port_id_.size() << std::endl;
  for (; ip_port_iter != ip_port_id_.end(); ++ip_port_iter) {
    std::cout<< "ip port " << ip_port_iter->first << std::endl;
    std::cout<< "id " << ip_port_iter->second << std::endl;
  }

  std::map<std::string, int>::iterator name_iter = name_id_.begin();
  std::cout<< "name_id: size " << name_id_.size() << std::endl;
  for (; name_iter != name_id_.end(); ++name_iter) {
    std::cout<< "name " << name_iter->first << std::endl;
    std::cout<< "id " << name_iter->second << std::endl;
  }

  std::cout<< std::endl;
  std::map<int, DprdRule*>::iterator rule_itr = rules_.begin();
  for (; rule_itr != rules_.end(); rule_itr++) {
    DprdRule* rule = rule_itr->second;
    std::cout<< "id: " <<rule->id_ << std::endl;
    const std::vector<DprdRuleStep*>& steps = rule->steps_;
    for (size_t i = 0; i != steps.size(); ++i) {
      DprdRuleStep* step = steps[i];
      std::cout<< "op: " << step->op_ << "arg1: "<< step->arg1_ << "arg2: "
        << step->arg2_ << std::endl;
    }
  }
}

