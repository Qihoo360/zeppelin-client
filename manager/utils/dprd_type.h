// Copyright 2017 Qihoo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef MANAGER_UTILS_DPRD_TYPE_H_
#define MANAGER_UTILS_DPRD_TYPE_H_

#include <iostream>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <utility>

enum {
  BUCKET_TYPE_ROOT,
  BUCKET_TYPE_RACK,
  BUCKET_TYPE_HOST,
  BUCKET_TYPE_NODE
};

/*
 * Dprd defines rule type and its arguments
 */
class DprdRuleStep {
 public:
  explicit DprdRuleStep(const int op, const int arg1, const int arg2);
  ~DprdRuleStep();
  int op() const {
    return op_;
  }
  int arg1() const {
    return arg1_;
  }
  int arg2() const {
    return arg2_;
  }
 private:
  int op_;
  int arg1_;
  int arg2_;
};

/* step op codes */
enum DprdRuleRuleCodes {
  DPRD_RULE_NOOP = 0,
  DPRD_RULE_TAKE = 1,          /* arg1 = value to start with */
  DPRD_RULE_CHOOSE_FIRSTN = 2, /* arg1 = num items to pick */
  DPRD_RULE_EMIT = 3           /* no args */
};

class DprdRule {
 public:
  explicit DprdRule(const int id);
  ~DprdRule();
  void InsertStep(DprdRuleStep* step) {
    steps_.push_back(step);
  }
  const std::vector<DprdRuleStep*>& steps() const {
    return steps_;
  }
  int id() const {
    return id_;
  }
 private:
  int id_;
  std::vector<DprdRuleStep*> steps_;
};

class DprdBucket {
 public:
  explicit DprdBucket(const int id, const int type,
      const int weight, const int parent);
  explicit DprdBucket(const int id, const int type,
      const int weight);
  explicit DprdBucket(const int id, const int type,
      const int weight, const std::string& name);
  ~DprdBucket();
  void AddChild(int id);
  void RemoveChild(int id);
  int id() const {
    return id_;
  }
  int type() const {
    return type_;
  }
  int weight() const {
    return weight_;
  }
  bool set_weight(int weight) {
    weight_ = weight;
    return true;
  }
  int parent() const {
    return parent_;
  }
  const std::string& ip() const {
    return ip_;
  }
  bool set_ip(const std::string& ip) {
    ip_ = ip;
    return true;
  }
  int port() const {
    return port_;
  }
  bool set_port(int port) {
    port_ = port;
    return true;
  }
  const std::string name() const {
    return name_;
  }
  bool set_name(const std::string& name) {
    name_ = name;
    return true;
  }
  const std::vector<int>& children() const {
    return children_;
  }
  const std::set<int>& partitions() const {
    return partitions_;
  }
  int partition_size() const {
    return partitions_.size();
  }
  bool InsertPartition(int partition) {
    std::pair<std::set<int>::iterator, bool>
      res = partitions_.insert(partition);
    if (res.second == false) {
      return false;
    }
    return true;
  }
  bool RemovePartition(int partition) {
    std::set<int>::iterator target = partitions_.find(partition);
    if (target == partitions_.end()) {
      return false;
    }
    partitions_.erase(target);
    return true;
  }

 private:
  int id_;
  int type_;
  int weight_;
  int parent_;
  std::string ip_;
  int port_;
  std::string name_;
  std::vector<int> children_;
  std::set<int> partitions_;
};

/*
 * dprd map includes all buckets, rules, etc.
 */
class DprdMap {
 public:
  DprdMap();
  ~DprdMap();
  bool InsertBucket(const int id, DprdBucket* bucket);
  bool RemoveBucket(int id);
  bool InsertRule(const int id, DprdRule* rule);
  DprdBucket* FindBucket(const int bucket_id);
  DprdRule* FindRule(const int rule_id);
  bool FindId(const std::string& name, int* id);
  const std::map<int, DprdBucket*>& buckets() const {
    return buckets_;
  }
  const std::map<int, DprdRule*>& rules() const {
    return rules_;
  }
  int max_bucket() const {
    return max_bucket_;
  }
  void IncMaxBucket() {
    max_bucket_++;
  }
  void DecMaxBucket() {
    max_bucket_--;
  }
  void set_sum_weight(const int weight) {
    sum_weight_ = weight;
  }
  int sum_weight() const {
    return sum_weight_;
  }
  void PrintAll();

 private:
  std::map<int, DprdBucket*> buckets_;
  std::map<int, DprdRule*> rules_;
  std::map<std::string, int> name_id_;

  int max_bucket_;
  int sum_weight_;
};

#endif  // MANAGER_UTILS_DPRD_TYPE_H_
