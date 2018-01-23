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
  kBucketTypeRoot,
  kBucketTypeRack,
  kBucketTypeHost,
  kBucketTypeNode
};

/*
 * Dprd defines rule type and its arguments
 */
struct DprdRuleStep {
  DprdRuleStep(const int op, const int arg1, const int arg2);
  ~DprdRuleStep();
  int op_;
  int arg1_;
  int arg2_;
};

/* step op codes */
enum DprdRuleRuleCodes {
  kDprdRuleNoop = 0,
  kDprdRuleTake = 1,
  kDprdRuleChooseFirstN = 2,
  kDprdRuleEmit = 3
};

struct DprdRule {
 public:
  explicit DprdRule(const int id);
  ~DprdRule();
  int id_;
  std::vector<DprdRuleStep*> steps_;
};

struct DprdBucket {
  DprdBucket(const int id, const int type, const int weight, const int parent,
      const std::string& name, const std::string& ip, const int port);
  ~DprdBucket();
  void AddChild(int id);
  void RemoveChild(int id);
  bool InsertPartition(int partition);
  bool RemovePartition(int partition);

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
struct DprdMap {
  DprdMap();
  ~DprdMap();
  bool InsertBucket(const int id, DprdBucket* bucket);
  bool RemoveBucket(int id);
  bool InsertRule(const int id, DprdRule* rule);
  DprdBucket* FindBucket(const int bucket_id);
  DprdRule* FindRule(const int rule_id);
  bool FindId(const std::string& name, int* id);
  void PrintAll();

  std::map<int, DprdBucket*> buckets_;
  std::map<int, DprdRule*> rules_;
  std::map<std::string, int> name_id_;
  int max_bucket_;
  int sum_weight_;
};

#endif  // MANAGER_UTILS_DPRD_TYPE_H_
