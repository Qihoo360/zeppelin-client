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
#include "wrapper.h"

#include <sys/time.h>   // gettimeofday
#include <stdio.h>      // rename
#include <fstream>      // ifstream ofstream
#include <iostream>
#include <algorithm>    // std::sort
#include <cassert>      // assert
#include <cstdlib>      // std::rand, std::srand
#include <set>

bool debug_op = false;

DprdWrapper::DprdWrapper()
  :changed_partition_counter_(0), map_(NULL) {
}

DprdWrapper::~DprdWrapper() {
  if (map_) {
    delete map_;
  }
}

bool CompareFunction(std::pair<double, int> a, std::pair<double, int> b) {
  return a.first < b.first;
}

void DprdWrapper::CreateMap() {
  map_ = new DprdMap();
}

// 1. update parents info about this bucket child
// 2. if this bucket is BUCKET_TYPE_NODE type update weight to all its ancestors
// 3. set its ip and port
bool DprdWrapper::AddBucket(int parent,
    int type, int id, int weight, const std::string& ip, int port) {
  assert(map_);
  DprdBucket* dprd_bucket = new DprdBucket(id, type, weight, parent);
  map_->InsertBucket(id, dprd_bucket);
  map_->IncMaxBucket();
  // this is an root do not need to add child
  if (id == 0) {
    return true;
  }
  // add bucket to parent's child
  DprdBucket* parent_bucket = map_->FindBucket(parent);
  assert(parent_bucket);
  parent_bucket->AddChild(id);
  // if bucket type is BUCKET_TYPE_NODE type, update sum_weight
  if (type == BUCKET_TYPE_NODE) {
    UpdateWeightToTop(id, weight);
    map_->set_sum_weight(map_->sum_weight() + weight);
  }
  // set ip and port
  dprd_bucket->set_ip(ip);
  dprd_bucket->set_port(port);
  return true;
}

// just add bucket into map_
bool DprdWrapper::AddBucket(int type, int id, const std::string& name,
    int weight) {
  assert(map_);
  DprdBucket* dprd_bucket = new DprdBucket(id, type, weight, name);
  map_->InsertBucket(id, dprd_bucket);
  map_->IncMaxBucket();
  return true;
}

// Optimized shrink stategy
// Two conditions:
// 1. remove node id's level is bigger then choose_n rule level
//   means one partition is under this node's ancestorA(node go up to
//   choose_n level), another two replicas is under same level other
//   ancestors(ancestorB and ancestorC)
//   1.1 remove this partition from this id node
//   1.2 update weight
//   1.3 remove parents child
//   1.5 delete bucket
//   1.6 distribute this partition from choose_n level buckets(choose
//   one to distribute except ancestorB and ancestorC)
// 2. remove node id's level is less then choose_n rule level
//   means cut node will cut all replica partiitions
//   just distribute from id's parent
bool DprdWrapper::RemoveBucket(int id) {
  if (id == 0) {
    std::cout<< "Don't support remove root!" << std::endl;
    return false;
  }
  DprdBucket* bucket = map_->FindBucket(id);
  assert(bucket);
  int bucket_level = FindBucketLevel(bucket);

  int choose_n_level = 0;
  DprdRule* rule = map_->FindRule(0);
  const std::vector<DprdRuleStep*>& steps = rule->steps();
  for (size_t i = 0; i < steps.size(); ++i) {
    if (steps[i]->arg1() > 1) {
      choose_n_level = i;
      break;
    }
  }

  // partition and one of the leaf node which under id subtree
  std::map<int, int> partition_leaf;
  int upper_level_id = bucket->parent();
  bool easy_distribute = false;
  // partition and ancestors (buckets in choose_n level who has this partition)
  std::map<int, std::vector<int> > partitions_ancestors;
  if (choose_n_level > bucket_level) {
    easy_distribute = true;
  } else {
    // id goes up to choose_n_level to know which id is in this level
    int level_diff = bucket_level - choose_n_level;
    int choose_n_level_id = id;
    for (int i = 0; i < level_diff; ++i) {
      DprdBucket* cur = map_->FindBucket(choose_n_level_id);
      assert(cur);
      choose_n_level_id = cur->parent();
    }

    // get choose_n_level's parent
    DprdBucket* choose_n_level_bucket = map_->FindBucket(choose_n_level_id);
    assert(choose_n_level_bucket);
    upper_level_id = choose_n_level_bucket->parent();

    const std::set<int>& partitions = bucket->partitions();
    std::set<int>::const_iterator partitions_iter = partitions.begin();
    for (; partitions_iter != partitions.end(); ++partitions_iter) {
      int partition = *partitions_iter;
      // std::cout<< "partition: " << partition<< std::endl;
      std::map<int, std::vector<int> >::iterator par_map_iter
        = partitions_to_nodes_.find(partition);
      if (par_map_iter == partitions_to_nodes_.end()) {
        std::cout<< "partition: " << partition <<
          "doesn't exist in partitions_to_nodes" << std::endl;
        return false;
      }
      std::vector<int>& nodes = par_map_iter->second;
      std::vector<int> ancestors;
      for (size_t i = 0; i < nodes.size(); ++i) {
        int cur_bucket_id = nodes[i];
        DprdBucket* cur_bucket = map_->FindBucket(cur_bucket_id);
        int node_level = FindBucketLevel(cur_bucket);
        int level_diff = node_level - choose_n_level;
        int tmp_id = cur_bucket_id;
        DprdBucket* tmp_bucket = cur_bucket;
        // find this leaf's ancestor in choose_n level
        for (int j = 0; j < level_diff; ++j) {
          tmp_bucket = map_->FindBucket(tmp_id);
          assert(tmp_bucket);
          tmp_id = tmp_bucket->parent();
        }
        if (tmp_id == choose_n_level_id) {
          // find this leaf's ancestor in choose_n level
          partition_leaf[partition] = cur_bucket_id;
          continue;
        }
        ancestors.push_back(cur_bucket_id);
      }
      partitions_ancestors[partition] = ancestors;
    }
  }

  // hard copy here
  // store partitions bucket will be deleted later
  std::set<int> partitions = bucket->partitions();
  // Remove partition
  std::set<int>::iterator partitions_iter = partitions.begin();
  for (; partitions_iter != partitions.end(); ++partitions_iter) {
    RemoveBucketPartition(bucket, *partitions_iter);
  }

  // update weight
  int weight = -1 * bucket->weight();
  UpdateWeightToTop(id, weight);

  // update child in parent
  DprdBucket* parent = map_->FindBucket(bucket->parent());
  assert(parent);
  parent->RemoveChild(id);

  if (!LevelOrderTraversalRemove(bucket)) {
    return false;
  }

  // Find id bucekt's parent and do distribute
  if (easy_distribute) {
    std::cout<< "easy distribute" << std::endl;
    DprdBucket* upper_level_bucket = map_->FindBucket(upper_level_id);
    int level = FindBucketLevel(upper_level_bucket);
    std::set<int>::iterator partitions_iter = partitions.begin();
    for (; partitions_iter != partitions.end(); ++partitions_iter) {
      int partition = *partitions_iter;
      Distribute(upper_level_id, partition, level, 0);
    }
    return true;
  }

  std::map<int, std::vector<int> >::iterator distribute_iter
    = partitions_ancestors.begin();
  // upper_level_id refers to choose_n_level parent
  DprdBucket* upper_bucket = map_->FindBucket(upper_level_id);
  for (; distribute_iter != partitions_ancestors.end(); ++distribute_iter) {
    int partition = distribute_iter->first;
    std::vector<int>& block_list = distribute_iter->second;
    const std::vector<int>& choose_n_level_buckets = upper_bucket->children();
    std::vector<int> processed_ids;
    for (size_t i = 0; i < choose_n_level_buckets.size(); ++i) {
      int node_id = choose_n_level_buckets[i];
      bool blocked = false;
      // if this ancestor is record in block_list choose other one
      // record means this pg is already exist in this bucket
      for (size_t j = 0; j < block_list.size(); ++j) {
        if (block_list[j] == node_id) {
          blocked = true;
          break;
        }
      }
      if (blocked) {
        continue;
      }
      processed_ids.push_back(choose_n_level_buckets[i]);
    }

    // calculate Factor shuffle it and sort it, then find the smallest one
    std::vector<double> factors = CalcFactor(processed_ids);
    std::vector<std::pair<double, int> > factors_children;
    // build factors_children to sort it and choose first N
    for (size_t j = 0; j < processed_ids.size() && j < factors.size(); ++j) {
      factors_children.push_back(std::make_pair(factors[j], processed_ids[j]));
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    std::srand(tv.tv_usec);
    std::random_shuffle(factors_children.begin(), factors_children.end());

    std::sort(factors_children.begin(), factors_children.end(),
      CompareFunction);

    // choose the smallest candidate of choose_n level bucket and do distribute
    if (!factors_children.empty()) {
      int id = factors_children.begin()->second;
      DprdBucket* bucket = map_->FindBucket(id);
      assert(bucket);
      int level = FindBucketLevel(bucket);
      // std::cout<< "Distribute id: "<< id << " partition " << partition
      // << " level " << level<< std::endl;
      std::map<int, int>::iterator par_leaf_iter =
        partition_leaf.find(partition);
      if (par_leaf_iter == partition_leaf.end()) {
        return false;
      }
      Distribute(id, partition, level, 0, par_leaf_iter->second);
    }
  }

  map_->DecMaxBucket();
  return true;
}

bool DprdWrapper::LevelOrderTraversalRemove(const DprdBucket* root) {
  std::vector<int> queue;
  queue.push_back(root->id());
  assert(map_);
  while (!queue.empty()) {
    std::vector<int> layer;
    for (size_t i = 0; i < queue.size(); ++i) {
      DprdBucket* cur_bucket = map_->FindBucket(queue[i]);
      assert(cur_bucket);
      const std::vector<int>& children = cur_bucket->children();
      for (size_t j = 0; j < children.size(); ++j) {
         layer.push_back(children[j]);
      }
      if (!map_->RemoveBucket(cur_bucket->id())) {
        return false;
      }
      delete cur_bucket;
    }
    queue = layer;
  }
  return true;
}

bool DprdWrapper::AddRule(int id) {
  assert(map_);
  DprdRule* Dprd_rule = new DprdRule(id);
  map_->InsertRule(id, Dprd_rule);
  return true;
}

bool DprdWrapper::AddStep(int rule_id, int step_id,
    const int op, const int arg1, const int arg2) {
  assert(map_);
  DprdRule* rule = map_->FindRule(rule_id);
  if (rule == NULL) {
    return false;
  }
  DprdRuleStep* new_step = new DprdRuleStep(op, arg1, arg2);
  rule->InsertStep(new_step);
  return true;
}

// Remove this partition to target bucket and update info to all its ancestors
bool DprdWrapper::RemoveBucketPartition(DprdBucket* target, int partition) {
  DprdBucket* cur = target;
  while (cur->type() != BUCKET_TYPE_ROOT) {
    const std::set<int>& partitions = cur->partitions();
    std::set<int>::const_iterator partitions_itr = partitions.find(partition);
    if (partitions_itr == partitions.end()) {
      // should have this partition
      return false;
    }
    cur->RemovePartition(partition);
    assert(map_);
    cur = map_->FindBucket(cur->parent());
    assert(cur);
  }
  return true;
}

// Insert this partition to target bucket and update info to all its ancestors
bool DprdWrapper::InsertBucketPartition(DprdBucket* target, int partition) {
  // std::cout<< "Func: InsertBucketpartition " << std::endl;
  DprdBucket* cur = target;
  while (cur->type() != BUCKET_TYPE_ROOT) {
    bool res = cur->InsertPartition(partition);
    if (!res) {
      return false;
    }
    assert(map_);
    cur = map_->FindBucket(cur->parent());
    assert(cur);
  }
  return true;
}

// choose the first N nodes who have the least factor
void DprdWrapper::ChooseFirstN(const std::vector<DprdBucket*>& input,
    std::vector<DprdBucket*>* output, int n) {
  // std::cout<< "Function: ChooseFirstN()" << std::endl;
  assert(map_);
  for (size_t i = 0; i < input.size(); ++i) {
    const DprdBucket* bucket = input[i];
    const std::vector<int>& children = bucket->children();
    std::vector<double> factors = CalcFactor(children);
    std::vector<std::pair<double, int> > factors_children;
    // build factors_children to sort it and choose first N
    for (size_t j = 0; j < children.size() && j < factors.size(); ++j) {
      factors_children.push_back(std::make_pair(factors[j], children[j]));
    }
    // shuffle factors_children to make it uniform distributed
    struct timeval tv;
    gettimeofday(&tv, NULL);
    std::srand(tv.tv_usec);
    std::random_shuffle(factors_children.begin(), factors_children.end());

    std::sort(factors_children.begin(), factors_children.end(),
        CompareFunction);
    // choose first N
    for (size_t j = 0; j < static_cast<size_t>(n) &&
        j < factors_children.size(); ++j) {
      DprdBucket* bucket = map_->FindBucket(factors_children[j].second);
      assert(bucket);
      (*output).push_back(bucket);
    }
  }
}

// old_belonging is the old node id of this partition
// level describes which layer this partition will start distribution
bool DprdWrapper::Distribute(int root_id, int partition, int level,
    int ruleno, int old_belonging) {
  // std::cout<< "Function:Distribute():" << std::endl;
  assert(map_);
  DprdBucket* root = map_->FindBucket(root_id);
  assert(root);
  std::vector<DprdBucket*> input;
  // if level is not zero distribute starts in that root_id node
  if (level != 0) {
    input.push_back(root);
    level++;
  }
  DprdRule* rule = map_->FindRule(ruleno);
  const std::vector<DprdRuleStep*>& steps = rule->steps();
  for (size_t i = level; i < steps.size(); ++i) {
    std::vector<DprdBucket*> output;
    DprdRuleStep* step = steps[i];
    int op = step->op();
    int arg1 = step->arg1();
    switch (op) {
      case DPRD_RULE_TAKE:
        input.push_back(root);
        break;
      case DPRD_RULE_CHOOSE_FIRSTN:
        ChooseFirstN(input, &output, arg1);
        input = output;
        break;
      case DPRD_RULE_EMIT:
      {
        // bucket id of input
        std::vector<int> nodes;
        for (size_t i = 0; i < input.size(); ++i) {
          DprdBucket* bucket = input[i];
          nodes.push_back(bucket->id());
          InsertBucketPartition(bucket, partition);
        }
        // update partitions_to_nodes map
        if (input.size() == REPLICA_NUM) {
          std::map<int, std::vector<int> >::iterator iter
            = partitions_to_nodes_.find(partition);
          if (iter != partitions_to_nodes_.end()) {
            changed_partition_counter_ += REPLICA_NUM;
          }
          partitions_to_nodes_[partition] = nodes;
        }
        // the old_belong exist and now it changed, record this change
        // and update partitions_to_nodes
        if (old_belonging != -1 && input.size() == 1) {
          std::map<int, std::vector<int> >::iterator iter
            = partitions_to_nodes_.find(partition);
          if (iter != partitions_to_nodes_.end()) {
            std::vector<int>& nodes = iter->second;
            for (size_t i = 0; i < nodes.size(); ++i) {
              if (nodes[i] == old_belonging) {
                nodes[i] = input[0]->id();
                changed_partition_counter_++;
                break;
              }
            }
          }
        }
        break;
      }
      default:
        break;
    }
  }
  return true;
}

int DprdWrapper::FindBucketLevel(const DprdBucket* bucket) {
  const DprdBucket* cur = bucket;
  int res = 0;
  assert(map_);
  while (cur->type() != BUCKET_TYPE_ROOT) {
    res++;
    cur = map_->FindBucket(cur->parent());
  }
  return res;
}

// Calling before removing partition from this bucket
// if this bucket's level lower level rule need to choose 3,
// it needs to remove all its 3 partitions from other buckts
bool DprdWrapper::CheckMoveAllReplicas(const DprdBucket* bucket) {
  int level = FindBucketLevel(bucket);
  assert(map_);
  DprdRule* rule = map_->FindRule(0);
  const std::vector<DprdRuleStep*>& steps = rule->steps();
  // To find child to bottom if corresponding step has chooseFirstN n > 1 case
  for (size_t i = level + 1; i < steps.size(); ++i) {
    if (steps[i]->arg1() > 1) {
      return true;
    }
  }
  return false;
}

// To balance this bucekt's children
// 1. get src pos (need to remove pg in this level)
//   and dst (receive pg in this level)
// 2. get one of the leaf's partition of src pos
// 3. remove partition of leaf
// 4. distribute this partition to dst
void DprdWrapper::BalanceChildren(const DprdBucket* bucket) {
  const std::vector<int>& children = bucket->children();
  std::vector<int> src;
  std::vector<int> dst;
  std::set<std::pair<int, int> > history;
  assert(map_);
  while (!Balanced(children, &src, &dst)) {
    int partition = -1;
    DprdBucket* partition_from = NULL;
    bool res = false;
    int src_pos = -1;
    int dst_pos = -1;
    DprdBucket* dst_bucket = NULL;
    DprdBucket* src_bucket = NULL;
    // find one partition from all possible
    // src(over average bucket) and dst(below average bucket)
    for (size_t i = 0; i < dst.size(); ++i) {
      dst_bucket = map_->FindBucket(dst[i]);
      assert(dst_bucket);
      for (size_t j = 0; j < src.size(); ++j) {
        src_bucket = map_->FindBucket(src[j]);
        assert(src_bucket);
        res = ChooseNodeToRemove(src_bucket, dst_bucket, &partition,
            &partition_from);
        if (res) {
          src_pos = src[j];
          dst_pos = dst[i];
          break;
        }
      }
      if (res) {
        break;
      }
    }

    if (!res)  {
      std::cout<< "Can't find partition to move!!!!" << std::endl;
      if (debug_op) {
        std::cout<< "src "<< std::endl;
        for (size_t i = 0; i < src.size(); ++i) {
          std::cout << "id: " << src[i];
          DprdBucket* bucket = map_->FindBucket(src[i]);
          std::cout << " type: " << bucket->type();
          const std::vector<int>& children = bucket->children();
          std::vector<int>::const_iterator children_iter = children.begin();
          std::cout<< "children "<< std::endl;
          for (; children_iter != children.end(); ++children_iter) {
            std::cout<< " " << *children_iter;
          }
          std::cout<< std::endl;
          const std::set<int>& partitions = bucket->partitions();
          std::set<int>::const_iterator partition_iter = partitions.begin();
          std::cout<< "partition "<< std::endl;
          for (; partition_iter != partitions.end(); ++partition_iter) {
            std::cout<< " "<< *partition_iter;
          }
          std::cout<< std::endl;
          std::cout<< "size: "<< partitions.size()<< std::endl;
          std::cout<< std::endl;
        }
        std::cout<< std::endl;
        std::cout<< "dst " << std::endl;
        for (size_t i = 0; i < dst.size(); ++i) {
          std::cout<< dst[i];
          DprdBucket* bucket = map_->FindBucket(dst[i]);
          std::cout << " type: " << bucket->type();
          const std::vector<int>& children = bucket->children();
          std::vector<int>::const_iterator children_iter = children.begin();
          std::cout<< "children "<< std::endl;
          for (; children_iter != children.end(); ++children_iter) {
            std::cout<< " " << *children_iter;
          }
          std::cout<< std::endl;
          const std::set<int>& partitions = bucket->partitions();
          std::set<int>::const_iterator partition_iter = partitions.begin();
          std::cout<< "partition "<< std::endl;
          for (; partition_iter != partitions.end(); ++partition_iter) {
            std::cout<< " "<< *partition_iter;
          }
          std::cout<< std::endl;
          std::cout<< "size: "<< partitions.size()<< std::endl;
          std::cout<< std::endl;
        }
        std::cout<< std::endl;
      }
      return;
    }

    src.erase(src.begin(), src.end());
    dst.erase(dst.begin(), dst.end());

    // check shacking like posA to posB then posB to posA agian
    if (history.find(std::make_pair(dst_pos, src_pos)) != history.end()) {
      std::cout<< "Shaking!!!" << std::endl;
      return;
    } else {
      history.insert(std::make_pair(src_pos, dst_pos));
    }

    // Remove choosen partition from src_bucket
    if (CheckMoveAllReplicas(src_bucket)) {
      std::map<int, std::vector<int> >::iterator iter
        = partitions_to_nodes_.find(partition);
      std::vector<int> nodes;
      if (iter != partitions_to_nodes_.end()) {
        nodes = iter->second;
      }
      for (size_t i = 0; i < nodes.size(); ++i) {
        DprdBucket* bucket = map_->FindBucket(nodes[i]);
        assert(bucket);
        RemoveBucketPartition(bucket, partition);
      }
    } else {
      RemoveBucketPartition(partition_from, partition);
    }

    // distribute partition to dst_bucekt
    int level = FindBucketLevel(dst_bucket);
    Distribute(dst_pos, partition, level, 0, partition_from->id());
    if (debug_op) {
      std::cout<< "From " << src_bucket->id() << " ===>"
        << " To " << dst_bucket->id() << std::endl;
      std::cout<< "Get Down to get node" << partition_from->id()
        << " Partition: " << partition << std::endl;
    }
  }
}

void DprdWrapper::BuildCandidate(const DprdBucket* root,
    std::vector<int>* candidate) {
  const std::vector<int>& children = root->children();
  double average_factor = CalcAverageFactor(children);
  std::vector<double> factor = CalcFactor(children);
  std::vector<double> base = CalcBaseFactor(children);
  if (children.size() != factor.size()) {
    return;
  }
  // keep candidate from max to min according to the factor :
  // (factor[i] - average_factor) / base[i]
  std::multimap<double, int> res;
  for (size_t i = 0; i < factor.size(); ++i) {
    double distance = (factor[i] - average_factor) / base[i];
    res.insert(std::make_pair(distance, children[i]));
  }
  std::map<double, int>::reverse_iterator ritr;
  for (ritr = res.rbegin(); ritr != res.rend(); ++ritr) {
    (*candidate).push_back(ritr->second);
  }
}


// valid partition exist in root but not in dst
bool DprdWrapper::ChoosePartitionToRemove(const DprdBucket* root,
    const DprdBucket* dst, int* partition) {
  const std::set<int>& partitions = root->partitions();
  const std::set<int>& dst_partitions = dst->partitions();
  std::set<int>::const_iterator partitions_iter = partitions.begin();
  for (; partitions_iter != partitions.end(); ++partitions_iter) {
    int cur_partition = *partitions_iter;
    if (dst_partitions.find(cur_partition) != dst_partitions.end()) {
      continue;
    } else {
      (*partition) = cur_partition;
      return true;
    }
  }
  return false;
}


// do recursion to go throght all bucket of root subtree
// if get any leaf partition which dst dont have, stop recurstion
// and take that partition and choosen_node back
bool DprdWrapper::ChooseNodeToRemove(DprdBucket* root, const DprdBucket* dst,
    int* partition, DprdBucket** choosen_node) {
  if (root->type() == BUCKET_TYPE_NODE) {
    if (ChoosePartitionToRemove(root, dst, partition)) {
      *choosen_node = root;
      return true;
    } else {
      return false;
    }
  }
  std::vector<int> candidate;
  BuildCandidate(root, &candidate);
  assert(map_);
  for (size_t i = 0; i < candidate.size(); ++i) {
    DprdBucket* child_bucket = map_->FindBucket(candidate[i]);
    assert(child_bucket);
    bool res = ChooseNodeToRemove(child_bucket, dst, partition, choosen_node);
    if (res) {
      return res;
    }
  }
  return false;
}

double DprdWrapper::CalcAverageFactor(const std::vector<int>& buckets) {
  int sum_partitions = 0;
  int sum_weights = 0;
  assert(map_);
  for (size_t i = 0; i < buckets.size(); ++i) {
    DprdBucket* bucket = map_->FindBucket(buckets[i]);
    assert(bucket);
    sum_partitions += bucket->partition_size();
    sum_weights += bucket->weight();
  }
  if (sum_weights == 0) {
    return -1;
  }
  return sum_partitions * (static_cast<double>(map_->sum_weight())
      / static_cast<double>(sum_weights));
}

std::vector<double> DprdWrapper::CalcFactor(const std::vector<int>& nodes) {
  assert(map_);
  int sum_weight = map_->sum_weight();
  std::vector<double> factors;
  for (size_t i = 0; i < nodes.size(); ++i) {
    DprdBucket* bucket = map_->FindBucket(nodes[i]);
    assert(bucket);
    int weight = bucket->weight();
    int partition_num = bucket->partition_size();
    std::vector<DprdBucket*> output;
    double factor = partition_num * (static_cast<double>(sum_weight) / weight);
    factors.push_back(factor);
  }
  return factors;
}

std::vector<double> DprdWrapper::CalcBaseFactor(const std::vector<int>& nodes) {
  assert(map_);
  int sum_weight = map_->sum_weight();
  std::vector<double> base_factors;
  for (size_t i = 0; i < nodes.size(); ++i) {
    DprdBucket* bucket = map_->FindBucket(nodes[i]);
    assert(bucket);
    int weight = bucket->weight();
    std::vector<DprdBucket*> output;
    double factor = (static_cast<double>(sum_weight) / weight);
    base_factors.push_back(factor);
  }
  return base_factors;
}

// To check if children buckets are balanced
// 1. get min and max factors' positions
//    it is possible that sevral positions holds the same
//    min or max factor.
// 2. get positions of factors which is over average factor too much
//    get positions of factors which is below average factor too much
// 3. if both of src_pos and dst_pos is empty return true
//    that means all children is around average.
bool DprdWrapper::Balanced(const std::vector<int>& children,
    std::vector<int>* src_pos, std::vector<int>* dst_pos) {
  std::vector<double> factors;
  factors = CalcFactor(children);
  std::vector<double> base_factors;  // sum_weight over weight
  base_factors = CalcBaseFactor(children);
  int max_factor = -1;
  int min_factor = -1;
  std::vector<int> max_pos;
  std::vector<int> min_pos;
  double average_factor = CalcAverageFactor(children);
  for (size_t i = 0; i < factors.size() && i < children.size(); ++i) {
    if (i == 0) {
      max_factor = min_factor = factors[i];
      max_pos.push_back(children[i]);
      min_pos.push_back(children[i]);
      continue;
    }
    if (factors[i] >= max_factor) {
      if (factors[i] == max_factor) {
        max_pos.push_back(children[i]);
      } else {
        max_pos.erase(max_pos.begin(), max_pos.end());
        max_pos.push_back(children[i]);
      }
      max_factor = factors[i];
    }
    if (factors[i] <= min_factor) {
      if (factors[i] == min_factor) {
        min_pos.push_back(children[i]);
      } else {
        min_pos.erase(min_pos.begin(), min_pos.end());
        min_pos.push_back(children[i]);
      }
      min_factor = factors[i];
    }
  }
  for (size_t i = 0; i < factors.size() && i < base_factors.size() &&
      i < children.size(); ++i) {
    if (factors[i] - average_factor > base_factors[i]) {
      (*src_pos).push_back(children[i]);
    }
    if (average_factor - factors[i] > base_factors[i]) {
      (*dst_pos).push_back(children[i]);
    }
  }
  if ((*src_pos).empty() && (*dst_pos).empty()) {
    return true;
  } else if ((*src_pos).empty()) {
    (*src_pos) = max_pos;
  } else if ((*dst_pos).empty()) {
    (*dst_pos) = min_pos;
  }
  return false;
}

// Level Traversal toa make sure every bucket is balanced
void DprdWrapper::Migrate() {
  assert(map_);
  DprdBucket* root = map_->FindBucket(0);
  assert(root);
  LevelOrderTraversalBalance(root);
}

// level order traversal of bucket-tree to make sure this tree is balanced
void DprdWrapper::LevelOrderTraversalBalance(const DprdBucket* root) {
  std::vector<int> queue;
  queue.push_back(root->id());
  assert(map_);
  while (!queue.empty()) {
    std::vector<int> layer;
    for (size_t i = 0; i < queue.size(); ++i) {
      DprdBucket* cur_bucket = map_->FindBucket(queue[i]);
      assert(cur_bucket);
      if (cur_bucket->type() == BUCKET_TYPE_NODE) {
        continue;
      }
      BalanceChildren(cur_bucket);
      const std::vector<int>& children = cur_bucket->children();
      for (size_t j = 0; j < children.size(); ++j) {
         layer.push_back(children[j]);
      }
    }
    queue = layer;
  }
}

// update weight to all "id"'s ancestors
void DprdWrapper::UpdateWeightToTop(int id, int weight) {
  assert(map_);
  DprdBucket* cur_bucket = map_->FindBucket(id);
  assert(cur_bucket);
  cur_bucket = map_->FindBucket(cur_bucket->parent());
  assert(cur_bucket);
  while (cur_bucket->type() != BUCKET_TYPE_ROOT) {
    cur_bucket->set_weight(cur_bucket->weight() + weight);
    cur_bucket = map_->FindBucket(cur_bucket->parent());
    assert(cur_bucket);
  }
  cur_bucket->set_weight(cur_bucket->weight() + weight);
}

// print partition to nodes map into stdout
void DprdWrapper::DumpPartitionNodeMap() {
  std::map<int, std::vector<int> >::iterator iter
    = partitions_to_nodes_.begin();
  for (; iter != partitions_to_nodes_.end(); ++iter) {
    int partition = iter->first;
    std::vector<int>& node = iter->second;
    std::cout<< "Partition : " << partition << ", ";
    for (size_t j = 0; j < node.size(); ++j) {
      std::cout<< node[j]<< " ";
    }
    std::cout << std::endl;
  }
}

// when adding node to map, upper level weight will be updated
// map->SumWeight will be updated
void DprdWrapper::BuildTree(int rack_size, const std::vector<int>& hosts_size,
    int node_size) {
  assert(map_);
  int root_id = 0;
  AddBucket(0, BUCKET_TYPE_ROOT, root_id);
  DprdBucket* root = map_->FindBucket(root_id);
  root->set_name("root");
  int negative_id = -1;
  int positive_id = 1;
  for (int rack = 0; rack < rack_size; ++rack) {
    int rack_id = negative_id;
    AddBucket(root_id, BUCKET_TYPE_RACK, negative_id--);
    DprdBucket* rack_bucket = map_->FindBucket(rack_id);
    rack_bucket->set_name("rack" + std::to_string(rack_id));
    int host_size = hosts_size[rack];
    for (int host = 0; host < host_size; ++host) {
      int host_id = negative_id;
      AddBucket(rack_id, BUCKET_TYPE_HOST, negative_id--);
      DprdBucket* host_bucket = map_->FindBucket(host_id);
      host_bucket->set_name("host" + std::to_string(host_id));
      for (int node = 0; node < node_size; ++node) {
        int node_id = positive_id;
        AddBucket(host_id, BUCKET_TYPE_NODE, positive_id++, 1);
        DprdBucket* node_bucket = map_->FindBucket(node_id);
        node_bucket->set_name("node" + std::to_string(node_id));
        node_bucket->set_ip("1.1.1.1");
        node_bucket->set_port(1111);
      }
    }
  }
}

bool DprdWrapper::AddStepFromFile(const std::string& rule) {
  assert(map_);
  if (map_->FindRule(0) == NULL) {
    AddRule(0);
    AddStep(0, 0, DPRD_RULE_TAKE, 0, 0);
  }
  int step = 0;
  if (rule.find("rack") != std::string::npos) {
    step = 1;
  } else if (rule.find("host") != std::string::npos) {
    step = 2;
  } else if (rule.find("node") != std::string::npos) {
    step = 3;
  } else {
    return false;
  }
  std::size_t found = rule.find_first_of("0123456789");
  if (found == std::string::npos) {
    std::cout<< "Wrong rule format!" << std::endl;
    return false;
  }
  int choose_num =  rule[found] - '0';
  AddStep(0, step, DPRD_RULE_CHOOSE_FIRSTN, choose_num, 0);
  if (step == 3) {
    AddStep(0, 4, DPRD_RULE_EMIT, 0, 0);
  }
  return true;
}

bool DprdWrapper::AddNodeFromFile(std::ifstream& in, const std::string& buf) {
  std::string name(buf);
  std::string cur_buf;
  std::string ip;
  int id = 0, port = 0;
  int counter = 0;
  while (!in.eof()) {
    std::getline(in, cur_buf);
    RemoveHeadTailWhiteSpaces(&cur_buf);
    if (cur_buf.find("id") != std::string::npos) {
      std::size_t id_pos = cur_buf.find_first_of("-0123456789");
      if (id_pos != std::string::npos) {
        id = std::stoi(cur_buf.substr(id_pos));
      }
    } else if (cur_buf.find("ip") != std::string::npos) {
      std::size_t ip_pos = cur_buf.find_first_of("0123456789");
      if (ip_pos != std::string::npos) {
        ip = cur_buf.substr(ip_pos);
      }
    } else if (cur_buf.find("port") != std::string::npos) {
      std::size_t port_pos = cur_buf.find_first_of("0123456789");
      if (port_pos != std::string::npos) {
        port = std::stoi(cur_buf.substr(port_pos));
      }
    }
    counter++;
    if (counter == 3) {
      AddBucket(BUCKET_TYPE_NODE, id, name, 1);
      DprdBucket* new_bucket = map_->FindBucket(id);
      assert(new_bucket);
      new_bucket->set_ip(ip);
      new_bucket->set_port(port);
      break;
    }
  }
  if (counter != 3) {
    std::cout<< "Missing necessary parameter(id, ip or port)!" <<std::endl;
    return false;
  }
  return true;
}

// Add abstract bucket
bool DprdWrapper::AddBucketFromFile(std::ifstream& in, const std::string& buf) {
  std::string name(buf);
  int type = 0;
  if (buf.find("host") != std::string::npos) {
    type = BUCKET_TYPE_HOST;
  } else if (buf.find("rack") != std::string::npos) {
    type = BUCKET_TYPE_RACK;
  } else if (buf.find("root") != std::string::npos) {
    type = BUCKET_TYPE_ROOT;
  } else {
    return false;
  }
  std::string cur_buf;
  int id = 0;
  std::vector<std::string> children;
  bool children_flag = false;
  // read from "in", until hit a blank line or "# bucket end"
  // then
  // 1. create bucket with type id and name
  // 2. add child to cur bucekt
  // 3. update cur bucket weight
  while (!in.eof()) {
    std::getline(in, cur_buf);
    RemoveHeadTailWhiteSpaces(&cur_buf);
    if (cur_buf.empty() || cur_buf.find("# bucket end") != std::string::npos) {
      AddBucket(type, id, name);
      DprdBucket* bucket = map_->FindBucket(id);
      assert(bucket);
      int weight = 0;
      for (size_t i = 0; i < children.size(); ++i) {
        int child_id;
        if (map_->FindId(children[i], &child_id)) {
          bucket->AddChild(child_id);
        }
        DprdBucket* child = map_->FindBucket(child_id);
        if (child == NULL) {
          std::cout << "child id " << child_id << "is not initialized yet!"
            << std::endl;
          return false;
        }
        weight += child->weight();
      }
      bucket->set_weight(weight);
      children_flag = false;
      return true;
    } else if (cur_buf.find("id") != std::string::npos) {
      std::size_t id_pos = cur_buf.find_first_of("-0123456789");
      if (id_pos != std::string::npos) {
        id = std::stoi(cur_buf.substr(id_pos));
      }
    } else if (children_flag) {
      children.push_back(cur_buf);
    } else if (cur_buf.find("children") != std::string::npos) {
      children_flag = true;
    }
  }
  return true;
}

void DprdWrapper::RemoveHeadTailWhiteSpaces(std::string* buf) {
  std::string white_spaces(" \t\f\v\n\r");
  std::size_t found = buf->find_last_not_of(white_spaces);
  if (found != std::string::npos) {
    buf->erase(found + 1);
  }
  found = buf->find_first_not_of(white_spaces);
  if (found != std::string::npos) {
    *buf = buf->substr(found);
  } else {
    buf->erase(buf->begin(), buf->end());
  }
}

// Load tree type topology from file
bool DprdWrapper::LoadTree(const std::string& file) {
  assert(map_);
  std::ifstream in(file);
  if (!in.is_open()) {
    std::cout << "Open Failed" << std::endl;
    return false;
  }
  std::string buf;
  bool node_flag = false;
  bool bucket_flag = false;
  bool rule_flag = false;
  while (!in.eof()) {
    std::getline(in, buf);
    RemoveHeadTailWhiteSpaces(&buf);
    if (buf.empty()) {
      continue;
    }
    if (buf.find('#') != std::string::npos) {
      if (buf.find("rule") != std::string::npos) {
        if (buf.find("end") != std::string::npos) {
          rule_flag = false;
        } else {
          rule_flag = true;
        }
      } else if (buf.find("node") != std::string::npos) {
        if (buf.find("end") != std::string::npos) {
          node_flag = false;
        } else {
          node_flag = true;
        }
      } else if (buf.find("bucket") != std::string::npos) {
        if (buf.find("end") != std::string::npos) {
          bucket_flag = false;
        } else {
          bucket_flag = true;
        }
      }
    } else {
      if (rule_flag) {
        if (!AddStepFromFile(buf)) {
          return false;
        }
      } else if (node_flag) {
        if (buf.find("node") == std::string::npos) {
          return false;
        }
        if (!AddNodeFromFile(in, buf)) {
          return false;
        }
      } else if (bucket_flag) {
        if (buf.find("rack") == std::string::npos &&
            buf.find("host") == std::string::npos &&
            buf.find("root") == std::string::npos) {
          return false;
        }
        if (!AddBucketFromFile(in, buf)) {
          return false;
        }
      }
    }
  }
  DprdBucket* root_bucket = map_->FindBucket(0);
  map_->set_sum_weight(root_bucket->weight());
  return true;
}

// Dump rule steps
// By default, there is one rule(rule0) and 3 steps intotal
void DprdWrapper::DumpRule(std::ofstream& ofs) {
  assert(map_);
  int layer_counter = 1;
  const std::map<int, DprdRule*>& rules = map_->rules();
  std::map<int, DprdRule*>::const_iterator rule_iter = rules.begin();
  for (; rule_iter != rules.end(); ++rule_iter) {
    DprdRule* rule = rule_iter->second;
    const std::vector<DprdRuleStep*>& steps = rule->steps();
    ofs << "# rule" << std::endl;
    for (size_t i = 0; i < steps.size(); ++i) {
      DprdRuleStep* step = steps[i];
      if (step->op() == DPRD_RULE_CHOOSE_FIRSTN) {
        if (layer_counter == 1) {
          ofs << "rack:choose " << step->arg1()<< std::endl;
          layer_counter++;
        } else if (layer_counter == 2) {
          ofs << "host:choose " << step->arg1()<< std::endl;
          layer_counter++;
        } else if (layer_counter == 3) {
          ofs << "node:choose " << step->arg1()<< std::endl;
          layer_counter++;
        }
      }
    }
    ofs << "# rule end" << std::endl << std::endl;
  }
}

// Dump Node bucket info into ofs
void DprdWrapper::DumpNode(std::ofstream& ofs) {
  assert(map_);
  ofs << "# node" << std::endl;
  const std::map<int, DprdBucket*>& buckets = map_->buckets();
  std::map<int, DprdBucket*>::const_iterator bucket_iter = buckets.begin();
  for (; bucket_iter != buckets.end(); ++bucket_iter) {
    DprdBucket* bucket = bucket_iter->second;
    if (!bucket->ip().empty() && bucket->port()) {
      ofs << bucket->name() << std::endl;
      ofs << "  id " << bucket->id() << std::endl;
      ofs << "  ip " << bucket->ip() << std::endl;
      ofs << "  port " << bucket->port() << std::endl;
    }
  }
  ofs << "# node end" << std::endl;
}

// Dump host rack root (all abstract bucket) info into ofs
void DprdWrapper::DumpBucket(std::ofstream& ofs) {
  assert(map_);
  ofs << "# bucket" << std::endl;
  DprdBucket* root_bucket = NULL;
  const std::map<int, DprdBucket*>& buckets = map_->buckets();
  std::map<int, DprdBucket*>::const_reverse_iterator bucket_iter
    = buckets.rbegin();
  // go through map dump host first then rack then root
  for (; bucket_iter != buckets.rend(); ++bucket_iter) {
    DprdBucket* bucket = bucket_iter->second;
    if (bucket->name().find("root") != std::string::npos) {
      root_bucket = bucket;
    }
    if (bucket->name().find("host") != std::string::npos) {
      ofs << bucket->name() << std::endl;
      ofs << "  id " << bucket->id() << std::endl;
      ofs << "  children" << std::endl;
      const std::vector<int>& children = bucket->children();
      for (size_t i = 0; i < children.size(); ++i) {
         DprdBucket* child_bucket = map_->FindBucket(children[i]);
         assert(child_bucket);
         ofs << "    "<< child_bucket->name() << std::endl;
      }
      ofs << std::endl;
    }
  }

  for (bucket_iter = buckets.rbegin(); bucket_iter != buckets.rend();
      ++bucket_iter) {
    DprdBucket* bucket = bucket_iter->second;
    if (bucket->name().find("rack") != std::string::npos) {
      ofs << bucket->name() << std::endl;
      ofs << "  id " << bucket->id() << std::endl;
      ofs << "  children" << std::endl;
      const std::vector<int>& children = bucket->children();
      for (size_t i = 0; i < children.size(); ++i) {
         DprdBucket* child_bucket = map_->FindBucket(children[i]);
         assert(child_bucket);
         ofs << "    " << child_bucket->name() << std::endl;
      }
      ofs << std::endl;
    }
  }

  if (root_bucket) {
    ofs << root_bucket->name() << std::endl;
    ofs << "  id " << root_bucket->id() << std::endl;
    ofs << "  children" << std::endl;
    const std::vector<int>& children = root_bucket->children();
    for (size_t i = 0; i < children.size(); ++i) {
      DprdBucket* child_bucket = map_->FindBucket(children[i]);
      assert(child_bucket);
      ofs << "    "<< child_bucket->name() << std::endl;
    }
  }
  ofs << "# bucket end" << std::endl;
}

// dump tree type bucket topology into file
// if the file already exist it will be overwrite
bool DprdWrapper::DumpTree(const std::string& file) {
  assert(map_);
  std::ifstream ifs(file);
  bool file_exist = false;
  if (ifs.is_open()) {
    file_exist = true;
  }
  std::string output_file = file;
  if (file_exist) {
    output_file = file + "_temp";
  }
  std::ofstream ofs(output_file);
  if (!ofs.is_open()) {
    std::cout << "Open file failed!" << std::endl;
    return false;
  }
  DumpRule(ofs);
  DumpNode(ofs);
  DumpBucket(ofs);
  if (file_exist) {
    std::string old_path = output_file;
    std::string new_path = file;
    rename(old_path.c_str(), new_path.c_str());
  }
  return true;
}

// Print map info on stdout
// Includeing all bucket info and rule info
void DprdWrapper::DumpMapInfo() {
  assert(map_);
  map_->PrintAll();
}

// @param max_bucket the maximum number of buckets in map
int DprdWrapper::max_bucket() {
  assert(map_);
  return map_->max_bucket();
}


