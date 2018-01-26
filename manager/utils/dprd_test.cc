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
#include <math.h>
#include <string>
#include <iostream>

#include "wrapper.h"

extern bool debug_op;

enum TestOp{
  kAddNone,
  kAddOneNode,
  kAddOneHost,
  kAddOneRack,
  kRemoveOneNode,
  kRemoveOneHost,
  kRemoveOneRack
};

void PrintOption(int op) {
  switch (op) {
    case kAddNone:
      std::cout<< "Basic Partition Initial Distribution" << std::endl;
      break;
    case kAddOneNode:
      std::cout<< "Add one Node"<< std::endl;
      break;
    case kAddOneHost:
      std::cout<< "Add one Host"<< std::endl;
      break;
    case kAddOneRack:
      std::cout<< "Add one Rack"<< std::endl;
      break;
    case kRemoveOneNode:
      std::cout<< "Remove one Node" << std::endl;
      break;
    case kRemoveOneHost:
      std::cout<< "Remove one Host" << std::endl;
      break;
    case kRemoveOneRack:
      std::cout<< "Remove one Rack" << std::endl;
      break;
    default:
      break;
  }
}


void PrintRes(DprdWrapper* dprd, int sum_weight, int added_weight, int pg_num) {
  std::cout<< "Changed partitions: " << dprd->changed_partition_counter()
    << std::endl;
  std::cout << "Moving rate: " <<
    static_cast<double>(dprd->changed_partition_counter()) /
    static_cast<double>(pg_num) << std::endl;
  if (added_weight < 0) {
    std::cout<< "Theoretical Moving rate: " <<
      static_cast<double>(added_weight) / static_cast<double>(sum_weight)
      << std::endl;
  } else {
    std::cout << "Theoretical Moving rate: " <<
      static_cast<double>(added_weight) /
      static_cast<double>(added_weight + sum_weight) << std::endl;
  }
  std::cout << std::endl;
}

void CheckValidRes(DprdWrapper* dprd, int sum_weight, int added_weight,
  int pg_num, int op) {
  double act_moving_rate =
    static_cast<double>(dprd->changed_partition_counter()) /
    static_cast<double>(pg_num);
  double tho_moving_rate = 0;
  if (added_weight < 0) {
    tho_moving_rate = static_cast<double>(added_weight) /
      static_cast<double>(sum_weight);
  } else {
    tho_moving_rate = static_cast<double>(added_weight) /
      static_cast<double>(added_weight + sum_weight);
  }
  if (fabs(fabs(act_moving_rate) - fabs(tho_moving_rate))
      > fabs(tho_moving_rate)) {
    std::cout << "Over limit rate too much" << std::endl;
    PrintOption(op);
    std::cout << "Changed partitions: " << dprd->changed_partition_counter()
      << std::endl;
    std::cout << "Moving rate: " << act_moving_rate << std::endl;
    std::cout<< "Theoretical Moving rate: " << tho_moving_rate << std::endl;
  }
}

void AddRules(DprdWrapper* dprd) {
  dprd->AddRule(0);
  dprd->AddStep(0, 0, kDprdRuleTake, 0, 0);
  dprd->AddStep(0, 1, kDprdRuleChooseFirstN, 3, 0);
  dprd->AddStep(0, 2, kDprdRuleChooseFirstN, 1, 0);
  dprd->AddStep(0, 3, kDprdRuleChooseFirstN, 1, 0);
  dprd->AddStep(0, 4, kDprdRuleEmit, 0, 0);
}

void AddOneNode(DprdWrapper* dprd, int parent, int weight) {
  int id = dprd->max_pos_id() + 1;
  std::string node_name = "node" + std::to_string(id);
  dprd->AddBucket(kBucketTypeNode, id, node_name, weight, parent);
}

void AddOneHost(DprdWrapper* dprd, int host_parent, int node_size) {
  int host_id = dprd->min_neg_id() - 1;
  std::string host_name = "host" + std::to_string(host_id);
  dprd->AddBucket(kBucketTypeHost, host_id, host_name, 0, host_parent);
  std::string ip = "1.1.1." + std::to_string(-host_id);
  for (int node = 0; node < node_size; ++node) {
    int node_id = dprd->max_pos_id() + 1;
    std::string node_name = "node" + std::to_string(node_id);
    int port = 1111 + node;
    dprd->AddBucket(kBucketTypeNode, node_id, node_name, 1, host_id,
        ip, port);
  }
}

void AddOneRack(DprdWrapper* dprd, int host_size, int node_size) {
  int root_id = 0;
  int rack_id = dprd->min_neg_id() - 1;
  std::string rack_name = "rack" + std::to_string(rack_id);
  dprd->AddBucket(kBucketTypeRack, rack_id, rack_name, 0, root_id);
  for (int host = 0; host < host_size; ++host) {
    int host_id = dprd->min_neg_id() - 1;
    std::string host_name = "host" + std::to_string(host_id);
    dprd->AddBucket(kBucketTypeHost, host_id, host_name, 0, rack_id);
    std::string ip = "1.1.1." + std::to_string(-host_id);
    for (int node = 0; node < node_size; ++node) {
      int node_id = dprd->max_pos_id() + 1;
      std::string node_name = "node" + std::to_string(node_id);
      int port = 1111 + node;
      dprd->AddBucket(kBucketTypeNode, node_id, node_name, 1, host_id,
          ip, port);
    }
  }
}

void DoOption(int op, int node_size, const std::vector<int> hosts_size,
    DprdWrapper* dprd, int* added_weight) {
  switch (op) {
    case kAddOneNode:
      AddOneNode(dprd, -2, 1);
      *added_weight = 1;
      break;
    case kAddOneHost:
      AddOneHost(dprd, -1, node_size);
      *added_weight = node_size;
      break;
    case kAddOneRack:
      AddOneRack(dprd, 10, 10);
      *added_weight = 10 * 10;
      break;
    case kRemoveOneNode:
      // id 1 is a node
      dprd->RemoveBucket(1);
      *added_weight = -1;
      break;
    case kRemoveOneHost:
      // id 2 is a host
      dprd->RemoveBucket(-2);
      *added_weight = -1 * node_size;
      break;
    case kRemoveOneRack:
      // id 1 is a rack
      dprd->RemoveBucket(-1);
      *added_weight = -1 * hosts_size[0] * node_size;
      break;
    default:
      break;
  }
}

void CommonTreeTest(int op, std::vector<int> hosts_size) {
  for (size_t i = 0; i < hosts_size.size(); ++i) {
    std::cout<< hosts_size[i] << " ";
  }
  std::cout<< std::endl;
  DprdWrapper* dprd = new DprdWrapper;

  int rack_size = hosts_size.size();
  int node_size = 10;

  dprd->BuildTree(rack_size, hosts_size, node_size);
  AddRules(dprd);

  // Insert partition
  // Distribute(int root_id, int partition, int level, int ruleno);
  int partition_size = 1000;
  for (int i = 0; i < partition_size; ++i) {
    dprd->Distribute(0, i, 0, 0);
  }

  int added_weight = 0;

  DoOption(op, node_size, hosts_size, dprd, &added_weight);

  dprd->Migrate();
  // dprd->DumpPartitionNodeMap();
  // dprd->DumpMapInfo();

  int sum_hosts_size = 0;
  int sum_weight = 0;
  for (size_t i = 0; i < hosts_size.size(); ++i) {
    sum_hosts_size += hosts_size[i];
  }
  sum_weight = sum_hosts_size * node_size;
  int pg_num = partition_size * 3;
  CheckValidRes(dprd, sum_weight, added_weight, pg_num, op);
  delete dprd;
}


void BalancedTreeTest(int op) {
  std::cout<< "BalancedTreeTest ";
  PrintOption(op);

  DprdWrapper* dprd = new DprdWrapper;

  int rack_size = 4;
  int node_size = 10;
  std::vector<int> hosts_size;
  hosts_size.push_back(12);
  hosts_size.push_back(12);
  hosts_size.push_back(12);
  hosts_size.push_back(12);

  dprd->BuildTree(rack_size, hosts_size, node_size);
  AddRules(dprd);

  // Insert partition
  // Distribute(int root_id, int partition, int level, int ruleno);
  int partition_size = 1000;
  for (int i = 0; i < partition_size; ++i) {
    dprd->Distribute(0, i, 0, 0);
  }

  int added_weight = 0;

  DoOption(op, node_size, hosts_size, dprd, &added_weight);

  dprd->Migrate();
  // dprd->DumpPartitionNodeMap();
  // dprd->DumpMapInfo();

  int sum_hosts_size = 0;
  int sum_weight = 0;
  for (size_t i = 0; i < hosts_size.size(); ++i) {
    sum_hosts_size += hosts_size[i];
  }
  sum_weight = sum_hosts_size * node_size;
  int pg_num = partition_size * 3;
  PrintRes(dprd, sum_weight, added_weight, pg_num);
  delete dprd;
}

void UnbalancedTreeTest(int op) {
  std::cout<< "UnbalancedTreeTest ";
  PrintOption(op);
  DprdWrapper* dprd = new DprdWrapper;

  int rack_size = 5;
  int node_size = 10;
  std::vector<int> hosts_size;

  hosts_size.push_back(18);
  hosts_size.push_back(12);
  hosts_size.push_back(10);
  hosts_size.push_back(15);
  hosts_size.push_back(16);

  dprd->BuildTree(rack_size, hosts_size, node_size);
  AddRules(dprd);

  // Insert partition
  // Distribute(int root_id, int partition, int level, int ruleno);
  int partition_size = 1000;
  for (int i = 0; i < partition_size; ++i) {
    dprd->Distribute(0, i, 0, 0);
  }

  int added_weight = 0;

  DoOption(op, node_size, hosts_size, dprd, &added_weight);

  dprd->Migrate();
  // dprd->DumpPartitionNodeMap();
  // dprd->DumpMapInfo();

  int sum_hosts_size = 0;
  int sum_weight = 0;
  for (size_t i = 0; i < hosts_size.size(); ++i) {
    sum_hosts_size += hosts_size[i];
  }
  sum_weight = sum_hosts_size * node_size;
  int pg_num = partition_size * 3;

  PrintRes(dprd, sum_weight, added_weight, pg_num);
  delete dprd;
}

void UnbalancedTreeBadPerformanceTest(int op) {
  std::cout<< "UnbalancedTreeBadPerformanceTest ";
  PrintOption(op);
  DprdWrapper* dprd = new DprdWrapper;

  int rack_size = 4;
  int node_size = 10;
  std::vector<int> hosts_size;
  hosts_size.push_back(20);
  hosts_size.push_back(15);
  hosts_size.push_back(10);
  hosts_size.push_back(5);

  dprd->BuildTree(rack_size, hosts_size, node_size);
  AddRules(dprd);

  // Insert partition
  // Distribute(int root_id, int partition, int level, int ruleno);
  int partition_size = 1000;
  for (int i = 0; i < partition_size; ++i) {
    dprd->Distribute(0, i, 0, 0);
  }

  int added_weight = 0;

  DoOption(op, node_size, hosts_size, dprd, &added_weight);

  dprd->Migrate();
  // dprd->DumpPartitionNodeMap();
  // dprd->DumpMapInfo();

  int sum_hosts_size = 0;
  int sum_weight = 0;
  for (size_t i = 0; i < hosts_size.size(); ++i) {
    sum_hosts_size += hosts_size[i];
  }
  sum_weight = sum_hosts_size * node_size;
  int pg_num = partition_size * 3;
  PrintRes(dprd, sum_weight, added_weight, pg_num);
  delete dprd;
}

void LoadDumpTest() {
  std::cout<< "LoadDumpTest" << std::endl;
  DprdWrapper* dprd = new DprdWrapper;
  AddRules(dprd);

  int rack_size = 4;
  int node_size = 10;
  std::vector<int> hosts_size;
  hosts_size.push_back(10);
  hosts_size.push_back(10);
  hosts_size.push_back(10);
  hosts_size.push_back(10);

  dprd->BuildTree(rack_size, hosts_size, node_size);
  std::string dump_file = "dump_tree_test.example";
  if (dprd->DumpTree(dump_file)) {
    std::cout << "DumpTree success!" << std::endl;
    std::cout << "File name: " << dump_file << std::endl;
    std::cout << "Tree topology is rack_size: " << rack_size << std::endl;
    std::cout << "                 host per rack: ";
    for (size_t i = 0; i < hosts_size.size(); ++i) {
      std::cout << " " << hosts_size[i];
    }
    std::cout << std::endl;
    std::cout << "                 node_size: "<< node_size << std::endl;
    std::cout << "Total buckets: " << dprd->max_bucket() << std::endl;
  } else {
    std::cout << "DumpTree failed!" << std::endl;
    return;
  }

  delete dprd;

  dprd = new DprdWrapper;
  if (dprd->LoadTree(dump_file)) {
    std::cout<< "LoadTree success!" << std::endl;
    std::cout<< "Total buckets: " << dprd->max_bucket() << std::endl;
    std::cout<< std::endl;
  } else {
    std::cout<< "LoadTree failed!" << std::endl;
  }
  delete dprd;
}

void LoadPartitionTest(int op) {
  std::cout<< "LoadPartitionTest ";
  PrintOption(op);

  DprdWrapper* dprd = new DprdWrapper;
  std::string load_file = "dump_tree_test.example";
  if (!dprd->LoadTree(load_file)) {
    std::cout<< "LoadTree failed!" << std::endl;
    return;
  }
  // Insert partition
  // Distribute(int root_id, int partition, int level, int ruleno);
  int partition_size = 1000;
  for (int i = 0; i < partition_size; ++i) {
    dprd->Distribute(0, i, 0, 0);
  }
  std::map<int, std::vector<std::string> > partition_to_node;
  dprd->BuildPartitionToNodesMap(&partition_to_node);

  delete dprd;

  dprd = new DprdWrapper;
  if (!dprd->LoadTree(load_file)) {
    std::cout<< "LoadTree failed!" << std::endl;
    return;
  }
  dprd->LoadPartition(partition_to_node);

  int added_weight = 0;

  int node_size = 10;
  std::vector<int> hosts_size = {10, 10, 10, 10};
  DoOption(op, node_size, hosts_size, dprd, &added_weight);

  dprd->Migrate();
  // dprd->DumpPartitionNodeMap();
  // dprd->DumpMapInfo();

  int sum_hosts_size = 0;
  int sum_weight = 0;
  for (size_t i = 0; i < hosts_size.size(); ++i) {
    sum_hosts_size += hosts_size[i];
  }
  sum_weight = sum_hosts_size * node_size;
  int pg_num = partition_size * 3;
  PrintRes(dprd, sum_weight, added_weight, pg_num);
  delete dprd;
}

int main() {
  debug_op = false;
  // test initial partition distribute
  BalancedTreeTest(kAddNone);
  BalancedTreeTest(kAddOneNode);
  BalancedTreeTest(kAddOneHost);
  BalancedTreeTest(kAddOneRack);
  BalancedTreeTest(kRemoveOneNode);
  BalancedTreeTest(kRemoveOneHost);
  BalancedTreeTest(kRemoveOneRack);

  UnbalancedTreeTest(kAddNone);
  UnbalancedTreeTest(kAddOneNode);
  UnbalancedTreeTest(kAddOneHost);
  UnbalancedTreeTest(kAddOneRack);
  UnbalancedTreeTest(kRemoveOneNode);
  UnbalancedTreeTest(kRemoveOneHost);
  UnbalancedTreeTest(kRemoveOneRack);

  UnbalancedTreeBadPerformanceTest(kAddNone);
  UnbalancedTreeBadPerformanceTest(kAddOneNode);
  UnbalancedTreeBadPerformanceTest(kAddOneHost);
  UnbalancedTreeBadPerformanceTest(kAddOneRack);
  UnbalancedTreeBadPerformanceTest(kRemoveOneNode);
  UnbalancedTreeBadPerformanceTest(kRemoveOneHost);
  UnbalancedTreeBadPerformanceTest(kRemoveOneRack);

  LoadDumpTest();

  LoadPartitionTest(kAddNone);
  LoadPartitionTest(kAddOneNode);
  LoadPartitionTest(kAddOneHost);
  LoadPartitionTest(kAddOneRack);
  LoadPartitionTest(kRemoveOneNode);
  LoadPartitionTest(kRemoveOneHost);
  LoadPartitionTest(kRemoveOneRack);

  return 0;
}
