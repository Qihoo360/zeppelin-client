/*
 * "Copyright [2016] qihoo"
 */

#include "wrapper.h"

#include <math.h> 
#include <iostream>
#include <string>

extern bool debug_op;

enum TestOp{
  ADD_NONE,
  ADD_ONE_NODE,
  ADD_ONE_HOST,
  ADD_ONE_RACK,
  REMOVE_ONE_NODE,
  REMOVE_ONE_HOST,
  REMOVE_ONE_RACK
};

void PrintOption(int op) {
  switch(op) {
    case ADD_NONE:
      std::cout<< "Basic Partition Initial Distribution" << std::endl;
      break;
    case ADD_ONE_NODE:
      std::cout<< "Add one Node"<< std::endl;
      break;
    case ADD_ONE_HOST:
      std::cout<< "Add one Host"<< std::endl;
      break;
    case ADD_ONE_RACK:
      std::cout<< "Add one Rack"<< std::endl;
      break;
    case REMOVE_ONE_NODE:
      std::cout<< "Remove one Node" << std::endl;
      break;
    case REMOVE_ONE_HOST:
      std::cout<< "Remove one Host" << std::endl;
      break;
    case REMOVE_ONE_RACK:
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
    static_cast<double>(dprd->changed_partition_counter()) / pg_num << std::endl;
  if (added_weight < 0) {
    std::cout<< "Theoretical Moving rate: " <<
      static_cast<double>(added_weight) / static_cast<double>(sum_weight)
      << std::endl;
  }
  else {
    std::cout << "Theoretical Moving rate: " << 
      static_cast<double>(added_weight) / static_cast<double>(added_weight + sum_weight) 
      << std::endl;

  }
  std::cout << std::endl;
}

void CheckValidRes(DprdWrapper* dprd, int sum_weight, int added_weight, int pg_num, int op) {
  double act_moving_rate = static_cast<double>(dprd->changed_partition_counter()) / pg_num;
  double tho_moving_rate = 0;
  if (added_weight < 0) {
    tho_moving_rate = static_cast<double>(added_weight) / static_cast<double>(sum_weight);
  }
  else {
    tho_moving_rate = static_cast<double>(added_weight) / static_cast<double>(added_weight + sum_weight);
  }
  if (fabs(fabs(act_moving_rate) - fabs(tho_moving_rate)) > fabs(tho_moving_rate)) {
    std::cout<< "Over limit rate too much" << std::endl;
    PrintOption(op);
    std::cout<< "Changed partitions: " << dprd->changed_partition_counter()<< std::endl;
    std::cout << "Moving rate: " << act_moving_rate << std::endl;
    std::cout<< "Theoretical Moving rate: " << tho_moving_rate << std::endl;

  }
}

void AddRules(DprdWrapper* dprd) {
  dprd->AddRule(0);
  dprd->AddStep(0, 0, DPRD_RULE_TAKE, 0, 0);
  dprd->AddStep(0, 1, DPRD_RULE_CHOOSE_FIRSTN, 3, 0);
  dprd->AddStep(0, 2, DPRD_RULE_CHOOSE_FIRSTN, 1, 0);
  dprd->AddStep(0, 3, DPRD_RULE_CHOOSE_FIRSTN, 1, 0);
  dprd->AddStep(0, 4, DPRD_RULE_EMIT, 0, 0);
}

void AddOneNode(DprdWrapper* dprd, int parent, int& id, int weight) {
  dprd->AddBucket(parent, BUCKET_TYPE_NODE, id++, weight); 
}

void AddOneHost(DprdWrapper* dprd, int host_parent, int node_size, int& total_buckets) {
  int node_parent = total_buckets;
  dprd->AddBucket(host_parent, BUCKET_TYPE_HOST, total_buckets++);
  for (int i = 1; i <= node_size; ++i) {
    dprd->AddBucket(node_parent, BUCKET_TYPE_NODE, total_buckets++, 1);
  } 
}

void AddOneRack(DprdWrapper* dprd, int host_size, int node_size, int& total_buckets) {
  int rack_id = total_buckets;
  //rack's parent is 0 
  dprd->AddBucket(0, BUCKET_TYPE_RACK, total_buckets++);
  for (int host = 1; host <= host_size; ++host) {
    int host_id = total_buckets;
    dprd->AddBucket(rack_id, BUCKET_TYPE_HOST, total_buckets++);
    for (int node = 1; node <= node_size; ++node) {
      dprd->AddBucket(host_id, BUCKET_TYPE_NODE, total_buckets++, 1);
    }
  }
}

void DoOption(int op, DprdWrapper* dprd, int& total_buckets, int& added_weight,
  int node_size, const std::vector<int> hosts_size) {
   switch(op) {
    case ADD_ONE_NODE:
      AddOneNode(dprd, -2, total_buckets, 1);
      added_weight = 1;
      break;
    case ADD_ONE_HOST:
      AddOneHost(dprd, -1, node_size, total_buckets);
      added_weight = node_size;
      break;
    case ADD_ONE_RACK:
      AddOneRack(dprd, 10, 10, total_buckets); 
      added_weight = 10 * 10;  
      break;
    case REMOVE_ONE_NODE:
      //id 3 is a node
      dprd->RemoveBucket(-3);
      added_weight = -1;
      break;
    case REMOVE_ONE_HOST:
      //id 2 is a host
      dprd->RemoveBucket(-2);
      added_weight = -1 * node_size;
      break;
    case REMOVE_ONE_RACK:
      //id 1 is a rack
      dprd->RemoveBucket(-1);
      added_weight = -1 * hosts_size[0] * node_size;
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
  dprd->CreateMap();

  int rack_size = hosts_size.size();
  int node_size = 10;

  dprd->BuildTree(rack_size, hosts_size, node_size);
  AddRules(dprd); 
  
  int total_buckets = dprd->max_bucket();

  // Insert partition
  // Distribute(int root_id, int partition, int level, int ruleno);
  int partition_size = 1000;
  for (int i = 0; i < partition_size; ++i) {
    dprd->Distribute(0, i, 0, 0);
  }
  
  int added_weight = 0;
 
  DoOption(op, dprd, total_buckets, added_weight, node_size, hosts_size);
  
  dprd->Migrate();
  //dprd->DumpPartitionNodeMap();
  //dprd->DumpMapInfo();
  
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
  dprd->CreateMap();

  int rack_size = 4;
  int node_size = 10;
  std::vector<int> hosts_size;
  hosts_size.push_back(12);
  hosts_size.push_back(12);
  hosts_size.push_back(12);
  hosts_size.push_back(12);

  dprd->BuildTree(rack_size, hosts_size, node_size);
  AddRules(dprd); 
  
  int total_buckets = dprd->max_bucket();
  
  // Insert partition
  //Distribute(int root_id, int partition, int level, int ruleno);
  int partition_size = 1000;
  for (int i = 0; i < partition_size; ++i) {
    dprd->Distribute(0, i, 0, 0);
  }
  
  int added_weight = 0;
 
  DoOption(op, dprd, total_buckets, added_weight, node_size, hosts_size);
  
  dprd->Migrate();
  //dprd->DumpPartitionNodeMap();
  //dprd->DumpMapInfo();
  
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
  dprd->CreateMap();

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
 
  int total_buckets = dprd->max_bucket();

  // Insert partition
  //Distribute(int root_id, int partition, int level, int ruleno);
  int partition_size = 1000;
  for (int i = 0; i < partition_size; ++i) {
    dprd->Distribute(0, i, 0, 0);
  }
  
  int added_weight = 0;
  
  DoOption(op, dprd, total_buckets, added_weight, node_size, hosts_size);
 
  dprd->Migrate();
  //dprd->DumpPartitionNodeMap();
  //dprd->DumpMapInfo();
  
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
  dprd->CreateMap();

  int rack_size = 4;
  int node_size = 10;
  std::vector<int> hosts_size;
  hosts_size.push_back(20);
  hosts_size.push_back(15);
  hosts_size.push_back(10);
  hosts_size.push_back(5);

  dprd->BuildTree(rack_size, hosts_size, node_size);
  AddRules(dprd); 
 int total_buckets = dprd->max_bucket();
 
  // Insert partition
  //Distribute(int root_id, int partition, int level, int ruleno);
  int partition_size = 1000;
  for (int i = 0; i < partition_size; ++i) {
    dprd->Distribute(0, i, 0, 0);
  }
  
  int added_weight = 0;
   
  DoOption(op, dprd, total_buckets, added_weight, node_size, hosts_size);
  
  dprd->Migrate();
  //dprd->DumpPartitionNodeMap();
  //dprd->DumpMapInfo();
 
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
  dprd->CreateMap();
  AddRules(dprd);

  int rack_size = 5;
  int node_size = 10;
  std::vector<int> hosts_size;
  hosts_size.push_back(20);
  hosts_size.push_back(15);
  hosts_size.push_back(10);
  hosts_size.push_back(10);
  hosts_size.push_back(15);

  dprd->BuildTree(rack_size, hosts_size, node_size);
  std::string dump_file = "dump_tree_test.example";
  if (dprd->DumpTree(dump_file)) {
    std::cout<< "DumpTree success!" << std::endl;
    std::cout<< "File name: " << dump_file << std::endl; 
    std::cout<< "Tree topology is rack_size: " << rack_size << std::endl;
    std::cout<< "                 host per rack: ";
    for (size_t i = 0; i < hosts_size.size(); ++i) {
      std::cout<< " "<<hosts_size[i] ;
    }
    std::cout<< std::endl;
    std::cout<< "                 node_size: "<< node_size << std::endl;
    std::cout<< "Total buckets: " << dprd->max_bucket() << std::endl; 
  }
  else {
    std::cout<< "DumpTree failed!" << std::endl;
    return;
  }

  delete dprd;

  dprd = new DprdWrapper;
  dprd->CreateMap();
  AddRules(dprd);
  if (dprd->LoadTree(dump_file)) {
    std::cout<< "LoadTree success!" << std::endl;
    std::cout<< "Total buckets: " << dprd->max_bucket() << std::endl;
  }
  else {
    std::cout<< "LoadTree failed!" << std::endl; 
  }
}

int main() {
  debug_op = false;
  // test initial partition distribute 
  BalancedTreeTest(ADD_NONE);
  BalancedTreeTest(ADD_ONE_NODE);
  BalancedTreeTest(ADD_ONE_HOST);
  BalancedTreeTest(ADD_ONE_RACK);
  BalancedTreeTest(REMOVE_ONE_NODE);
  BalancedTreeTest(REMOVE_ONE_HOST);
  BalancedTreeTest(REMOVE_ONE_RACK);

  UnbalancedTreeTest(ADD_NONE);
  UnbalancedTreeTest(ADD_ONE_NODE);
  UnbalancedTreeTest(ADD_ONE_HOST);
  UnbalancedTreeTest(ADD_ONE_RACK);
  UnbalancedTreeTest(REMOVE_ONE_NODE);
  UnbalancedTreeTest(REMOVE_ONE_HOST);
  UnbalancedTreeTest(REMOVE_ONE_RACK);

  UnbalancedTreeBadPerformanceTest(ADD_NONE);
  UnbalancedTreeBadPerformanceTest(ADD_ONE_NODE);
  UnbalancedTreeBadPerformanceTest(ADD_ONE_HOST);
  UnbalancedTreeBadPerformanceTest(ADD_ONE_RACK);
  UnbalancedTreeBadPerformanceTest(REMOVE_ONE_NODE);
  UnbalancedTreeBadPerformanceTest(REMOVE_ONE_HOST);
  UnbalancedTreeBadPerformanceTest(REMOVE_ONE_RACK);

  LoadDumpTest();
  return 0;
}
