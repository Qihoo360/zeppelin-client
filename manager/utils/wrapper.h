#ifndef UTILS_DPRD_WRAPPER_H_
#define UTILS_DPRD_WRAPPER_H_

#include "dprd_type.h"

#define REPLICA_NUM 3

bool CompareFunction(std::pair<double,int> a, std::pair<double,int> b);


class DprdWrapper {
 public:
  explicit DprdWrapper();
  ~DprdWrapper();
  void CreateMap();
  bool AddBucket(int parent, int type, int id, int weight = 0, 
    const std::string& ip = "", int port = 0);
  bool AddBucket(int type, int id, const std::string& name, int weight = 0);
  bool RemoveBucket(int id);
  bool AddRule(int id);
  bool AddStep(int rule_id, int step_id, int op, int arg1, int arg2);
  bool Distribute(int root_id, int partition, int level, int ruleno, 
      int old_belonging = -1);
  void Migrate();
  void BuildTree(int rack_size, const std::vector<int>& hosts_size, 
      int partition_size);
  bool LoadTree(const std::string& file);
  bool DumpTree(const std::string& file);
  void DumpPartitionNodeMap();
  void DumpMapInfo();
    void ResetCounter() {
    changed_partition_counter_ = 0;
  }
  int changed_partition_counter() {
    return changed_partition_counter_;
  }
  int max_bucket();
 private:
  bool RemoveBucketPartition(DprdBucket* target, int partition);
  bool InsertBucketPartition(DprdBucket* target, int partition);
  void ChooseFirstN(const std::vector<DprdBucket*>& input, 
      std::vector<DprdBucket*>& output, int n);
  int FindBucketLevel(const DprdBucket* bucket);
  bool CheckMoveAllReplicas(const DprdBucket* bucket);
  void BalanceChildren(const DprdBucket* bucket);
  void BuildCandidate(const DprdBucket* root, std::vector<int>& candidate);
  bool ChoosePartitionToRemove(const DprdBucket* root, const DprdBucket* dst, 
      int& partition);
  bool ChooseNodeToRemove(DprdBucket* root, const DprdBucket* dst, 
      int& partition, DprdBucket*& choosen_node);
  double CalcAverageFactor(const std::vector<int>& buckets);
  std::vector<double> CalcFactor(const std::vector<int>& partitions);
  std::vector<double> CalcBaseFactor(const std::vector<int>& partitions);
  bool Balanced(const std::vector<int>& children, std::vector<int>& src, 
      std::vector<int>& dst);
  void LevelOrderTraversalBalance(const DprdBucket* root);
  void UpdateWeightToTop(int id, int weight);
  bool LevelOrderTraversalRemove(const DprdBucket* root);
  void RemoveHeadTailWhiteSpaces(std::string& buf);
  bool AddBucketFromFile(std::ifstream& in, const std::string& buf);
  bool AddNodeFromFile(std::ifstream& in, const std::string& buf);
  bool AddStepFromFile(const std::string& rule);
  void DumpRule(std::ofstream& ofs);
  void DumpNode(std::ofstream& ofs);
  void DumpBucket(std::ofstream& ofs);

  std::map<int, std::vector<int> > partitions_to_nodes_;
  int changed_partition_counter_;
  DprdMap* map_;
};


#endif
