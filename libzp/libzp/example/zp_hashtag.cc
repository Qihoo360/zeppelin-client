#include <vector>
#include <string>
#include <map>

#include "libzp/include/zp_cluster.h"
#include "libzp/include/zp_client.h"
#include "slash/include/slash_status.h"

int main() {
  libzp::Options options;
  options.meta_addr.push_back(libzp::Node("127.0.0.1", 9221));
  options.op_timeout = 100000;
  libzp::Cluster* cluster = new libzp::Cluster(options);
  slash::Status s;

  std::string table_name("test");

  std::string hash_tag(libzp::kLBrace + "hashkey" + libzp::kRBrace);
  libzp::Cluster::Batch batch(hash_tag);
  batch.Write("testkey1", "testvalue1");
  batch.Write("testkey2", "testvalue2");
  batch.Write("testkey3", "testvalue3");

  printf("WriteBatch\n");
  s = cluster->WriteBatch(table_name, batch);
  if (!s.ok()) {
    std::cout << "WriteBatch Error: " << s.ToString() << std::endl;
    return -1;
  }

  printf("Write by hash tag\n");
  std::string key1("example1{hashkey}key1");
  std::string key2("example{hashkey}key2");
  std::string key3("exa{hashkey}keykk3");
  cluster->Set(table_name, key1, key1 + "value");
  cluster->Set(table_name, key2, key2 + "value");
  cluster->Set(table_name, key3, key3 + "value");

  printf("ListbyTag\n");
  std::map<std::string, std::string> results;
  s = cluster->ListbyTag(table_name, hash_tag, &results);
  if (!s.ok()) {
    std::cout << "ListbyTag Error: " << s.ToString() << std::endl;
    return -1;
  }
  for (auto& kv : results) {
    printf("key: %s, value: %s\n", kv.first.c_str(), kv.second.c_str());
  }

  printf("DeletebyTag\n");
  s = cluster->DeletebyTag(table_name, hash_tag);
  if (!s.ok()) {
    std::cout << "DeletebyTag Error: " << s.ToString() << std::endl;
    return -1;
  }

  delete cluster;
  return 0;
}
