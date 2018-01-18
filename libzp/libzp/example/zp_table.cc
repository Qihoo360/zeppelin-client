#include <vector>
#include <string>
#include <map>
#include <memory>

#include "libzp/include/zp_cluster.h"
#include "libzp/include/zp_client.h"
#include "slash/include/slash_status.h"

int main() {
  libzp::Options options;
  options.meta_addr.push_back(libzp::Node("127.0.0.1", 9221));
  options.op_timeout = 1000;
  std::string primary_key("testkey");
  std::string table_name("test");
  std::unique_ptr<libzp::Client> client(new libzp::Client(options, table_name));
  slash::Status s;

  // PutRow, insert new
  printf("PutRow, insert new row\n");
  std::map<std::string, std::string> columns {
    {"col1", "value1"},
    {"col2", "value2"},
    {"col3", "value3"},
  };
  s = client->PutRow(primary_key, columns);
  if (!s.ok()) {
    std::cout << "PutRow Error: " << s.ToString() << std::endl;
    return -1;
  }

  // PutRow, update exist row
  printf("PutRow, update exist row\n");
  std::map<std::string, std::string> columns1 {
    {"col4", "value4"},
    {"col5", "value5"},
  };
  s = client->PutRow(primary_key, columns1);
  if (!s.ok()) {
    std::cout << "PutRow Error: " << s.ToString() << std::endl;
    return -1;
  }

  // GetRow, get all columns
  printf("GetRow, get all columns\n");
  std::vector<std::string> empty_col;
  std::map<std::string, std::string> results;
  s = client->GetRow(primary_key, empty_col, &results);
  if (!s.ok()) {
    std::cout << "GetRow Error: " << s.ToString() << std::endl;
    return -1;
  }
  printf("Get all columns result: \n");
  for (auto& kv : results) {
    printf("key: %s, value: %s\n", kv.first.c_str(), kv.second.c_str());
  }

  // GetRow, get specified columns
  printf("GetRow, get specified columns\n");
  std::vector<std::string> col_to_get{"col1", "col2"};
  results.clear();
  s = client->GetRow(primary_key, col_to_get, &results);
  if (!s.ok()) {
    std::cout << "GetRow Error: " << s.ToString() << std::endl;
    return -1;
  }
  printf("Get specified columns result: \n");
  for (auto& kv : results) {
    printf("key: %s, value: %s\n", kv.first.c_str(), kv.second.c_str());
  }

  // DeleteRow, delete specified columns
  printf("DeleteRow, delete specified columns\n");
  s = client->DeleteRow(primary_key, col_to_get);
  if (!s.ok()) {
    std::cout << "DeleteRow Error: " << s.ToString() << std::endl;
    return -1;
  }
  results.clear();
  s = client->GetRow(primary_key, empty_col, &results);
  if (!s.ok()) {
    std::cout << "GetRow Error: " << s.ToString() << std::endl;
    return -1;
  }
  printf("Delete specified columns result: \n");
  for (auto& kv : results) {
    printf("key: %s, value: %s\n", kv.first.c_str(), kv.second.c_str());
  }

  // DeleteRow, delete all columns
  printf("DeleteRow, delete all columns\n");
  s = client->DeleteRow(primary_key, empty_col);
  if (!s.ok()) {
    std::cout << "DeleteRow Error: " << s.ToString() << std::endl;
    return -1;
  }
  results.clear();
  s = client->GetRow(primary_key, empty_col, &results);
  if (!s.ok()) {
    std::cout << "GetRow Error: " << s.ToString() << std::endl;
    return -1;
  }
  printf("results size: %d\n", results.size());

  return 0;
}
