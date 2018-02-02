/*
 * "Copyright [2016] qihoo"
 */
#include <unistd.h>

#include <string>
#include <iostream>
#include <memory>

#include "slash/include/slash_status.h"

#include "libzp/include/zp_client.h"


int main(int argc, char* argv[]) {
  // client handle io operation
  std::cout << "create client" << std::endl;
  libzp::Options opts;
  opts.op_timeout = 0;
  opts.meta_addr.push_back(libzp::Node("127.0.0.1", 9222));
  std::string table_name("test");
  std::unique_ptr<libzp::Client> client(new libzp::Client(opts, table_name));
  libzp::Status s;
  
  std::vector<std::pair<std::string, std::string>> kvs {
    {"mkey1", "mvalue1"},
    {"mkey2", "mvalue2"},
    {"mkey3", "mvalue3"},
    {"mkey4", "mvalue4"},
    {"mkey5", "mvalue5"},
    {"mkey6", "mvalue6"},
    {"mkey7", "mvalue7"},
    {"mkey8", "mvalue8"},
    {"mkey9", "mvalue9"},
    {"mkey10", "mvalue10"},
  };
  s = client->Mset(kvs);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  }

  return 0;
}
