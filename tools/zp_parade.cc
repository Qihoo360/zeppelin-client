#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "slash/include/slash_status.h"
#include "libzp/include/zp_cluster.h"


void usage() {
  std::cout << "usage:\n"
            << "      zp_parade host port\n";
}

int main(int argc, char* argv[]) {
  if (argc != 3) {
    usage();
    return -1;
  }

  std::cout << "Create client" << std::endl;
  libzp::Client* client = new libzp::Client(argv[1], atoi(argv[2]), "parade");
  
  std::cout << "Connect cluster" << std::endl;
  slash::Status s = client->Connect();
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return -1;
  }

  uint64_t start_time = 0, end_time = 0;
  std::cout << "Begin Set 1000 item[10B value] ..." << std::endl;
  for(int i = 0; i < 100000; i++) {
    std::string key = "smallkey_" + std::to_string(i);
    s = client->Set(key, "small value");
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      return -1;
    }
  }
  std::cout << "Success Set 1000 item[10B value] ..." << std::endl;
  delete client;
}
