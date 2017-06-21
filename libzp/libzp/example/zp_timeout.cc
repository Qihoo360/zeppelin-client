#include <unistd.h>

#include <string>
#include <iostream>

#include "slash/include/slash_status.h"
#include "libzp/include/zp_client.h"


void usage() {
  std::cout << "usage:\n"
            << "      zp_timeout host port table\n";
}

int main(int argc, char* argv[]) {
  if (argc != 4) {
    usage();
    return -1;
  }

  // client handle io operation
  std::cout << "create client" << std::endl;
  libzp::Options opt;
  opt.op_timeout = 100;
  opt.meta_addr.push_back(libzp::Node(argv[1], atoi(argv[2])));

  libzp::Client* client = new libzp::Client(opt, argv[3]);
  
  std::string key("test_timeout");
  libzp::Status s = client->Set(key, "value_timeout");
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  } else {
    std::cout << "set ok."<< std::endl;
  }

  std::string value;
  s = client->Get(key, &value);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  } else {
    std::cout << "get ok, value: " << value << std::endl;
  }

  s = client->Delete(key);
  if (!s.ok()) {
    std::cout << s.ToString() << std::endl;
  } else {
    std::cout << "delete ok." << std::endl;
  }

  delete client;
  return 1;
}
