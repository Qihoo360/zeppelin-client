#include <stdio.h>
#include <vector>

#include "libzp/include/zp_cluster.h"

#define NONE                 "\e[0m"
#define BLACK                "\e[0;30m"
#define L_BLACK              "\e[1;30m"
#define RED                  "\e[0;31m"
#define L_RED                "\e[1;31m"
#define GREEN                "\e[0;32m"
#define L_GREEN              "\e[1;32m"
#define BROWN                "\e[0;33m"
#define YELLOW               "\e[1;33m"
#define BLUE                 "\e[0;34m"
#define L_BLUE               "\e[1;34m"
#define PURPLE               "\e[0;35m"
#define L_PURPLE             "\e[1;35m"
#define CYAN                 "\e[0;36m"
#define L_CYAN               "\e[1;36m"
#define GRAY                 "\e[0;37m"
#define WHITE                "\e[1;37m"

#define BOLD                 "\e[1m"
#define UNDERLINE            "\e[4m"
#define BLINK                "\e[5m"
#define REVERSE              "\e[7m"
#define HIDE                 "\e[8m"
#define CLEAR                "\e[2J"
#define CLRLINE              "\r\e[K" //or "\e[1K\r"

libzp::Cluster* cluster;
void ListNode() {
  std::vector<libzp::Node> nodes;
  std::vector<std::string> status;
  slash::Status s = cluster->ListNode(&nodes, &status);
  if (!s.ok()) {
    printf(RED "Failed: %s" NONE "\n", s.ToString().c_str());
    return;
  }
  if (nodes.size() == 0) {
    printf(RED "No Nodes" NONE "\n");
    return;
  }
  printf(BLUE REVERSE "Nodes Count: %8lu\n" NONE, nodes.size());
  for (size_t i = 0; i < nodes.size();) {
    printf(REVERSE "%15s:%5d" NONE" ", nodes[i].ip.c_str(), nodes[i].port);
    if (i + 1 < nodes.size()) {
      printf(REVERSE "%15s:%5d" NONE" ", nodes[i + 1].ip.c_str(), nodes[i + 1].port);
    }
    if (i + 2 < nodes.size()) {
      printf(REVERSE "%15s:%5d" NONE" ", nodes[i + 2].ip.c_str(), nodes[i + 2].port);
    }

    printf("\n");

    int num = 0;
    while (num < 3 && i < nodes.size()) {
      if (status[i] == "up") {
        printf(GREEN "%21s" NONE, "UP"); 
      } else {
        printf(RED "%21s" NONE, "DOWN"); 
      }
      i++;
      num++;
    }
    printf("\n");
  }
}

int main(int argc, char* argv[]) {
  libzp::Options option;
  const char* ip = "127.0.0.1";
  int port = 9221;
  option.meta_addr.push_back(libzp::Node(ip, port));
  option.op_timeout = 5000;
  // cluster handle cluster operation
  cluster = new libzp::Cluster(option);
  ListNode();

//  printf("\e[7m%21s\e[0m \e[7m%21s\e[0m \e[7m%21s\e[0m\n", "127.0.0.1:8123", "127.0.0.0:8124", "127.0.0.1:8125");
//  printf("\e[0;32m%21s\e[0m \e[0;31m%21s\e[0m \e[0;32m%21s\e[0m\n", "UP", "DOWN", "UP");
//  printf("\e[7m%21s\e[0m \e[7m%21s\e[0m \e[7m%21s\e[0m\n", "127.0.0.1:8126", "127.0.0.0:8127", "127.0.0.1:8128");
//  printf("\e[0;32m%21s\e[0m \e[0;32m%21s\e[0m \e[0;31m%21s\e[0m\n", "UP", "UP", "DOWN");
//  printf("\e[7m%21s\e[0m \e[7m%21s\e[0m \e[7m%21s\e[0m\n", "127.0.0.1:8129", "127.0.0.0:8130", "127.0.0.1:8131");
//  printf("\e[0;32m%21s\e[0m \e[0;32m%21s\e[0m \e[0;32m%21s\e[0m\n", "UP", "UP", "UP");
  return 0;
}
