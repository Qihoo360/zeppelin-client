#ifndef ZP_MANAGER_HELP
#define ZP_MANAGER_HELP

#include <string>

struct CommandHelp {
  std::string name;
  std::string params;
  int params_num;
  std::string summary;
};

CommandHelp commandHelp[] = {
  { "SET",
    "table key value [ttl]",
    3,
    "set key"},

  { "GET",
    "table key",
    2,
    "get key's value"},
  
  { "MGET",
    "key [key ...]",
    2,
    "Get the values of all the given keys"},

  { "Delete",
    "table key",
    2,
    "delete key"},

  { "CREATE",
    "table partition",
    2,
    "create table"},

  { "PULL",
    "table",
    1,
    "pull table info"},

  { "SETMASTER",
    "table partit p",
    4,
    "set a partition's master"},

  { "ADDSLAVE",
    "table partition ip port",
    4,
    "add master for partition"},

  { "REMOVESLAVE",
    "table partition ip port",
    4,
    "remove master for partition"},

  { "LISTTABLE",
    "",
    0,
    "list all tables"},

  { "DROPTABLE",
    "table",
    1,
    "drop one table"},

  { "FLUSHTABLE",
    "table",
    1,
    "clean one table"},

  { "LISTNODE",
    "",
    0,
    "list all data nodes"},

  { "LISTMETA",
    "",
    0,
    "list all meta nodes"},

  { "METASTATUS",
    "",
    0,
    "list meta internal details"},

  { "QPS",
    "table",
    1,
    "get qps for a table"},

  { "REPLSTATE",
    "table ip port",
    3,
    "check replication state"},
  
  { "NODESTATE",
    "ip port",
    2,
    "check node server state"},

  { "SPACE",
    "table",
    1,
    "get space info for a table"},

  { "DUMP",
    "table",
    1,
    "get space info for a table"},

  { "LOCATE",
    "table key",
    2,
    "locate a key, find corresponding nodes"},

  { "EXIT",
    "",
    0,
    "exit the zp_manager"}
};

#endif
