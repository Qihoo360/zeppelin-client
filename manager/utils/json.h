#ifndef MJSON_H
#define MJSON_H

#include <string>
#include <vector>

namespace mjson {

enum JsonElementType {
  kEnone = 0,
  kStr, /* value is a string */
  kInt, /* value is a integer */
  kJson, /* value is json */
};

enum JsonType {
  kJnone = 0,
  kSingle,
  kArray
};

class Json;

struct JsonElement {
  JsonElement(const JsonElementType t = JsonElementType::kEnone);
  JsonElement(const JsonElement& json_ele);
  JsonElement& operator=(const JsonElement& json_ele);
  virtual ~JsonElement();

  void Clear();

  std::string field;
  JsonElementType type;
  union {
    std::string *v_str;
    int64_t v_int;
    Json* v_json;
  } value;
};

typedef std::vector<JsonElement> SingleJson;
typedef std::vector<Json> ArrayJson;

class Json {
  static const char TAB_C = '\t';
public:
  Json(const JsonType t = JsonType::kJnone);
  Json(const Json& json);
  Json& operator=(const Json& json);
  virtual ~Json();

	/*
	 * for single json
	 */
  int32_t AddJsonElement(const JsonElement& json_ele);
  int32_t AddStr(const std::string& field, const std::string& value);
  int32_t AddInt(const std::string& field, const int64_t value);
	int32_t AddJson(const std::string& field, const Json& json);
  /*
	 * for json array
	 */
  int32_t PushJson(const Json& json);

  void Clear();
  std::string GetHstr(uint32_t indent = 0) const;
  std::string Encode() const; 
  int32_t GetJsonValue(const std::string& field, Json* j_v) const;
  int32_t GetStrValue(const std::string& field, std::string* s_v) const;
  int32_t GetIntValue(const std::string& field, int64_t* i_v) const;

  JsonType type() const {
    return type_;
  }
  const SingleJson* single_json() const {
    return value_.s_json; 
  }
  const ArrayJson* array_json() const {
    return value_.a_json;
  }

private:
  std::string GetArrayJsonHstr(const ArrayJson& a_json, uint32_t indent = 0) const;
  std::string GetSingleJsonHstr(const SingleJson& s_json, uint32_t indent = 0) const;

  std::string EncodeArrayJson() const;
  std::string EncodeSingleJson() const;

  const JsonElement* GetEle(const std::string& field, JsonElementType ele_type) const;

  JsonType type_;
  union {
    SingleJson* s_json;
    ArrayJson* a_json;
  } value_;
};

class JsonInterpreter {
public: 
  const static char LEFT_LARGE_BRACE_C = '{';
  const static char RIGHT_LARGE_BRACE_C = '}';
  const static char LEFT_MID_BRACE_C = '[';
  const static char RIGHT_MID_BRACE_C = ']';
  const static char SPACE_C = ' ';
  const static char QUOTE_C = '\"';
  const static char SEPERATE_C = ':';
  const static char COMMA_C = ',';
  const static char LF_C = '\n';
 
  int32_t Decode(const std::string& str, Json* json);
  int32_t Encode(const Json& json, std::string* str);

private:
  Json* DecodeArrayJson(const std::string& str,
                        std::string::const_iterator& iter);
  Json* DecodeJson(const std::string& str,
                   std::string::const_iterator& iter);

  int32_t ValidCheck(const std::string& str);
};

} /* namespace mjson */
#endif
