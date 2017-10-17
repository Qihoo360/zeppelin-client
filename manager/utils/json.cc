#include "json.h"

#include <string.h>

namespace mjson {

JsonElement::JsonElement(const JsonElementType t) :
    type(t) {
  memset(reinterpret_cast<void*>(&value), 0, sizeof(value));
}

JsonElement::JsonElement(const JsonElement& json_ele) :
    field(json_ele.field),
    type(json_ele.type) {
  memset(reinterpret_cast<void*>(&value), 0, sizeof(value));
  switch (type) {
  case JsonElementType::kStr:
    value.v_str = new std::string(*json_ele.value.v_str);
    break;
  case JsonElementType::kInt:
    value.v_int = json_ele.value.v_int;
    break;
  case JsonElementType::kJson:
    value.v_json = new Json(*json_ele.value.v_json);
  default:
    break;
  }
}

JsonElement& JsonElement::operator=(const JsonElement& json_ele) {
  switch (this->type) {
  case JsonElementType::kStr:
    delete this->value.v_str;
    break; 
  case JsonElementType::kJson:
    delete this->value.v_json;
    break;
  default:
    /*
     * do nothing
     */
    break;
  }
  memset(&this->value, 0, sizeof(this->value));

  this->field = json_ele.field;
  this->type = json_ele.type; 
  switch (this->type) {
  case JsonElementType::kStr:
    this->value.v_str = new std::string(*json_ele.value.v_str);
    break;
  case JsonElementType::kInt:
    this->value.v_int = json_ele.value.v_int;
    break;
  case JsonElementType::kJson:
    this->value.v_json = new Json(*json_ele.value.v_json);
    break;
  default:
    /*
     * do nothing
     */
    break;
  } 
  return *this; 
}

JsonElement::~JsonElement() {
  switch (type) {
  case JsonElementType::kStr:
    delete value.v_str;
    break;
  case JsonElementType::kJson:
    delete value.v_json;
    break;
  default:
    break;
  }
}

void JsonElement::Clear() {
  field.clear();
  switch(type) {
  case JsonElementType::kStr: 
    delete value.v_str;
    break;
  case JsonElementType::kJson:
    delete value.v_json;
    break;
  default:
    break;
  }
  type = JsonElementType::kEnone;
  memset(reinterpret_cast<void*>(&value), 0, sizeof(value));
}

Json::Json(const JsonType t) :
    type_(t) {
  memset(reinterpret_cast<void*>(&value_), 0, sizeof(value_));
  switch (type_) {
  case JsonType::kSingle:
    value_.s_json = new SingleJson();
    break;
  case JsonType::kArray:
    value_.a_json = new ArrayJson();
    break;
  default:
    break;
  }
}

Json::Json(const Json& json) :
    type_(json.type()) {

  switch (type_) {
  case JsonType::kSingle:
    value_.s_json = new SingleJson(*json.single_json());
    break;
  case JsonType::kArray:
    value_.a_json = new ArrayJson(*json.array_json());
    break;
  default:
    break;
  }
}

Json& Json::operator=(const Json& json) {
  switch (type_) {
  case JsonType::kSingle:
    delete value_.s_json;
    break;
  case JsonType::kArray:
    delete value_.a_json;
    break;
  default:
    break;
  }
  memset(reinterpret_cast<void*>(&value_), 0, sizeof(value_));

  type_ = json.type();
  switch (type_) {
  case JsonType::kSingle:
    value_.s_json = new SingleJson(*json.single_json());
    break;
  case JsonType::kArray:
    value_.a_json = new ArrayJson(*json.array_json());
    break;
  default:
    break;
  }
  return *this;
}

Json::~Json() {
  switch (type_) {
  case JsonType::kSingle:
    delete value_.s_json;
    break;
  case JsonType::kArray:
    delete value_.a_json;
    break;
  default:
    break;
  }
}

int32_t Json::AddJsonElement(const JsonElement& json_ele) {
  if (type_ != JsonType::kSingle) {
    return -1;
  }
  value_.s_json->push_back(json_ele);
  return 0;
}

int32_t Json::AddStr(const std::string& field, const std::string& value) {
  JsonElement ele;
  ele.type = JsonElementType::kStr;
  ele.field = field;
  ele.value.v_str = new std::string(value);
  return AddJsonElement(ele);
}

int32_t Json::AddInt(const std::string& field, const int64_t value) {
  JsonElement ele;
  ele.type = JsonElementType::kInt;
  ele.field = field;
  ele.value.v_int = value;
  return AddJsonElement(ele);
}

int32_t Json::AddJson(const std::string& field, const Json& json) {
	JsonElement ele;
	ele.type = JsonElementType::kJson;
	ele.field = field;
	ele.value.v_json = new Json(json);
	return AddJsonElement(ele);
}

int32_t Json::PushJson(const Json& json) {
  if (type_ != JsonType::kArray) {
    return -1;
  }
  value_.a_json->push_back(json);
  return 0;
}

void Json::Clear() {
  switch (type_) {
  case JsonType::kSingle:
    value_.s_json->clear();
    break;
  case JsonType::kArray:
    value_.a_json->clear();
    break;
  default:
    break;
  }
}

std::string Json::GetHstr(uint32_t indent) const {
  if (type_ == JsonType::kJnone) {
    return "";
  }
  if (type_ == JsonType::kSingle) {
    return GetSingleJsonHstr(*value_.s_json, indent);
  }
  return GetArrayJsonHstr(*value_.a_json, indent);
}

std::string Json::GetArrayJsonHstr(const ArrayJson& a_json, uint32_t indent) const {
  std::string indent_str(indent, TAB_C);

  std::string str = indent_str + JsonInterpreter::LEFT_MID_BRACE_C;
  for (ArrayJson::const_iterator iter = a_json.begin();
      iter != a_json.end();
      ++iter) {
    str += JsonInterpreter::LF_C;
    str += iter->GetHstr(indent + 1);
    str += JsonInterpreter::COMMA_C;
  }
  if (str.back() == JsonInterpreter::COMMA_C) {
    str.erase(str.size() - 1);
  }
  str += JsonInterpreter::LF_C + indent_str + JsonInterpreter::RIGHT_MID_BRACE_C;
  return str;
}

std::string Json::GetSingleJsonHstr(const SingleJson& s_json, uint32_t indent) const {
  std::string indent_str(indent, TAB_C);
  std::string str = indent_str + JsonInterpreter::LEFT_LARGE_BRACE_C, tmp_str;
  for (SingleJson::const_iterator iter = s_json.begin();
      iter != s_json.end();
      ++iter) {
    str += JsonInterpreter::LF_C;
    str += indent_str + TAB_C;
    str += JsonInterpreter::QUOTE_C + iter->field + JsonInterpreter::QUOTE_C + " : ";
    switch (iter->type) {
    case JsonElementType::kStr:
      str.append(JsonInterpreter::QUOTE_C + *iter->value.v_str + JsonInterpreter::QUOTE_C);
      break;
    case JsonElementType::kInt:
      str.append(std::to_string(iter->value.v_int));
      break;
    case JsonElementType::kJson:
      tmp_str = iter->value.v_json->GetHstr(indent + 1);
      str += tmp_str.substr(indent+1); /* tmp must contains at least indent+1 chars, and the first indent+1 chars are all '\t' */
      break;
    default:
      str.append(2, JsonInterpreter::QUOTE_C);
      break;
    }
    str += JsonInterpreter::COMMA_C;
  }
  if (str.back() == JsonInterpreter::COMMA_C) {
    str.erase(str.size() - 1);
  }
  str += JsonInterpreter::LF_C + indent_str + JsonInterpreter::RIGHT_LARGE_BRACE_C;
  return str;
}

std::string Json::Encode() const {
  if (type_ == JsonType::kSingle) {
    return EncodeSingleJson();
  } else if (type_ == JsonType::kArray) {
    return EncodeArrayJson();
  }
  return "";
}

int32_t Json::GetJsonValue(const std::string& field,
                           Json* j_v) const {
  if (!j_v) {
    return -1;
  }
  const JsonElement* p_ele = GetEle(field, JsonElementType::kJson);
  if (!p_ele) {
    return -1;
  }
  *j_v = *p_ele->value.v_json;
  return 0;
}

int32_t Json::GetStrValue(const std::string& field,
                          std::string* s_v) const {
  if (!s_v) {
    return -1;
  }
  const JsonElement* p_ele = GetEle(field, JsonElementType::kStr);
  if (!p_ele) {
    return -1;
  }
  *s_v = *p_ele->value.v_str;
  return 0;
}

int32_t Json::GetIntValue(const std::string& field,
                          int64_t* i_v) const {
  if(!i_v) {
    return -1;
  }
  const JsonElement* p_ele = GetEle(field, JsonElementType::kInt);
  if (!p_ele) {
    return -1;
  }
  *i_v = p_ele->value.v_int;
  return 0;
}

const JsonElement* Json::GetEle(const std::string& field,
                       JsonElementType ele_type) const {
  if (type_ != JsonType::kSingle) {
    return NULL;
  }
  for (const JsonElement& ele : *value_.s_json) {
    if (ele.field != field) {
      continue;
    }
    if (ele.type != ele_type) {
      return NULL;
    }
    return &ele; 
  }
  return NULL;
}

std::string Json::EncodeSingleJson() const {
  std::string str;
  for (const JsonElement& ele : *value_.s_json) {
    if ((ele.type == JsonElementType::kEnone) && ele.field.empty()) {
      continue;
    }
    str += JsonInterpreter::QUOTE_C + ele.field + JsonInterpreter::QUOTE_C;
    str += JsonInterpreter::SEPERATE_C;
    switch (ele.type) {
    case JsonElementType::kStr:
      str += JsonInterpreter::QUOTE_C + *ele.value.v_str + JsonInterpreter::QUOTE_C;
      break;
    case JsonElementType::kInt:
      str += std::to_string(ele.value.v_int);
      break;
    case JsonElementType::kJson:
      str += ele.value.v_json->Encode();
      break;
    default:
      /*
       * type is none ,but field is not empty, such case let the value is a empty string
       */
      str.append(2, JsonInterpreter::QUOTE_C);
    }
    str += JsonInterpreter::COMMA_C; 
  }
  if (str.back() == JsonInterpreter::COMMA_C) {
    str.erase(str.size()-1);
  }
  return JsonInterpreter::LEFT_LARGE_BRACE_C + str + JsonInterpreter::RIGHT_LARGE_BRACE_C;
}

std::string Json::EncodeArrayJson() const {
  std::string str;
  for (const Json& json : *value_.a_json) {
    str += json.Encode();
    str += JsonInterpreter::COMMA_C;
  }
  if (str.back() == JsonInterpreter::COMMA_C) {
    str.erase(str.size()-1);
  }
  return JsonInterpreter::LEFT_MID_BRACE_C + str + JsonInterpreter::RIGHT_MID_BRACE_C;
}

int32_t JsonInterpreter::ValidCheck(const std::string& str) {
  int32_t mid_bra_nu = 0, lar_bra_nu = 0;
  if (str.empty()) {
    return false;
  }
  if ((str.front() != LEFT_LARGE_BRACE_C || str.back() != RIGHT_LARGE_BRACE_C)
      && (str.front() != LEFT_MID_BRACE_C || str.back() != RIGHT_MID_BRACE_C)) {
    return false;
  }
  std::string::const_iterator iter = str.begin(); 
  while (iter != str.end()) {
    switch (*iter) {
    case LEFT_LARGE_BRACE_C:
      ++lar_bra_nu;
      break;
    case RIGHT_LARGE_BRACE_C:
      --lar_bra_nu;
      break;
    case LEFT_MID_BRACE_C:
      ++mid_bra_nu;
      break;
    case RIGHT_MID_BRACE_C:
      --mid_bra_nu;
      break;
    default:
      /*
       * do nothing
       */
      break;
    }
    if (mid_bra_nu < 0
        || lar_bra_nu < 0) {
      return -1;
    }
    ++iter;
  }
  if (!mid_bra_nu && !lar_bra_nu) {
    return 0;
  }
  return -1;
}

int32_t JsonInterpreter::Decode(const std::string& str, Json* json) {
  if (ValidCheck(str) == -1) {
    return -1;
  }
  Json* tmp = NULL;
  std::string::const_iterator iter = str.begin();

  if (*iter == LEFT_LARGE_BRACE_C) {
    tmp = DecodeJson(str, iter);
  } else if (*iter == LEFT_MID_BRACE_C) {
    tmp = DecodeArrayJson(str, iter);
  }
  if (!tmp) {
    return -1;
  }
  if (++iter != str.end()) {
    delete tmp;
    return -1;
  }
  *json = *tmp;
  return 0;
}

Json* JsonInterpreter::DecodeJson(const std::string& str,
                                  std::string::const_iterator& iter) {
  bool st_k = false, ed_k = false, st_v = false, ed_v =false;
  bool in_quote = false, has_sep = false;
  
  ++iter;
  Json* json = new Json(JsonType::kSingle);

  JsonElement json_ele;
  std::string int_str;
  for (; iter != str.end(); ++iter) {
    if (*iter == SPACE_C && !in_quote) {
      continue;
    }

    if ((!st_k && !ed_k && !st_v && !ed_v && *iter != QUOTE_C)
        || (ed_k && !has_sep && *iter != SEPERATE_C)
        || (ed_v && *iter != COMMA_C && *iter != RIGHT_LARGE_BRACE_C)) {
      delete json;
      return NULL;
    }

    if ((*iter == COMMA_C || *iter == RIGHT_LARGE_BRACE_C)
        && has_sep
        && !in_quote) {
      if (*iter == COMMA_C && !st_k && !ed_k && !st_v && !ed_v) { /* ommit the "{... ,, ...} case" */
        continue;
      }
      if (json_ele.type == JsonElementType::kInt) {
        /*
         * not consider the non-numeric num is invalid, e.g. 12mdf will result in 12mdf
         */
        json_ele.value.v_int = atoll(int_str.c_str()); /* if the value is null, will convert to 0 */
        st_v = false;
        ed_v = true;
      } else if (json_ele.type == JsonElementType::kEnone
          && !json_ele.field.empty()) {
        json_ele.type =JsonElementType::kStr;
        json_ele.value.v_str = new std::string(2, QUOTE_C);
        ed_k = false;
        ed_v = true;
      }
      if (ed_v) {
        json->AddJsonElement(json_ele);
        json_ele.Clear();
        ed_v = false; /* when coming here should the ed_v is true */
        has_sep = false;
        int_str.clear();
      }

      if (*iter == RIGHT_LARGE_BRACE_C) { /* current json is over */
        return json;
      }
      continue;
    }

    if (*iter == QUOTE_C && *(iter-1) != '\\') {
      in_quote = !in_quote;
      if (!has_sep && !st_k && !ed_k) {
        st_k = true;
      } else if (!has_sep && st_k && !ed_k) {
        st_k = false;
        ed_k = true;
      } else if (has_sep && !st_v && !ed_v) {
        if (json_ele.type != JsonElementType::kEnone) { /* is type has been fixed */
          delete json;
          return NULL;
        }
        ed_k = false;
        st_v = true;
        json_ele.type = JsonElementType::kStr;
        json_ele.value.v_str = new std::string; /* because type is kEnone before, so don's need to care the pointer */
      } else if (has_sep && st_v && !ed_v) {
        st_v = false;
        ed_v = true;
      } else {
        delete json; 
        return NULL;
      } 
      continue;
    }

    if ((*iter == LEFT_LARGE_BRACE_C || *iter == LEFT_MID_BRACE_C)
        && has_sep && !st_v && !ed_v) {
      json_ele.type = JsonElementType::kJson;
      if (*iter == LEFT_MID_BRACE_C) {
        json_ele.value.v_json = DecodeArrayJson(str, iter);
      } else {
        json_ele.value.v_json = DecodeJson(str, iter);
      }
      if (!json_ele.value.v_json) {
        delete json;
        return NULL;
      }
      ed_k = false;
      ed_v = true;
      continue;
    }
    
    if (has_sep && !st_v && !ed_v) { /* now value is not start with quote, should be a integer */
      json_ele.type = JsonElementType::kInt;
      ed_k = false;
      st_v = true;
      int_str.append(1, *iter);
      continue;
    }

    if (ed_k && !has_sep && *iter == SEPERATE_C) {
      has_sep = true;
      continue;
    }

    if (st_k && !ed_k) {
      json_ele.field.append(1, *iter);
      continue;
    }
    if (json_ele.type == JsonElementType::kStr && st_v && !ed_v) {
      json_ele.value.v_str->append(1, *iter);
      continue;
    }
    if (json_ele.type == JsonElementType::kInt && st_v && !ed_v) {
      int_str.append(1, *iter);
      continue;
    }

    delete json;
    return NULL;
  }
  return NULL;
}

Json* JsonInterpreter::DecodeArrayJson(const std::string& str,
                      std::string::const_iterator& iter) {
  Json *a_json = new Json(JsonType::kArray), *ele_json = NULL;
  ++iter; 
  for (; iter != str.end(); ++iter) {
    if (*iter == COMMA_C) {
      continue;
    }
    if (*iter == RIGHT_MID_BRACE_C) {
      return a_json;
    }
    if (*iter == RIGHT_MID_BRACE_C) {
      ele_json = DecodeArrayJson(str, iter);
    } else if (*iter == LEFT_LARGE_BRACE_C) {
      ele_json = DecodeJson(str, iter);
      if (!ele_json) {
        delete a_json;
        return NULL;
      }
      a_json->PushJson(*ele_json);
    } else {
      return NULL;
    }
  }
  delete a_json;
  return NULL;
}

int32_t JsonInterpreter::Encode(const Json& json, std::string* str) {
  *str = json.Encode();
  return 0;
}

} /* namespace mjson */
