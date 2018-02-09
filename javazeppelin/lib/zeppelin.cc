// Copyright 2017 Qihoo
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http:// www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include "net_qihoo_zeppelin_Zeppelin.h"

#include <jni.h>
#include <stdlib.h>
#include <stdint.h>

#include <string>

#include "zp_client.h"

#define INVALID_PARAM "Invalid parameter"

static void print_error_message(JNIEnv *env, jobject obj, const std::string& msg);
static libzp::Client* get_client(JNIEnv *env, jobject obj);
static jobject convert_to_java_map(JNIEnv *env, const std::map<std::string, std::string>& values);

JNIEXPORT void JNICALL Java_net_qihoo_zeppelin_Zeppelin_createZeppelin(
    JNIEnv * env, jobject obj, jstring ip, jstring port, jstring table) {
  const char *ip_c_str = env->GetStringUTFChars(ip, NULL);
  if (NULL == ip_c_str) {
    print_error_message(env, obj, INVALID_PARAM);
    return;
  }
  const char *port_c_str = env->GetStringUTFChars(port, NULL);
  if (NULL == port_c_str) {
    env->ReleaseStringUTFChars(ip, ip_c_str);
    print_error_message(env, obj, INVALID_PARAM);
    return;
  }
  const char *table_c_str = env->GetStringUTFChars(table, NULL);
  if (NULL == table_c_str) {
    env->ReleaseStringUTFChars(ip, ip_c_str);
    env->ReleaseStringUTFChars(port, port_c_str); 
    print_error_message(env, obj, INVALID_PARAM);
    return;
  }
  
  std::string ip_str(ip_c_str);
  std::string port_str(port_c_str);
  std::string table_str(table_c_str);
  env->ReleaseStringUTFChars(ip, ip_c_str);
  env->ReleaseStringUTFChars(port, port_c_str); 
  env->ReleaseStringUTFChars(table, table_c_str);
  libzp::Client* client = new libzp::Client(ip_str, atoi(port_str.c_str()), table_str);

  jclass cls = env->GetObjectClass(obj);
  jfieldID fid_ptr = env->GetFieldID(cls, "ptr", "J");
  if (NULL == fid_ptr) {
    print_error_message(env, obj, INVALID_PARAM);
    return;
  }
  jlong ptr = env->GetLongField(obj, fid_ptr);
  ptr = (jlong)client;
  env->SetLongField(obj, fid_ptr, ptr);
}

JNIEXPORT void JNICALL Java_net_qihoo_zeppelin_Zeppelin_removeZeppelin(
    JNIEnv * env, jobject obj) {
  libzp::Client* client = get_client(env, obj);
  if (NULL == client) {
    print_error_message(env, obj, INVALID_PARAM);
    return;
  }
  
  delete client;

  jclass cls = env->GetObjectClass(obj);
  jfieldID fid_ptr = env->GetFieldID(cls, "ptr", "J");
  if (NULL == fid_ptr) {
    print_error_message(env, obj, INVALID_PARAM);
    return;
  }
  jlong ptr = 0;
  env->SetLongField(obj, fid_ptr, ptr);
}

JNIEXPORT jboolean JNICALL Java_net_qihoo_zeppelin_Zeppelin_set(JNIEnv * env,
    jobject obj, jstring key, jstring value) {
  libzp::Client* client = get_client(env, obj);
  if (NULL == client) {
    print_error_message(env, obj, INVALID_PARAM);
    return false;
  }

  const char *key_c_str = env->GetStringUTFChars(key, NULL);
  if (NULL == key_c_str) {
    print_error_message(env, obj, INVALID_PARAM);
    return false;
  }
  const char *value_c_str = env->GetStringUTFChars(value, NULL);
  if (NULL == value_c_str) {
    env->ReleaseStringUTFChars(value, value_c_str);
    print_error_message(env, obj, INVALID_PARAM);
    return false;
  }
  std::string key_str(key_c_str);
  std::string value_str(value_c_str);
  env->ReleaseStringUTFChars(key, key_c_str);
  env->ReleaseStringUTFChars(value, value_c_str);
  
  libzp::Status s = client->Set(key_str, value_str);
  if (!s.ok()) {
    print_error_message(env, obj, s.ToString()); 
    return false;
  }
  return true;
}

JNIEXPORT jstring JNICALL Java_net_qihoo_zeppelin_Zeppelin_get(JNIEnv *env,
    jobject obj, jstring key) {
  libzp::Client* client = get_client(env, obj);
  if (NULL == client) {
    print_error_message(env, obj, INVALID_PARAM);
    return NULL;
  }
  const char *key_c_str = env->GetStringUTFChars(key, NULL);
  if (NULL == key_c_str) {
    print_error_message(env, obj, INVALID_PARAM);
    return NULL;
  }
  
  std::string key_str(key_c_str);
  env->ReleaseStringUTFChars(key, key_c_str);
  std::string value_str;
  libzp::Status s = client->Get(key_str, &value_str);
  if (!s.ok() && !s.IsNotFound()) {
    print_error_message(env, obj, s.ToString()); 
    return NULL;
  }
  return env->NewStringUTF(value_str.c_str()); 
}

JNIEXPORT jobject JNICALL Java_net_qihoo_zeppelin_Zeppelin_mget(JNIEnv *env,
    jobject obj, jobjectArray string_array) {
  libzp::Client* client = get_client(env, obj);
  if (NULL == client) {
    print_error_message(env, obj, INVALID_PARAM);
    return NULL;
  }
  
  std::vector<std::string> keys;
  jsize length = env->GetArrayLength(string_array);
  for (int i = 0; i < length; ++i) {
    jstring key = (jstring)env->GetObjectArrayElement(string_array, i);
    const char *key_c_str = env->GetStringUTFChars(key, NULL);
    std::string key_str(key_c_str);
    env->ReleaseStringUTFChars(key, key_c_str);
    keys.push_back(key_str);
  }
  
  std::map<std::string, std::string> values;
  libzp::Status s = client->Mget(keys, &values);
  if (!s.ok()) {
    print_error_message(env, obj, s.ToString());
    return NULL;
  }
  return convert_to_java_map(env, values);
}

JNIEXPORT jboolean JNICALL Java_net_qihoo_zeppelin_Zeppelin_delete(JNIEnv *env,
    jobject obj, jstring key) {
  libzp::Client* client = get_client(env, obj);
  if (NULL == client) {
    print_error_message(env, obj, INVALID_PARAM);
    return false;
  }
  const char *key_c_str = env->GetStringUTFChars(key, NULL);
  if (NULL == key_c_str) {
    print_error_message(env, obj, INVALID_PARAM);
    return false;
  }
  
  std::string key_str(key_c_str);
  env->ReleaseStringUTFChars(key, key_c_str);
  libzp::Status s = client->Delete(key_str);
  if (!s.ok() && !s.IsNotFound()) {
    print_error_message(env, obj, s.ToString());
    return false;
  }
  return true;
}

static void print_error_message(JNIEnv *env, jobject obj,
    const std::string& msg) {
  jclass jc = env->GetObjectClass(obj);
  jmethodID mid = env->GetStaticMethodID(jc, "exceptionCallback",
      "(Ljava/lang/String;)V");
  if (NULL == mid) {
    return;
  }
  jstring msg_j = env->NewStringUTF(msg.c_str());
  env->CallStaticVoidMethod(jc, mid, msg_j);
  env->DeleteLocalRef(msg_j);
}

static libzp::Client* get_client(JNIEnv *env, jobject obj) {
  jclass cls = env->GetObjectClass(obj);
  jfieldID fid_ptr = env->GetFieldID(cls, "ptr", "J");
  if (NULL == fid_ptr) {
    print_error_message(env, obj, INVALID_PARAM);
    return NULL;
  }
  jlong ptr = env->GetLongField(obj, fid_ptr);
  return (libzp::Client*)ptr; 
}

static jobject convert_to_java_map(JNIEnv *env,
    const std::map<std::string, std::string>& values) {
  if (values.empty()) {
    return NULL;
  }
  jclass cls_treemap = env->FindClass("java/util/TreeMap");
  jmethodID mid_init = env->GetMethodID(cls_treemap, "<init>", "()V");
  if (NULL == mid_init) {
    env->DeleteLocalRef(cls_treemap);
    return NULL;
  }
  jobject obj_treemap = env->NewObject(cls_treemap, mid_init, "");
  jmethodID mid_put = env->GetMethodID(cls_treemap, "put",
      "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
  env->DeleteLocalRef(cls_treemap);
  std::map<std::string, std::string>::const_iterator iter = values.begin();
  for (; iter != values.end(); ++iter) {
    const std::string& key_str = iter->first;
    const std::string& value_str = iter->second;
    jstring key_j = env->NewStringUTF(key_str.c_str());
    jstring value_j = env->NewStringUTF(value_str.c_str());
    env->CallObjectMethod(obj_treemap, mid_put, key_j, value_j);
    env->DeleteLocalRef(key_j);
    env->DeleteLocalRef(value_j);
  }
  return obj_treemap;
}

