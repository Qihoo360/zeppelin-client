/*
 * "Copyright [2016] qihoo"
 */
#ifndef CLIENT_INCLUDE_ZP_CLIENT_C_H_
#define CLIENT_INCLUDE_ZP_CLIENT_C_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct zp_status_t      zp_status_t;
typedef struct zp_option_t      zp_option_t;
typedef struct zp_client_t      zp_client_t;
typedef struct zp_node_t        zp_node_t;
typedef struct zp_node_vec_t    zp_node_vec_t;
typedef struct zp_string_t      zp_string_t;
typedef struct zp_string_vec_t  zp_string_vec_t;

// struct's constructor and destructor
extern int zp_status_ok(const zp_status_t* s);
extern zp_string_t* zp_status_tostring(const zp_status_t* s);
extern void zp_status_destroy(zp_status_t* s);

extern zp_option_t* zp_option_create(zp_node_vec_t* metas, int op_timeout);
extern void zp_option_destroy(zp_option_t* option);

extern zp_client_t* zp_client_create(const zp_option_t* options, const char* tb_name);
extern void zp_client_destroy(zp_client_t* client);

extern zp_node_t* zp_node_create1(const char* ip, int port);
extern zp_node_t* zp_node_create();
extern void zp_node_destroy(zp_node_t* node);
extern char* zp_node_ip(zp_node_t* node);
extern int zp_node_port(zp_node_t* node);

extern zp_node_vec_t* zp_nodevec_create();
extern void zp_nodevec_destroy(zp_node_vec_t* vec);
extern void zp_nodevec_pushback(zp_node_vec_t* nodevec, const zp_node_t* node);
extern zp_node_t* zp_nodevec_popback(zp_node_vec_t* nodevec);

extern zp_string_t* zp_string_create1(const char* data, int length);
extern zp_string_t* zp_string_create();
extern void zp_string_destroy(zp_string_t* str);
extern char* zp_string_data(zp_string_t* str);
extern int zp_string_length(zp_string_t* str);

extern zp_string_vec_t* zp_strvec_create();
extern void zp_strvec_destroy(zp_string_vec_t* vec);
extern void zp_strvec_pushback(zp_string_vec_t* nodevec, zp_string_t* str);
extern zp_string_t* zp_strvec_popback(zp_string_vec_t* strvec);

// Zeppelin client interface
extern zp_status_t* zp_set(
    const zp_client_t* client,
    const zp_string_t* key,
    const zp_string_t* value,
    int ttl);

extern zp_status_t* zp_get(
    const zp_client_t* client,
    const zp_string_t* key,
    zp_string_t* value);

extern zp_status_t* zp_mget(
    const zp_client_t* client,
    zp_string_vec_t* keys,
    zp_string_vec_t* res_keys,
    zp_string_vec_t* res_values);

extern zp_status_t* zp_delete(
    const zp_client_t* client,
    const zp_string_t* key);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // CLIENT_INCLUDE_ZP_CLIENT_H_
