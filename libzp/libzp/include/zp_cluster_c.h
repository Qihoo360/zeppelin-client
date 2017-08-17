/*
 * "Copyright [2016] qihoo"
 */
#ifndef CLIENT_INCLUDE_ZP_CLUSTER_C_H_
#define CLIENT_INCLUDE_ZP_CLUSTER_C_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct zp_status_t      zp_status_t;
typedef struct zp_option_t      zp_option_t;
typedef struct zp_cluster_t     zp_cluster_t;
typedef struct { char ip[32]; int port; } zp_node_t;
typedef struct zp_node_vec_t    zp_node_vec_t;
typedef struct zp_string_vec_t  zp_string_vec_t;

extern zp_option_t* zp_option_create(
    zp_node_vec_t* metas,
    int op_timeout);

extern void zp_option_destroy(zp_option_t* option);

extern zp_cluster_t* zp_cluster_create(
    const zp_option_t* options);

extern void zp_cluster_destroy(zp_cluster_t* cluster);

extern zp_status_t* zp_create_table(
    zp_cluster_t* cluster,
    const char* table_name,
    int partition_num);

extern zp_status_t* zp_drop_table(
    zp_cluster_t* cluster,
    const char* table_name);

extern zp_status_t* zp_pull(
    zp_cluster_t* cluster,
    const char* table);

// statistical cmd
extern zp_status_t* zp_list_table(
    zp_cluster_t* cluster,
    zp_string_vec_t* tables);

extern zp_status_t* zp_list_meta(
    zp_cluster_t* cluster,
    zp_node_t* master,
    zp_node_vec_t* slaves);

extern zp_status_t* zp_metastatus(
    zp_cluster_t* cluster,
    char* meta_status,
    int buf_len);

extern zp_status_t* zp_list_node(
    zp_cluster_t* cluster,
    zp_node_vec_t* nodes,
    zp_string_vec_t* status);

extern zp_node_t* zp_node_create(const char* ip, int port);
extern void zp_node_destroy(zp_node_t* node);

extern int zp_status_ok(const zp_status_t* s);
extern void zp_status_tostring(const zp_status_t* s, char* buf, int buf_len);
extern void zp_status_destroy(zp_status_t* s);

extern zp_node_vec_t* zp_nodevec_create();
extern void zp_nodevec_destroy(zp_node_vec_t* vec);
extern void zp_nodevec_pushback(zp_node_vec_t* nodevec, const zp_node_t* node);
extern zp_node_t* zp_nodevec_popback(zp_node_vec_t* nodevec);

extern zp_string_vec_t* zp_strvec_create();
extern void zp_strvec_destroy(zp_string_vec_t* vec);
extern int zp_strvec_popback(zp_string_vec_t* strvec, char* buf, int buf_len);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // CLIENT_INCLUDE_ZP_CLUSTER_H_
