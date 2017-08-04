/*
  +----------------------------------------------------------------------+
  | PHP Version 7                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2017 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author:                                                              |
  +----------------------------------------------------------------------+
*/

/* $Id$ */
extern "C"
{
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_zeppelin.h"
}

#include <vector>
#include <string>

#include "libzp/include/zp_client.h"
#include "slash/include/slash_status.h"

/* If you declare any globals in php_zeppelin.h uncomment this:
ZEND_DECLARE_MODULE_GLOBALS(zeppelin)
*/

/* True global resources - no need for thread safety here */
static int le_zeppelin;

/* Every user-visible function in PHP should document itself in the source */
/* {{{ proto string confirm_zeppelin_compiled(string arg)
   Return a string to confirm that the module is compiled in */
PHP_FUNCTION(confirm_zeppelin_compiled)
{
	char *arg = NULL;
	size_t arg_len, len;
	zend_string *strg;

	if (zend_parse_parameters(ZEND_NUM_ARGS(), "s", &arg, &arg_len) == FAILURE) {
		return;
	}

	strg = strpprintf(0, "Congratulations! You have successfully modified ext/%.78s/config.m4. Module %.78s is now compiled into PHP.", "zeppelin", arg);

	RETURN_STR(strg);
}
/* }}} */
/* The previous line is meant for vim and emacs, so it can correctly fold and
   unfold functions in source code. See the corresponding marks just before
   function definition, where the functions purpose is also documented. Please
   follow this convention for the convenience of others editing your code.
*/

zend_object_handlers zeppelin_object_handlers;
struct zeppelin_object
{
    zend_object std;
	libzp::Client *zp;
};

void zeppelin_free_storage(zend_object *object TSRMLS_DC)
{
    zeppelin_object *obj = (zeppelin_object *)object;
    delete obj->zp;
    efree(obj);
}

static inline struct zeppelin_object * php_custom_object_fetch_object(zend_object *obj) {
      return (struct qconfzk_object *)((char *)obj - XtOffsetOf(struct qconfzk_object , std));
}

#define Z_ZEPPLIN_OBJ_P(zv) php_custom_object_fetch_object(Z_OBJ_P(zv));

zend_object * zeppelin_create_handler(zend_class_entry *ce TSRMLS_DC) {
     // Allocate sizeof(custom) + sizeof(properties table requirements)
	struct zeppelin_object *intern = (struct zeppelin_object *)ecalloc(
		1, sizeof(struct qconfzk_object) + zend_object_properties_size(ce));
	zend_object_std_init(&intern->std, ce TSRMLS_CC);
	zeppelin_object_handlers.offset = XtOffsetOf(struct zeppelin_object, std);
	zeppelin_object_handlers.free_obj = zeppelin_free_storage;

     intern->std.handlers = &zeppelin_object_handlers;

     return &intern->std;
}

zend_class_entry *zeppelin_ce;

/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(zeppelin)
{
	/* If you have INI entries, uncomment these lines
	REGISTER_INI_ENTRIES();
	*/

    zend_class_entry ce;
    INIT_CLASS_ENTRY(ce, "Zeppelin", zeppelin_functions);
    zeppelin_ce = zend_register_internal_class(&ce TSRMLS_CC);
    zeppelin_ce->create_object = zeppelin_create_handler;
    memcpy(&zeppelin_object_handlers,
    zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    zeppelin_object_handlers.clone_obj = NULL;

	return SUCCESS;
}
/* }}} */

PHP_METHOD(Zeppelin, __construct)
{
	char *ip = NULL;
	int ip_len = 0;
	int port = 0;
	char *table = NULL;
	int table_len = 0;
	zval *self;
	zval *object;
	int id;
	int timeout = -1;
	libzp::Options options;
	libzp::Client *zp = NULL;
	char * addrs = NULL;
	int addrs_len = 0;

	if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Oss|l",
									 &object, zeppelin_client_ext, &addrs,
									 &addrs_len, &table,
									 &table_len, &timeout) == SUCCESS) {
		std::vector<std::string> addr_v;
		char *addr = strtok(addrs, ";");
		while(addr != NULL) {
			addr_v.push_back(std::string(addr));
			addr = strtok(NULL,";");
		}

		std::string ip;
		int port = 0;
		// Split address into ip and port
		for (size_t i = 0; i < addr_v.size(); i++) {
			if(!slash::ParseIpPortString(addr_v[i], ip, port)) {
				RETURN_FALSE;
			}
			libzp::Node node(ip, port);
			options.meta_addr.push_back(node);
		}
		// Timeout
		if (timeout != -1) {
			options.op_timeout = timeout;
		}
		// Connect
		zp = new libzp::Client(options, std::string(table, table_len));
	} else {
		RETURN_FALSE;
	}

	zeppelin_object *obj = Z_ZEPPELIN_OBJ_P(getThis());
	obj->zp = zp;
}

PHP_METHOD(Zeppelin, __destruct)
{
    // TODO
}

PHP_METHOD(Zeppelin, set)
{
    char *key   = NULL;
    char *value = NULL;
    int key_len   = 0;
    int value_len = 0;
    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss",
            &key, &key_len, &value, &value_len) == FAILURE) {
        RETVAL_FALSE;
    }

	libzp::Client *zp = NULL;
    zepplin_object *obj = Z_ZEPPLIN_OBJ_P(getThis());
    zp = obj->zp;
	if (zp != NULL) {
		slash::Status s = zp->Set(std::string(key, key_len), std::string(value, value_len));
		if (s.ok()) {
			RETVAL_TRUE;
		}
	}

	RETURN_FALSE;
}

PHP_METHOD(Zeppelin, get)
{
    struct timeval tv;
    char *key   = NULL;
    int key_len   = 0;
    zval *object;
    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
            &key, &key_len) == FAILURE) {
		RETURN_FALSE;
    }

	libzp::Client *zp = NULL;
    zepplin_object *obj = Z_ZEPPLIN_OBJ_P(getThis());
    zp = obj->zp;
	if (zp != NULL) {
		std::string val;
		libzp::Status s = zp->Get(std::string(key, key_len), &val);
		if (s.ok()) {
			RETVAL_STRINGL((char *)val.data(), val.size());
		}
	}
	RETVAL_FALSE;
}

PHP_METHOD(Zeppelin, mget)
{
    zval *keys = NULL;
    zval *object;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "a",
            &keys) == FAILURE) {
        RETURN_FALSE;
    }

	libzp::Client *zp = NULL;
    zepplin_object *obj = Z_ZEPPLIN_OBJ_P(getThis());
    zp = obj->zp;
	if (zp != NULL) {
		std::vector<std::string> vkeys;
		if (Z_TYPE_P(keys) == IS_ARRAY) {
			zval **arrval;
			HashPosition pos;
			zend_hash_internal_pointer_reset_ex(Z_ARRVAL_P(keys), &pos);
			while (zend_hash_get_current_data_ex(Z_ARRVAL_P(keys), (void **)&arrval, &pos) == SUCCESS) {
				if (Z_TYPE_PP(arrval) == IS_STRING) {
					vkeys.push_back(std::string(Z_STRVAL_PP(arrval), Z_STRLEN_PP(arrval)));
				}
				zend_hash_move_forward_ex(Z_ARRVAL_P(keys), &pos);
			}
		}

		std::map<std::string, std::string> result;
		libzp::Status s = zp->Mget(vkeys, &result);
		if (s.ok()) {
			if (return_value_used) {
				array_init(return_value);
				std::map<std::string, std::string>::iterator r_it = result.begin();
				for (; r_it != result.end(); ++r_it) {
					add_assoc_stringl_ex(return_value, (char *) r_it->first.data(), r_it->first.size(),
										 (char *) r_it->second.data(), r_it->second.size(), 1);
				}
			}
		} else {
			RETVAL_FALSE;
		}
	} else {
		RETVAL_FALSE;
	}
}

PHP_METHOD(Zeppelin, delete)
{
    char *key   = NULL;
    int key_len   = 0;
    zval *object;
    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
            &key, &key_len) == FAILURE) {
		RETURN_FALSE;
    }

	libzp::Client *zp = NULL;
    zepplin_object *obj = Z_ZEPPLIN_OBJ_P(getThis());
    zp = obj->zp;
	if (zp != NULL) {

		libzp::Status s = zp->Delete(std::string(key, key_len));
		if (s.ok()) {
			RETVAL_TRUE;
		}
	}
	RETVAL_FALSE;
}

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(zeppelin)
{
	/* uncomment this line if you have INI entries
	UNREGISTER_INI_ENTRIES();
	*/
	return SUCCESS;
}
/* }}} */

/* Remove if there's nothing to do at request start */
/* {{{ PHP_RINIT_FUNCTION
 */
PHP_RINIT_FUNCTION(zeppelin)
{
#if defined(COMPILE_DL_ZEPPELIN) && defined(ZTS)
	ZEND_TSRMLS_CACHE_UPDATE();
#endif
	return SUCCESS;
}
/* }}} */

/* Remove if there's nothing to do at request end */
/* {{{ PHP_RSHUTDOWN_FUNCTION
 */
PHP_RSHUTDOWN_FUNCTION(zeppelin)
{
	return SUCCESS;
}
/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(zeppelin)
{
	php_info_print_table_start();
	php_info_print_table_header(2, "zeppelin support", "enabled");
	php_info_print_table_end();

	/* Remove comments if you have entries in php.ini
	DISPLAY_INI_ENTRIES();
	*/
}
/* }}} */

/* {{{ zeppelin_functions[]
 *
 * Every user visible function must have an entry in zeppelin_functions[].
 */
const zend_function_entry zeppelin_functions[] = {
	PHP_FE(confirm_zeppelin_compiled,	NULL)		/* For testing, remove later. */
    PHP_ME(Zeppelin, __construct,		NULL, ZEND_ACC_CTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Zeppelin, __destruct,		NULL, ZEND_ACC_DTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Zeppelin, set,				NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Zeppelin, get,				NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Zeppelin, delete,			NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Zeppelin, mget,				NULL, ZEND_ACC_PUBLIC)
	PHP_FE_END	/* Must be the last line in zeppelin_functions[] */
};
/* }}} */

/* {{{ zeppelin_module_entry
 */
zend_module_entry zeppelin_module_entry = {
	STANDARD_MODULE_HEADER,
	"zeppelin",
	zeppelin_functions,
	PHP_MINIT(zeppelin),
	PHP_MSHUTDOWN(zeppelin),
	PHP_RINIT(zeppelin),		/* Replace with NULL if there's nothing to do at request start */
	PHP_RSHUTDOWN(zeppelin),	/* Replace with NULL if there's nothing to do at request end */
	PHP_MINFO(zeppelin),
	PHP_ZEPPELIN_VERSION,
	STANDARD_MODULE_PROPERTIES
};
/* }}} */

#ifdef COMPILE_DL_ZEPPELIN
#ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE()
#endif
ZEND_GET_MODULE(zeppelin)
#endif

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
