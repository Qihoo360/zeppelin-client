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

/* If you declare any globals in php_zeppelin.h uncomment this:*/
ZEND_DECLARE_MODULE_GLOBALS(zeppelin)

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

struct ClientInfo {
	ClientInfo() : timeout(-1), zp_cli(NULL) {}
	~ClientInfo() { delete zp_cli; }
	std::string addrs;
	std::string table;
	int timeout;
	libzp::Client* zp_cli;
};

static bool need_reconnect(ClientInfo* info, std::string new_addrs,
						   std::string new_table, int new_timeout) {
	if (info->addrs != new_addrs ||
		info->table != new_table ||
		info->timeout != new_timeout) {
		return true;
	}
	return false;
}

zend_class_entry *zeppelin_ce;

PHP_METHOD(Zeppelin, __construct)
{
	char *ip = NULL;
	int ip_len = 0;
	int port = 0;
	char *table = NULL;
	int table_len = 0;
	int id;
	int timeout = -1;
	libzp::Options options;
	char * addrs = NULL;
	int addrs_len = 0;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss|l",
									 &addrs, &addrs_len, &table, &table_len,
									 &timeout) == SUCCESS) {
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
		// Connect TODO (gaodq) thread safe ?
		ClientInfo* info = reinterpret_cast<ClientInfo*>(ZEPPELIN_G(g_zp_cli));

		if (info->zp_cli == NULL ||
			need_reconnect(info, addrs, table, timeout)) {
			delete info->zp_cli;
			info->zp_cli = new libzp::Client(options, std::string(table, table_len));
		}

		RETVAL_TRUE;
	} else {
		RETURN_FALSE;
	}
}

PHP_METHOD(Zeppelin, __destruct)
{
    // Do nothing
}

PHP_METHOD(Zeppelin, set)
{
    char *key   = NULL;
    char *value = NULL;
    int key_len   = 0;
    int value_len = 0;
	int ttl = -1;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "ss|l",
            &key, &key_len, &value, &value_len, &ttl) == FAILURE) {
        RETVAL_FALSE;
    }

	ClientInfo* info = reinterpret_cast<ClientInfo*>(ZEPPELIN_G(g_zp_cli));
	libzp::Client *zp = info->zp_cli;
	if (zp != NULL) {
		slash::Status s = zp->Set(std::string(key, key_len), std::string(value, value_len), ttl);
		if (s.ok()) {
			RETVAL_TRUE; // Won't return
		} else {
			RETURN_FALSE;
		}
	} else {
		RETURN_FALSE;
	}
}

PHP_METHOD(Zeppelin, get)
{
    struct timeval tv;
    char *key   = NULL;
    int key_len   = 0;
    zval *object;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
            &key, &key_len) == FAILURE) {
		RETURN_FALSE;
    }

	ClientInfo* info = reinterpret_cast<ClientInfo*>(ZEPPELIN_G(g_zp_cli));
	libzp::Client *zp = info->zp_cli;
	if (zp != NULL) {
		std::string val;
		libzp::Status s = zp->Get(std::string(key, key_len), &val);
		if (s.ok()) {
			RETVAL_STRINGL((char *)val.data(), val.size());
		} else {
			RETVAL_FALSE;
		}
	} else {
		RETVAL_FALSE;
	}
}

PHP_METHOD(Zeppelin, mget)
{
    zval *keys = NULL;
    zval *object;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "a", &keys) == FAILURE) {
        RETURN_FALSE;
    }

	ClientInfo* info = reinterpret_cast<ClientInfo*>(ZEPPELIN_G(g_zp_cli));
	libzp::Client *zp = info->zp_cli;
	if (zp != NULL) {
		// keys value
		std::vector<std::string> vkeys;
		HashTable *keys_arr = Z_ARRVAL_P(keys);
		zval* val;
		ZEND_HASH_FOREACH_VAL(keys_arr, val) {
			vkeys.push_back(std::string(Z_STR_P(val)->val, Z_STR_P(val)->len));
		} ZEND_HASH_FOREACH_END();

		std::map<std::string, std::string> result;
		libzp::Status s = zp->Mget(vkeys, &result);
		if (s.ok()) {
			array_init(return_value);
			std::map<std::string, std::string>::iterator r_it = result.begin();
			for (; r_it != result.end(); ++r_it) {
				add_assoc_stringl_ex(return_value, (char *) r_it->first.data(), r_it->first.size(),
									 (char *) r_it->second.data(), r_it->second.size());
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
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s",
            &key, &key_len) == FAILURE) {
		RETURN_FALSE;
    }

	ClientInfo* info = reinterpret_cast<ClientInfo*>(ZEPPELIN_G(g_zp_cli));
	libzp::Client *zp = info->zp_cli;
	if (zp != NULL) {
		libzp::Status s = zp->Delete(std::string(key, key_len));
		if (s.ok()) {
			RETVAL_TRUE;
		} else {
			RETVAL_FALSE;
		}
	} else {
		RETVAL_FALSE;
	}
}

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(zeppelin)
{
	/* uncomment this line if you have INI entries
	UNREGISTER_INI_ENTRIES();
	*/
	delete reinterpret_cast<ClientInfo*>(ZEPPELIN_G(g_zp_cli));
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

	ZEPPELIN_G(g_zp_cli) = reinterpret_cast<void*>(new ClientInfo());

	return SUCCESS;
}
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
