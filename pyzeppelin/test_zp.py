#!/usr/bin/env python
# -*- coding: utf-8 -*-

# test_zp.py
import pyzeppelin
#                               ip        port  table_name
b = pyzeppelin.create_client("127.0.0.1", 9221, "test")

test_key   = "key"
test_value = "value"

test_key1   = "key1"
test_value1 = "value1"
(s, msg) = pyzeppelin.set(b, test_key, test_value)
if s != 0:
    print msg
    (s, msg) = pyzeppelin.remove_client(b)

(s, msg) = pyzeppelin.set(b, test_key1, test_value1)
if s != 0:
    print msg
    (s, msg) = pyzeppelin.remove_client(b)

(s, msg) = pyzeppelin.get(b, test_key)
if s == 0 or s== 1: # s==1 msg ==None; the key not found
    result = msg
    print "get value:", result
else:
    print msg
    (s, msg) = pyzeppelin.remove_client(b)

(s, msg) = pyzeppelin.mget(b, [test_key, test_key1])
if (s == 0):
    print msg
else:
    print msg
    (s, msg) = pyzeppelin.remove_client(b)

(s, msg) = pyzeppelin.delete(b, "key")
if s != 0:
    print msg
    (s, msg) = pyzeppelin.remove_client(b)
