zeppelin的管理端命令行工具zp_manager，提供对集群的数据访问，建表，迁移，切主，状态查询等功能。
复杂功能如扩容，缩容，迁移，由基本命令setmaster, addslave, removeslave 组合完成

#### 使用
```
./zp_manager meta_ip meta_port
```

#### 建表
```
> create $table_name $paritition_count
```
例：
```
> create test 16 # 创建名为test的Table，其中分配16个partition
```

#### 查看元信息
```
> pull $table_name
```
例：
```
> pull test
reset table:test
OK
current table info:
epoch:4
  name: test
  partition: 3
    partition: 0    master: xxx.xxx.xxx.xxx : 6669
    partition: 1    master: xxx.xxx.xxx.xxx : 6666
    partition: 2    master: xxx.xxx.xxx.xxx : 6667
```

#### 查看Node状态
```
> listnode
```
例：
```
>listnode
xxx.xxx.xxx.xxx:6667 up
xxx.xxx.xxx.xxx:6666 up
xxx.xxx.xxx.xxx:6668 up
xxx.xxx.xxx.xxx:6669 up
OK
```

#### 查看Meta信息
```
> listmeta
```
例：
```
>listmeta
master:xxx.xxx.xxx.xxx 15223
slave:
xxx.xxx.xxx.xxx:15221
xxx.xxx.xxx.xxx:15222
OK
```

#### 查看Table
```
> listtable
```
例：
```
>listtable
test
OK
```
 
#### 查看指定key的映射信息
```
> locate $table_name $key_name
```
例：
```
> locate test wk
partition_id: 0
master: xxx.xxx.xxx.xxx:6669
slave: xxx.xxx.xxx.xxx:6666
slave: xxx.xxx.xxx.xxx:6667
```

#### 切主
```
> setmaster $table_name $patition_num $ip $port
```
例：
```
> setmaster test 0 xxx.xxx.xxx.xxx 6669
OK
```

#### 加从
```
> addslave $table_num $parititon_num $ip $port
```
例：
```
> addslave test 0 xxx.xxx.xxx.xxx 6669
OK
```

#### 删从
```
> removeslave $table_num $parititon_num $ip $port
```
例：
```
> removeslave test 0 xxx.xxx.xxx.xxx 6669
OK
```

#### 查看容量
```
> space $table_num
```
例：
```
> space test
node: xxx.xxx.xxx.xxx 6667
  used:255321586 bytes
  remain:107791286272 bytes
```

#### 查看QPS
```
> qps $table_num
```
例：
```
> space test
qps:0
total query:6093572
```

#### 查看某个节点的binlog位置
```
> offset $table_num $node_ip  $node_port
```
例：
```
> offset test xxx.xxx.xxx.xxx 6667
partition:0
  filenum:0
  offset:77067506
partition:1
  filenum:0
  offset:77200811
partition:2
  filenum:0
  offset:77016777
```

