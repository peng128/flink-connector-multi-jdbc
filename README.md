# FLINK MULTI JDBC CONNECTOR

提供分库分表的jdbc链接方法，目前只提供批模式。

## 依赖
flink 1.13.2

## 使用方式
### 参数
|参数名称|是否必填|说明|
|-------|-------|----|
|connector|是|填写multi-jdbc|
|url |是 |jdbc的链接串，用分号分割。jdbc:mysql://${connection1};jdbc:mysql://${connection2} |
|table-name |是 |可用正则匹配 |
|schema-name |是 |可用正则匹配 |
|username |否 |用户名 |
|password |否 |密码 |
|driver |否 | |
|scan.partition.column |是 |分片的字段，最好是自增id主键 |
|scan.batch.size |否 |scan.batch.size 和 scan.partition.num必填写一个。每个batch的大小，每个表会多一个select count（1）的查询去获取表的数据量进行计算 |
|scan.partition.num |否 |scan.batch.size 和 scan.partition.num必填写一个。每张表分多少个片。 |

### 例子
```sql
CREATE TABLE test_multi_jdbc(
`id` BIGINT COMMENT '',
`dbctime` TIMESTAMP(3) COMMENT '',
`dbutime` TIMESTAMP(3) COMMENT '',
`extension` STRING COMMENT '') COMMENT 'null'
WITH (
	'connector' = 'multi-jdbc',
	'table-name' = 'test_table.*',
	'schema-name' = 'test_database.*',
	'username' = '${username}',
	'password' = '${password}',
	'scan.partition.column' = 'id',
	'scan.batch.size' = '100000',
	'url' = 'jdbc:mysql://${connection1};jdbc:mysql://${connection2}'
)
```

### 设计
基于flip-27设计的flink接口，实现的分库分表jdbc的链接器。
enumerator(在jobmanager内)负责
1. 把符合条件的库名找出来。
2. 把符合表名称的表明拿出来。
3. 取这张表的最大值以及最小值。
4. 获取一个批次的步长。
   1. 若填写了batch size。则通过select count 获取表行数计算，在使用 批次大小/总行数 * （max id - min id）计算出步长。
   2. 拖填写了partition num。 则使用 （max id - min id） / 分片数量 计算出步长。
5. 根据步长来生成对应的sql（select * from xxxx where column between xxxx and xxxx）