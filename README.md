UID History Events Grepper
==================

追踪某类特性用户的历史事件
-----------------------------------

### 流程
* 根据某Filter过滤, 获取到相应的uid, 写入本地文件(或者hdfs)
* 去重uid
* 生成MR任务, 筛选对应uid的历史事件