# 基于flink的数据流处理系统 
      基于 apache flink1.10


## 模块划分
- fiflow-ui       web页面 sql的执行 和 任务的创建 管理
- fiflow-web      与前端对应的后台
- fiflow-api      fiflow 对外提供的操作api 
- fiflow-core     flink 操作的封装
- fiflow-runtime  提交任务到flink  local、standalone、yarn 以及与flink集群的交互

## 如何使用 
在输入框中输入 help 给出提示信息;

![如何使用](./docs/fiflow.png)

- demo mysql    jdbc 示例
- demo kafka    kafka 示例 


## 如何运行
- 后端为基于spring boot的程序，运行 FiflowWebMain 
- 前端为vue程序 在fiflow-ui目录下 见该 README 

## todo 
- flink 集群管理 
- 任务管理 
- 数据源管理

## Thanks 
- flink 
- zeppelin 
- spring boot 
- vue 
- element-ui 
