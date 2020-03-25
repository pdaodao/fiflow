# flink 系统相关

## flink 安装环境说明
flink-1.9 安装在bgydashuju 的 datanode01节点上 


https://blog.csdn.net/lvwenyuan_1/article/details/102938784


1. 安装  prometheus 存储指标时序数据

启动 
./prometheus --config.file=prometheus.yml --web.listen-address=:9070 &


验证prometheus是否正常启动：
netstat -lntp | grep prometheus

查看端口 lsof -i tcp:9070

netstat -apn | grep -E '9091|3000|9090|9100'



2. 安装 pushgateway

nohup ./pushgateway --web.listen-address=:9071 &

flink 配置 
metrics.reporter.promgateway.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter
metrics.reporter.promgateway.host: 0.0.0.0
metrics.reporter.promgateway.port: 9071
metrics.reporter.promgateway.jobName: flinkjob
metrics.reporter.promgateway.randomJobNameSuffix: false
metrics.reporter.promgateway.deleteOnShutdown: true


ui 
prometheus
http://10.162.12.126:9070/

pushgateway
http://10.162.12.126:9071/ 

flink
http://10.162.12.126:8081/#/overview