# kubernetes
Python采集K8S：
1、需手动添加k8s采集的Python库；
2、该文件中会调用多个内部一套系统的接口，获取K8S的配置信息数据；
3、该采集原理是通过提供k8s的集群名称、可读权限token、url，master节点IP/端口（需与采集机器开通网络权限），调用Python的第三方库的原生接口，来获取信息；
4、可根据采集到的信息，自行修改。
