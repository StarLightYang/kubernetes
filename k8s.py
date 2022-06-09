#! /usr/local/easyops/python/bin/python
# -*- coding: utf-8 -*-
import time
from kubernetes import client, config
# import kubernetes.client
from kubernetes.client.rest import ApiException
from kubernetes.client import api_client
from kubernetes.client.apis import core_v1_api
from kubernetes.client.apis import apps_v1_api
from kubernetes.client.apis import networking_v1beta1_api
from pprint import pprint
from requests.packages import urllib3
import yaml
import os
import sys
import codecs
import json
import datetime
# import tzutc
# from tzutc import get_localzone
import requests
import logging
import urllib
import re
import collections
import hashlib
import hmac

reload(sys)
sys.setdefaultencoding('utf-8')

# CMDB_IP = '10.177.96.195'
# EASYOPS_ORG = 3055

# 如果是本地就是True，放到easyops上改为False
is_local = False

if is_local:
    CMDB_IP = EASYOPS_CMDB_HOST
    EASYOPS_ORG = EASYOPS_ORG
else:
    CMDB_IP = EASYOPS_CMDB_HOST
    EASYOPS_BASE_PATH = "/usr/local/easyops"
    sys.path.insert(0, os.path.join(EASYOPS_BASE_PATH, "agent/plugins"))

from utils.http_util import *
from utils.log_config import load_logger  # 内置的log sdk

load_logger()

CMDB_HEADERS = {
    'host': 'cmdb.easyops-only.com',
    'org': str(EASYOPS_ORG),
    'user': 'easyops'
}


class OpenApi(object):

    def __init__(self, access_key, secret_key, host):
        self.ACCESS_KEY = access_key
        self.SECRET_KEY = secret_key
        self.HOST = host
        self.HEADER = {'host': 'openapi.easyops-only.com', 'Content-Type': 'application/json'}

    # 生成签名算法
    def signature(self, request_time, method, url, data):

        # 签名准备
        method = method.upper()

        if method in ["GET", "DELETE"]:
            params = "".join(["%s%s" % (key, data[key]) for key in sorted(data.keys())])
            content = ''
        else:
            params = ''
            m = hashlib.md5()
            m.update(json.dumps(data).encode('utf-8'))
            content = m.hexdigest()

        str_sign = '\n'.join([
            method,
            url,
            params,
            'application/json',
            content,
            str(request_time),
            self.ACCESS_KEY])
        # 生成签名
        return hmac.new(self.SECRET_KEY, str_sign, hashlib.sha1).hexdigest()

    # 发起请求函数
    def do_request(self, host, op_url, method, header, data):
        url = "http://%s%s" % (host, op_url)
        #    print(u"准备发送请求: %s \n %s \n %s \n %s" % (method, url, header, str(data)))
        try:
            # 发起请求
            if method.upper() in ["GET", "DELETE"]:
                r = requests.request(method=method, url=url, headers=header, params=data)
            else:
                r = requests.request(method=method, url=url, headers=header, json=data)
            #   print r.text
            # 请求状态判断
            if r.status_code == 200:
                #    print(u"请求成功!")
                jsn = r.json()
                return 0, jsn['data']
            else:
                print(u"请求失败!错误码 %s 详情为:%s" % (r.status_code, r.text))

        except Exception as e:
            print(e)

    # 发起OpenApi请求函数
    def start(self, method, op_url, data=None):
        # 签名预备
        data_ = data or {}
        time_ = int(time.time())
        # 生成签名
        signature = self.signature(request_time=time_, method=method, url=op_url, data=data_)
        # 添加keys并发起连接
        op_url = "%s?accesskey=%s&signature=%s&expires=%s" % (op_url, self.ACCESS_KEY, signature, str(time_))
        return self.do_request(header=self.HEADER, host=self.HOST, op_url=op_url, method=method, data=data)


def get_cmdb_object_instance(object_id, params=None, pagesize=3000):
    proxies = {'http': None, 'https': None}
    search_url = 'http://{EASYOPS_CMDB_HOST}/object/{object_id}/instance/_search' \
        .format(EASYOPS_CMDB_HOST=cmdb_ip, object_id=object_id)
    # search_url = 'http://10.177.96.195/object/{object_id}/instance/_search'.format(object_id=object_id)

    if params is None:
        params = {}
    params['page'] = 1
    params['page_size'] = pagesize

    try:
        result = easy_http(search_url, 'POST', headers=CMDB_HEADERS, json=params, proxies=proxies, verify=False)
        if result:
            if int(result['total']) > pagesize:
                pages = int(result['total']) / pagesize
                result = result['list']
                for i in range(2, pages + 2):
                    params['page'] = i
                    result = result + easy_http(search_url, 'POST', headers=CMDB_HEADERS, json=params, proxies=proxies,
                                                verify=False)['list']
                return result
            else:
                return result['list']
        else:
            return None
    except Exception as e:
        logging.error(u'Get cmdb instance error:{} '.format(e))


# 根据object_id获取{$name: $instanceId}字典
def cmdb_object_name_id_dict(object_id):
    cmdb_instances = get_cmdb_object_instance(object_id)
    if not cmdb_instances:
        logging.error('获取 {} 实例失败'.format(object_id))
        return {}
    object_name_id_dict = {x['name']: x['instanceId'] for x in cmdb_instances}
    return object_name_id_dict


# 批量删除关系
def batch_delete_relation(object_id, relation_side_id, instance_ids, related_instance_ids):
    '''
    POST /object/@object_id/relation/@relation_side_id/remove
    '''
    if not (isinstance(instance_ids, list) and isinstance(related_instance_ids, list)):
        return
    if not (instance_ids and related_instance_ids):
        return
    params = {}
    params['instance_ids'] = instance_ids
    params['related_instance_ids'] = related_instance_ids
    url = 'http://{EASYOPS_CMDB_HOST}/object/{object_id}/relation/{relation_side_id}/remove' \
        .format(EASYOPS_CMDB_HOST=EASYOPS_CMDB_HOST, object_id=object_id, relation_side_id=relation_side_id)
    try:
        easy_http(url, 'POST', headers=CMDB_HEADERS, json=params)
        # TODO 需要优化

        return True
    except Exception as e:
        logging.error(u'批量删除 {} 关系失败: {}'.format(object_id, e))
        return False


class AutoDiscovery():
    # 定义全局变量autodiscovery_data_list
    global autodiscovery_data_list
    autodiscovery_data_list = []

    # 数据格式化
    def data_format(self, json_data, object_id, pks, upsert=False):
        # json_data：需要上报的数据
        # object_id：CMDB模型ID
        # pks：更新依据，格式为list，例如['name']
        # upsert：不存在实例时是否创建,bool类型，True or False，默认为False
        for info_list in json_data:

            if info_list:
                for info in info_list:
                    result = {
                        'dims': {
                            "pks": pks,  # 用于查询出唯一实例的模型字段组合
                            "object_id": object_id,  # CMDB模型ID
                            "upsert": upsert  # 不存在时是否创建，默认为False
                        },
                        'vals': info  # 上报的数据
                    }
                    # 将格式化后的数据append至全局变量autodiscovery_data_list
                    autodiscovery_data_list.append(result)

    # 数据上报
    def report_data(self):
        # 注意：自动采集中只能输出最终的json数据，不能有其它多余输出
        print("-----BEGIN GATHERING DATA-----")
        print(json.dumps(autodiscovery_data_list))
        print("-----END GATHERING DATA-----")


AutoDiscovery = AutoDiscovery()


def decoReturn(f):
    def func(*args, **kwargs):
        try:
            rval = f(*args, **kwargs)
            return rval
        except ApiException, e:
            logging.error('调用函数 {} 时，报错：{}'.format(f.__name__, e))

    return func


# k8s api

class K8S(object):

    def __init__(self, k8s_url, k8s_token, k8s_cluster, timeout_seconds=60, watch=False):
        # self.configuration = configuration
        configuration = client.Configuration()
        configuration.host = k8s_url
        configuration.verify_ssl = False
        configuration.api_key = {"authorization": "Bearer " + k8s_token}
        client1 = api_client.ApiClient(configuration=configuration)
        # 获取大部分k8s数据
        self.api_instance = core_v1_api.CoreV1Api(client1)
        # 获取deployment数据
        self.api_instance1 = apps_v1_api.AppsV1Api(client1)
        # 获取ingress数据
        self.api_instance2 = networking_v1beta1_api.NetworkingV1beta1Api(client1)
        self.timeout_seconds = timeout_seconds
        self.watch = watch
        self.cluster_name = k8s_cluster

    @decoReturn
    def list_persistent_volume(self):
        return self.api_instance.list_persistent_volume(timeout_seconds=self.timeout_seconds, watch=self.watch)

    @decoReturn
    def list_persistent_volume_claim_for_all_namespaces(self):
        return self.api_instance.list_persistent_volume_claim_for_all_namespaces(timeout_seconds=self.timeout_seconds,
                                                                                 watch=self.watch)

    @decoReturn
    def list_limit_range_for_all_namespaces(self):
        return self.api_instance.list_limit_range_for_all_namespaces(timeout_seconds=self.timeout_seconds,
                                                                     watch=self.watch)

    @decoReturn
    def list_service_account_for_all_namespaces(self):
        return self.api_instance.list_service_account_for_all_namespaces(timeout_seconds=self.timeout_seconds,
                                                                         watch=self.watch)

    @decoReturn
    def list_namespace(self):
        return self.api_instance.list_namespace(timeout_seconds=self.timeout_seconds, watch=self.watch)

    @decoReturn
    def list_secret_for_all_namespaces(self):
        return self.api_instance.list_secret_for_all_namespaces(timeout_seconds=self.timeout_seconds, watch=self.watch)

    @decoReturn
    def list_resource_quota_for_all_namespaces(self):
        return self.api_instance.list_resource_quota_for_all_namespaces(timeout_seconds=self.timeout_seconds,
                                                                        watch=self.watch)

    @decoReturn
    def list_service_for_all_namespaces(self):
        return self.api_instance.list_service_for_all_namespaces(timeout_seconds=self.timeout_seconds, watch=self.watch)

    @decoReturn
    def list_endpoints_for_all_namespaces(self):
        return self.api_instance.list_endpoints_for_all_namespaces(timeout_seconds=self.timeout_seconds,
                                                                   watch=self.watch)

    @decoReturn
    def list_config_map_for_all_namespaces(self):
        return self.api_instance.list_config_map_for_all_namespaces(timeout_seconds=self.timeout_seconds,
                                                                    watch=self.watch)

    @decoReturn
    def list_ingress_for_all_namespaces(self):
        return self.api_instance2.list_ingress_for_all_namespaces(timeout_seconds=self.timeout_seconds,
                                                                  watch=self.watch)

    @decoReturn
    def list_deployment_for_all_namespaces(self):
        return self.api_instance1.list_deployment_for_all_namespaces(timeout_seconds=self.timeout_seconds,
                                                                     watch=self.watch)

    @decoReturn
    def list_pod_for_all_namespaces(self):
        return self.api_instance.list_pod_for_all_namespaces(timeout_seconds=self.timeout_seconds, watch=self.watch)

    @decoReturn
    def list_node(self):
        return self.api_instance.list_node(timeout_seconds=self.timeout_seconds, watch=self.watch)


# 蛇形转小驼峰
def snake_to_camel(snake_str):
    return reduce(lambda x, y: x + y.title(), snake_str.split('_'))


def get_k8s_res_info(k8s_resource, collectType, attrs=[]):
    attr_list = ['api_version', 'kind', 'metadata', 'spec', 'name', 'namespace']

    if attrs:
        attr_list.extend(attrs)

    if not k8s_resource:
        logging.warn('There is not k8s_resource')
        return

    if not k8s_resource.items:
        logging.warn('There is not items in {}'.format(k8s_resource))
        return

    ret = []
    nodesInfoList = []
    for x in k8s_resource.items:
        metadata = x.metadata
        spec = x.spec
        meta_dict = {}
        meta_dict["pvcList"] = []
        meta_dict['serviceNameList'] = []
        meta_dict["portsInfo"] = []
        meta_dict["nodesInfo"] = []
        meta_dict["configMap"] = []
        # 获取通用信息
        for attr in attr_list:

            if attr in ['api_version', 'kind']:
                meta_dict[snake_to_camel(attr)] = getattr(k8s_resource, attr)

            # metadata中的字段加在这里
            elif attr in ['name', 'namespace']:
                meta_dict[attr] = getattr(metadata, attr)
                if attr == "name":
                    meta_dict['srcName'] = getattr(metadata, attr)
            else:
                meta_dict[attr] = str(getattr(x, attr))

        # 因为meta_dict append到ret之后是的字符串用单引号，所以不能用json.loads加载，只能在这里解析
        # 获取ingress信息
        if collectType == "ingress":

            try:
                rule_list = getattr(spec, "rules")
                for rule in rule_list:
                    path_list = rule.http.paths
                    for path in path_list:
                        meta_dict['serviceNameList'].append(path.backend.service_name)
            except:
                print(meta_dict['srcName'], "get ingress info failed")

                # 获取service信息
        elif collectType == "service":
            ports = {}

            if spec.selector:
                if spec.selector.get("app"):
                    meta_dict["appLabels"] = spec.selector.get("app")

            if getattr(spec, "ports"):
                if getattr(spec, "ports")[0]:
                    portsInfo = getattr(spec, "ports")[0]
                    ports["name"] = getattr(portsInfo, "name")
                    ports["nodePort"] = getattr(portsInfo, "node_port")
                    ports["port"] = getattr(portsInfo, "port")
                    ports["protocol"] = getattr(portsInfo, "protocol")
                    ports["targetPort"] = getattr(portsInfo, "target_port")
                    meta_dict["portsInfo"].append(ports)

        # 获取deployment信息
        elif collectType == "deployment":
            try:
                meta_dict["appLabels"] = getattr(spec.template.metadata, "labels").get("app")
            except:
                print(meta_dict['srcName'], "get deployment appLabels info failed")

            try:
                meta_dict["replicas"] = spec.replicas

            except Exception as e:
                print(meta_dict['srcName'], "get deployment replicas info failed", e)

            try:
                meta_dict["image"] = []

                # containers_list下有image信息
                containers_list = spec.template.spec.containers
                for containers in containers_list:
                    meta_dict["image"].append(getattr(containers, "image"))
            except:
                print(meta_dict['srcName'], "get deployment image info failed")

            # volumes下面有config_map的信息
            volumes_list = spec.template.spec.volumes
            if volumes_list:
                for volumes in volumes_list:
                    config_map_dict = {}
                    config_map_dict["items"] = ""
                    config_map = volumes.config_map

                    if not config_map:
                        continue

                    if config_map.default_mode:
                        config_map_dict["defaultMode"] = config_map.default_mode

                    if config_map.name:
                        config_map_dict["name"] = config_map.name

                    if config_map.optional:
                        config_map_dict["optional"] = config_map.optional

                    items_string = ""

                    if config_map.items:
                        items = config_map.items
                        for item in items:
                            items_string += "[key:" + str(item.key) + ","
                            items_string += "mode:" + str(item.mode) + ","
                            items_string += "path:" + str(item.path) + "]"
                    config_map_dict["items"] = items_string

                    meta_dict["configMap"].append(config_map_dict)

        # 获取pod信息
        elif collectType == "pod":
            # 获取pod信息中的ip
            pod_datas = getattr(metadata, "managed_fields")
            if pod_datas:
                for pod_data in pod_datas:
                    pod_fields = getattr(pod_data, "fields_v1")
                    if pod_fields.has_key('f:status'):
                        pod_field = pod_fields['f:status']
                        if pod_field.has_key('f:podIPs'):
                            pod_ips = pod_field['f:podIPs'].keys()
                            for pod_ip in pod_ips:
                                if "k:" in pod_ip:
                                    podIp = eval(pod_ip.partition(":")[2])
                                    meta_dict['ip'] = podIp['ip']

            labels_datas = getattr(metadata, "labels")
            if labels_datas:
                if labels_datas.get('app'):
                    meta_dict["appLabels"] = labels_datas.get("app")

            '''
            try:
                meta_dict["appLabels"] = getattr(metadata, "labels").get("app")
            except:
                print(meta_dict['srcName'], "get deployment appLabels info failed")
            '''

            # volumes_list里面有pod下面pvc名字(persistent_volume_claim)
            volumes_list = spec.volumes
            if volumes_list:
                for volumes in volumes_list:
                    volume_claim = getattr(volumes, "persistent_volume_claim")
                    if volume_claim:
                        if volume_claim.claim_name:
                            meta_dict["pvcList"].append(volume_claim.claim_name)
            '''
            try:
                for volumes in volumes_list:
                    meta_dict["pvcList"].append(volumes.persistent_volume_claim.claim_name)                    
            except:
                print(meta_dict['srcName'], "get pod info failed")
            '''

        # 获取nodes信息
        elif collectType == "nodes":
            # labels下有master/worker的信息
            labels = metadata.labels
            masterWorkInfo = ""
            masterWorkDict = {}
            # status.node_info下有version信息
            version = x.status.node_info.kubelet_version

            # annotations下有主机ip信息
            annotations = metadata.annotations
            ip = annotations.get("rke.cattle.io/internal-ip")
            try:
                if labels.get("node-role.kubernetes.io/controlplane") == "true":
                    masterWorkInfo += "controlplane,"

                if labels.get("node-role.kubernetes.io/etcd") == "true":
                    masterWorkInfo += "etcd,"

                if labels.get("node-role.kubernetes.io/worker") == "true":
                    masterWorkInfo += "worker,"

                masterWorkDict = {"hostname": meta_dict['srcName'], "masterWorkInfo": masterWorkInfo, "ip": ip,
                                  "version": version}
                global nodesInfoList
                nodesInfoList.append(masterWorkDict)
                meta_dict["nodesInfo"] = nodesInfoList

            except:
                print(meta_dict['srcName'], "get nodes info failed")

        # 获取namespace信息
        elif collectType == "namespace":
            project_name = ""
            namespace_labels = getattr(metadata, "labels")

            if namespace_labels:
                if namespace_labels.get("monitoring.pandaria.io/projectName"):
                    project_name = namespace_labels.get("monitoring.pandaria.io/projectName")
            meta_dict["projectName"] = project_name

        if meta_dict.get('srcName'):
            ret.append(meta_dict)

    return ret


conn = OpenApi(access_key='29ed29c02f471ad825a3c928',
               secret_key='594c54767869736e4b4341725865716b626f6e4d646e776b75756261656f5745', host='10.2.239.45')


# namespace关联关系用到的name是cluster:name，其他都是cluster_name：namespace
def get_namespace_info(k8s, k8s_info):
    k8s_namespace = k8s.list_namespace()
    ret = get_k8s_res_info(k8s_namespace, "namespace")

    cluster_relation_params = {
        "fields": {
            "name": True
        },
        "page": 1,
        "page_size": 500
    }
    cluster_relation_code, cluster_relation_datas = conn.start(method='post',
                                                               op_url='/cmdb_resource/object/KUBERNETES_CLUSTER/instance/_search',
                                                               data=cluster_relation_params)
    cluster_name_instanceId = {i["name"]: i["instanceId"] for i in cluster_relation_datas['list']}

    for x in ret:
        x['name'] = k8s.cluster_name + ":" + x['srcName']
        x['environment'] = k8s_info['k8s_environment']
        x['datastatus'] = "运行中"

        # cluster关系
        if cluster_relation_code == 0:
            if cluster_name_instanceId.has_key(k8s.cluster_name):
                x["KUBERNETES_CLUSTER"] = [cluster_name_instanceId[k8s.cluster_name]]

        # BUSINESS关系
        if x['projectName']:
            business_relation_url = "/object/BUSINESS/instance/_search"
            business_relation_params = {
                "fields": {
                    "name": True,
                    "shortName": True
                },
                "query": {
                    "shortName": x['projectName'].upper()
                },
                "page": 1,
                "page_size": 10
            }
            #    business_relation_code, business_relation_datas = easyops_request(business_relation_url, "POST", business_relation_params)
            business_relation_code, business_relation_datas = conn.start(method='post',
                                                                         op_url='/cmdb_resource/object/BUSINESS/instance/_search',
                                                                         data=business_relation_params)
            if business_relation_code == 0:
                if len(business_relation_datas['list']) > 0:
                    x['BUSINESS'] = [{'instanceId': business_relation_datas['list'][0]['instanceId']}]

    return ret


def get_deployment_info(k8s):
    k8s_deployment = k8s.list_deployment_for_all_namespaces()
    ret = get_k8s_res_info(k8s_deployment, "deployment")

    pod_relation_url = "/cmdb_resource/object/KUBERNETES_POD/instance/_search"
    pod_relation_params = {
        "fields": {
            "name": True
        }
    }
    pod_relation_code, pod_relation_datas = get_all(url=pod_relation_url, method="POST", data=pod_relation_params)
    pod_name_instanceId = {i["name"]: i["instanceId"] for i in pod_relation_datas}

    for x in ret:
        x['name'] = k8s.cluster_name + ":" + x['namespace'] + ":" + x['srcName']
        x['datastatus'] = "运行中"

        if x.has_key('appLabels'):
            if not x['appLabels']:
                x['appLabels'] = ""

        '''
        if len(x['configMap']) == 0:
            del x['configMap']
        if x.has_key('portsInfo'):
            del x['portsInfo']
        if x.has_key('serviceNameList'):
            del x['serviceNameList']
        if x.has_key('nodesInfo'):
            del x['nodesInfo']
        '''

        # 关系
        # print(x['srcName'], x["appLabels"])
        x['KUBERNETES_POD'] = []
        deploy_pod_name = k8s.cluster_name + ":" + x['namespace'] + ":" + x['srcName']
        if str(pod_relation_code) == "True":
            for pod_instance in pod_name_instanceId:
                if deploy_pod_name in pod_instance:
                    pod_relate = {}
                    pod_relate['instanceId'] = pod_name_instanceId[pod_instance]
                    x['KUBERNETES_POD'].append(pod_relate)

    return ret


def get_ingress_info(k8s):
    k8s_ingress = k8s.list_ingress_for_all_namespaces()
    ret = get_k8s_res_info(k8s_ingress, "ingress")

    service_relation_url = "/cmdb_resource/object/KUBERNETES_SERVICE/instance/_search"
    service_relation_params = {
        "fields": {
            "name": True
        }
    }
    service_relation_code, service_relation_datas = get_all(url=service_relation_url, method="POST",
                                                            data=service_relation_params)
    service_name_instanceId = {i["name"]: i["instanceId"] for i in service_relation_datas}

    for x in ret:
        x['name'] = k8s.cluster_name + ":" + x['namespace'] + ":" + x['srcName']
        x['datastatus'] = "运行中"

        # 关系
        x['KUBERNETES_SERVICE'] = []
        for service_name in x["serviceNameList"]:
            if str(service_relation_code) == "True":
                if service_name_instanceId.has_key(k8s.cluster_name + ":" + x['namespace'] + ":" + service_name):
                    x['KUBERNETES_SERVICE'] = [
                        service_name_instanceId[k8s.cluster_name + ":" + x['namespace'] + ":" + service_name]]

    return ret


def get_service_info(k8s):
    k8s_ingress = k8s.list_service_for_all_namespaces()
    ret = get_k8s_res_info(k8s_ingress, "service")

    deploy_relation_url = "/cmdb_resource/object/KUBERNETES_DEPLOYMENT/instance/_search"
    deploy_relation_params = {
        "fields": {
            "name": True
        }
    }
    deploy_relation_code, deploy_relation_datas = get_all(url=deploy_relation_url, method="POST",
                                                          data=deploy_relation_params)
    deploy_name_instanceId = {i["name"]: i["instanceId"] for i in deploy_relation_datas}

    for x in ret:
        x['name'] = k8s.cluster_name + ":" + x['namespace'] + ":" + x['srcName']
        x['datastatus'] = "运行中"

        if x.has_key('appLabels'):
            if not x['appLabels']:
                x['appLabels'] = ""

        # 关系
        if x.get('appLabels'):
            if str(deploy_relation_code) == "True":
                if deploy_name_instanceId.has_key(k8s.cluster_name + ":" + x['namespace'] + ":" + x['appLabels']):
                    x['KUBERNETES_DEPLOYMENT'] = [
                        deploy_name_instanceId[k8s.cluster_name + ":" + x['namespace'] + ":" + x['appLabels']]]

    return ret


def get_pod_info(k8s, k8s_info):
    k8s_pod = k8s.list_pod_for_all_namespaces()
    ret = get_k8s_res_info(k8s_pod, "pod")

    pvc_relation_url = "/cmdb_resource/object/KUBERNETES_PVC/instance/_search"
    pvc_relation_params = {
        "fields": {
            "name": True
        }
    }
    pvc_relation_code, pvc_relation_datas = get_all(url=pvc_relation_url, method="POST", data=pvc_relation_params)
    pvc_name_instanceId = {i["name"]: i["instanceId"] for i in pvc_relation_datas}

    cluster_relation_params = {
        "fields": {
            "name": True
        },
        "page": 1,
        "page_size": 500
    }
    cluster_relation_code, cluster_relation_datas = conn.start(method='post',
                                                               op_url='/cmdb_resource/object/KUBERNETES_CLUSTER/instance/_search',
                                                               data=cluster_relation_params)
    cluster_name_instanceId = {i["name"]: i["instanceId"] for i in cluster_relation_datas['list']}

    namespace_relation_url = "/cmdb_resource/object/KUBERNETES_NAMESPACE/instance/_search"
    namespace_relation_params = {
        "fields": {
            "name": True
        }
    }
    namespace_relation_code, namespace_relation_datas = get_all(url=namespace_relation_url, method="POST",
                                                                data=namespace_relation_params)
    namespace_name_instanceId = {i["name"]: i["instanceId"] for i in namespace_relation_datas}

    for x in ret:
        x['realName'] = k8s.cluster_name + ":" + x['namespace'] + ":" + x['srcName']
        # 关联deployment的依据

        x['environment'] = k8s_info['k8s_environment']
        x['datastatus'] = "运行中"

        x['name'] = k8s.cluster_name + ":" + x['namespace'] + ":" + x['srcName']

        if x.has_key('appLabels'):
            if not x['appLabels']:
                x['appLabels'] = ""

        '''
        if x.has_key('metadata'):
            del x['metadata']
        if x.has_key('spec'):
            del x['spec']        
        '''

        # 关系
        if str(pvc_relation_code) == "True":
            for pvc in x["pvcList"]:
                if pvc_name_instanceId.has_key(k8s.cluster_name + ":" + x['namespace'] + ":" + pvc):
                    x['KUBERNETES_PVC'] = [pvc_name_instanceId[k8s.cluster_name + ":" + x['namespace'] + ":" + pvc]]
        if cluster_relation_code == 0:
            if cluster_name_instanceId.has_key(k8s.cluster_name):
                x["KUBERNETES_CLUSTER"] = [cluster_name_instanceId[k8s.cluster_name]]
        if str(namespace_relation_code) == "True":
            if namespace_name_instanceId.has_key(k8s.cluster_name + ":" + x['namespace']):
                x['KUBERNETES_NAMESPACE'] = [namespace_name_instanceId[k8s.cluster_name + ":" + x['namespace']]]

    return ret


def get_pvc_info(k8s):
    k8s_pod = k8s.list_persistent_volume_claim_for_all_namespaces()
    ret = get_k8s_res_info(k8s_pod, "pvc")

    cluster_relation_url = "/object/KUBERNETES_CLUSTER/instance/_search"
    cluster_relation_params = {
        "fields": {
            "name": True
        },
        "page": 1,
        "page_size": 500
    }
    cluster_relation_code, cluster_relation_datas = conn.start(method='post',
                                                               op_url='/cmdb_resource/object/KUBERNETES_CLUSTER/instance/_search',
                                                               data=cluster_relation_params)
    cluster_name_instanceId = {i["name"]: i["instanceId"] for i in cluster_relation_datas['list']}

    for x in ret:
        x['name'] = k8s.cluster_name + ":" + x['namespace'] + ":" + x['srcName']
        x['datastatus'] = "运行中"
        # 关系
        if cluster_relation_code == 0:
            if cluster_name_instanceId.has_key(k8s.cluster_name):
                x["KUBERNETES_CLUSTER"] = [cluster_name_instanceId[k8s.cluster_name]]
        # print(x)
    return ret


def get_nodes_info(k8s):
    k8s_nodes = k8s.list_node()
    ret = get_k8s_res_info(k8s_nodes, "nodes")

    if not ret:
        logging.warn('There is not ret')
        return

    for x in ret:
        x['name'] = k8s.cluster_name

        # 关系
        '''
        x['HOST'] = []        
        for nodesInfo in x['nodesInfo']:
            x['HOST'].append({'ip': nodesInfo["ip"]})
        '''
        # print(x)
    return ret


# 分页获取所有数据
def get_all(url, method, data=None):
    #  data = kwargs.get("json", {}) or kwargs["params"]
    data["page"] = 1
    data["page_size"] = data.get("page_size", 500)

    # 标准openAPI请求发起器
    conn = OpenApi(access_key='29ed29c02f471ad825a3c928',
                   secret_key='594c54767869736e4b4341725865716b626f6e4d646e776b75756261656f5745', host='10.2.239.45')
    code, r = conn.start(method=method, op_url=url, data=data)

    total = r["total"]
    pages = total / data['page_size'] + (total % data['page_size'] and total / data['page_size'] != 0)

    all_data = r["list"]

    for x in range(2, pages + 1):
        data["page"] = x
        other_code, other_data = conn.start(method='post', op_url=url, data=data)
        all_data += other_data['list']

    return True, all_data


def easyops_request(url, method, params=None):
    headers = {'Host': 'cmdb.easyops-only.com', 'org': str(EASYOPS_ORG), 'user': 'easyops',
               'content-type': 'application/json'}
    url = 'http://{EASYOPS_CMDB_HOST}{url}'.format(EASYOPS_CMDB_HOST=EASYOPS_CMDB_HOST, url=url)
    print url
    #  print headers
    #  print params
    #  print method
    response = requests.request(method, url, headers=headers, json=params)
    print response.text
    try:
        response_json = response.json()
        if response.status_code == 200:
            if response_json['code'] == 0:
                return 0, response_json['data']  # success
            else:
                return response_json['code'], response_json['data']
        else:
            try:
                return response_json['code'], response_json['data']
            except Exception as e:
                print "http exception: ", e
    except Exception as e:
        print e


if __name__ == "__main__":

    # 标准openAPI请求发起器
    conn = OpenApi(access_key='29ed29c02f471ad825a3c928',
                   secret_key='594c54767869736e4b4341725865716b626f6e4d646e776b75756261656f5745', host='10.2.239.45')

    k8s_config = []
    cluster_url = "/object/KUBERNETES_CLUSTER/instance/_search"
    cluster_params = {
        "fields": {
            "name": True,
            "token": True,
            "url": True,
            "environment": True
        },
        "page": 1,
        "page_size": 3000
    }
    #    cluster_code, cluster_datas = easyops_request(cluster_url, "POST", cluster_params)
    cluster_code, cluster_datas = conn.start(method='post',
                                             op_url='/cmdb_resource/object/KUBERNETES_CLUSTER/instance/_search',
                                             data=cluster_params)

    if cluster_code == 0:
        if len(cluster_datas['list']) > 0:
            for cluster_data in cluster_datas['list']:
                cluster_info = {}
                cluster_info['k8s_cluster'] = cluster_data['name']
                cluster_info['k8s_token'] = cluster_data['token']
                cluster_info['k8s_url'] = cluster_data['url']
                cluster_info['k8s_environment'] = cluster_data['environment']
                k8s_config.append(cluster_info)

    urllib3.disable_warnings()

    nodes_list = []
    namespaces_list = []
    deployment_list = []
    ingress_list = []
    service_list = []
    pod_list = []
    pvc_list = []
    
    # k8s_config = [{'k8s_environment': u'\u9884\u53d1\u5e03', 'k8s_cluster': u'test-cluster1', 'k8s_url': u'https://10.23.0.15:6443', 'k8s_token': u'eyJhbGciOiJSUzI1NiIsImtpZCI6IjRhQXRvT1F0ZDhIN1M5ODdnbFBjdjlFY2p6Mk5hX0dUbG9TUHoycWktWEUifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImVjcy1jbWRiLXRva2VuLW5nNnBoIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImVjcy1jbWRiIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQudWlkIjoiYTQ4M2QzY2EtMWNhZS00NmU0LTlhYzEtMjFiOTQ1YTkyNTM3Iiwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50OmRlZmF1bHQ6ZWNzLWNtZGIifQ.x8I7vuZMv5W226e7IgxK2Cwr0V5NjL2_Vasw2dL6VzIeE_F33dug2FzcBY9EbZR4EJ9amAW5bXj51OvYSOAr9M8ua6-ZoZF4A4dXUYBQ7Fx0x6N6--RiyBmTtCC2EatjPpTdU6Pld5zjP30dQTqbwZUJYWleQDv1y3R1TCfzGbwcn7jw2rzWgLvd8dN4CuOyssFhqUVrncGDSZHSDRlDOIO9xmLmvRbnCyQo2QT3MeSSnEL3nw0nL_mXtv1EnS_kElg5O-tOtYpses1jBnL3ExaiT_4iKU4MkSPL37TUp9avKP4Rff6ie9KJmPJC7Pv0CejpRtwl2G3YkvcbhITFHw'}]
    #此k8s_config是通过上面的接口获取的环境类型、集群名称、k8s url、可读权限token的列表格式数据，测试的话，可自定义一条数据
    
    for k8s_info in k8s_config:
        k8s = K8S(k8s_info['k8s_url'], k8s_info['k8s_token'], k8s_info['k8s_cluster'])
        logging.info(u'开始采集nodes')
        nodes = get_nodes_info(k8s)
        nodes_list.append(nodes)
        # cluster nodes
        import_cluster_url = "/object/KUBERNETES_CLUSTER/instance/_import"
        cluster_contact = []
        for info_list in nodes_list:
            if info_list:
                for info in info_list:
                    cluster_contact.append(info)
        import_cluster_params = {
            "keys": ["name"],
            "datas": cluster_contact
        }

        print "---开始导入KUBERNETES_CLUSTER---"
        #     easyops_request(import_cluster_url, "POST", import_cluster_params)
        conn.start(method='post', op_url='/cmdb_resource/object/KUBERNETES_CLUSTER/instance/_import',
                   data=import_cluster_params)
        print "---导入KUBERNETES_CLUSTER完成---"

        logging.info(u'开始采集namespace')
        namespaces = get_namespace_info(k8s, k8s_info)
        namespaces_list.append(namespaces)
        # namespace
        import_namespaces_url = "/object/KUBERNETES_NAMESPACE/instance/_import"
        namespaces_contact = []
        namespace_name = []
        for namespaces_datas in namespaces_list:
            if namespaces_datas:
                for namespaces_data in namespaces_datas:
                    namespace_name.append(namespaces_data['name'])
                    namespaces_contact.append(namespaces_data)
        import_namespaces_params = {
            "keys": ["name"],
            "datas": namespaces_contact
        }

        print "---开始导入KUBERNETES_NAMESPACE---"
        #     easyops_request(import_namespaces_url, "POST", import_namespaces_params)
        conn.start(method='post', op_url='/cmdb_resource/object/KUBERNETES_NAMESPACE/instance/_import',
                   data=import_namespaces_params)
        print "---导入KUBERNETES_NAMESPACE完成---"
        print "---开始梳理KUBERNETES_NAMESPACE旧数据---"
        namespace_url = "/cmdb_resource/object/KUBERNETES_NAMESPACE/instance/_search"
        namespace_params = {
            "fields": {
                "name": True
            },
            "query": {
                "name": {
                    "$nin": namespace_name
                },
                "KUBERNETES_CLUSTER.name": k8s_info['k8s_cluster']
            }
        }
        namespace_code, namespace_datas = get_all(url=namespace_url, method="POST", data=namespace_params)
        if str(namespace_code) == "True":
            namespace_contact = []
            for namespace_data in namespace_datas:
                namespace_dict = {}
                namespace_dict['name'] = namespace_data['name']
                namespace_dict['datastatus'] = "已下线"
                namespace_contact.append(namespace_dict)
            namespace_update_url = "/cmdb_resource/object/KUBERNETES_NAMESPACE/instance/_import"
            namespace_update_method = "POST"
            namespace_update_params = {
                "keys": ["name"],
                "datas": namespace_contact
            }
            conn.start(method="POST", op_url=namespace_update_url, data=namespace_update_params)
        print "---梳理KUBERNETES_NAMESPACE旧数据完成---"

        logging.info(u'开始采集pvc')
        pvc = get_pvc_info(k8s)
        pvc_list.append(pvc)
        # pvc
        import_pvc_url = "/object/KUBERNETES_PVC/instance/_import"
        pvc_contact = []
        pvc_name = []
        for pvc_datas in pvc_list:
            if pvc_datas:
                for pvc_data in pvc_datas:
                    pvc_name.append(pvc_data['name'])
                    pvc_contact.append(pvc_data)
        import_pvc_params = {
            "keys": ["name"],
            "datas": pvc_contact
        }
        print "---开始导入KUBERNETES_PVC---"
        #     easyops_request(import_pvc_url, "POST", import_pvc_params)
        conn.start(method='post', op_url='/cmdb_resource/object/KUBERNETES_PVC/instance/_import',
                   data=import_pvc_params)
        print "---导入KUBERNETES_PVC完成---"
        print "---开始梳理KUBERNETES_PVC旧数据---"
        pvc_url = "/cmdb_resource/object/KUBERNETES_PVC/instance/_search"
        pvc_params = {
            "fields": {
                "name": True
            },
            "query": {
                "name": {
                    "$nin": pvc_name
                },
                "KUBERNETES_CLUSTER.name": k8s_info['k8s_cluster']
            }
        }
        pvc_code, pvc_datas = get_all(url=pvc_url, method="POST", data=pvc_params)
        if str(pvc_code) == "True":
            pvc_contact = []
            for pvc_data in pvc_datas:
                pvc_dict = {}
                pvc_dict['name'] = pvc_data['name']
                pvc_dict['datastatus'] = "已下线"
                pvc_contact.append(pvc_dict)
            pvc_update_url = "/cmdb_resource/object/KUBERNETES_PVC/instance/_import"
            pvc_update_method = "POST"
            pvc_update_params = {
                "keys": ["name"],
                "datas": pvc_contact
            }
            conn.start(method="POST", op_url=pvc_update_url, data=pvc_update_params)
        print "---梳理KUBERNETES_PVC旧数据完成---"

        logging.info(u'开始采集pod')
        pod = get_pod_info(k8s, k8s_info)
        pod_list.append(pod)
        # pod
        pod_name = []
        import_pod_url = "/object/KUBERNETES_POD/instance/_import"
        pod_contact = []
        for pod_datas in pod_list:
            if pod_datas:
                for pod_data in pod_datas:
                    pod_name.append(pod_data['name'])
                    pod_contact.append(pod_data)
        import_pod_params = {
            "keys": ["name"],
            "datas": pod_contact
        }
        print "---开始导入KUBERNETES_POD---"
        #     easyops_request(import_pod_url, "POST", import_pod_params)
        conn.start(method='post', op_url='/cmdb_resource/object/KUBERNETES_POD/instance/_import',
                   data=import_pod_params)
        print "---导入KUBERNETES_POD完成---"
        print "---开始梳理KUBERNETES_POD旧数据---"
        pod_url = "/cmdb_resource/object/KUBERNETES_POD/instance/_search"

        pod_params = {
            "fields": {
                "name": True
            },
            "query": {
                "name": {"$nin": pod_name},
                "KUBERNETES_CLUSTER.name": k8s_info['k8s_cluster']
            }
        }
        pod_code, pod_datas = get_all(url=pod_url, method="POST", data=pod_params)
        if str(pod_code) == "True":
            datastatus_contact = []
            for pod_data in pod_datas:
                datastatus_dict = {}
                datastatus_dict['name'] = pod_data['name']
                datastatus_dict['datastatus'] = "已下线"
                datastatus_contact.append(datastatus_dict)

            pod_update_url = "/cmdb_resource/object/KUBERNETES_POD/instance/_import"
            pod_update_method = "POST"
            pod_update_params = {
                "keys": ["name"],
                "datas": datastatus_contact
            }
            conn.start(method="POST", op_url=pod_update_url, data=pod_update_params)
        print "---梳理KUBERNETES_POD旧数据完成---"

        logging.info(u'开始采集deployment')
        deployment = get_deployment_info(k8s)
        deployment_list.append(deployment)
        # deployment
        import_deploy_url = "/object/KUBERNETES_DEPLOYMENT/instance/_import"
        deploy_contact = []
        deploy_name = []
        for deploy_datas in deployment_list:
            if deploy_datas:
                for deploy_data in deploy_datas:
                    deploy_name.append(deploy_data['name'])
                    deploy_contact.append(deploy_data)
        import_deploy_params = {
            "keys": ["name"],
            "datas": deploy_contact
        }
        print "---开始导入KUBERNETES_DEPLOYMENT---"
        #     easyops_request(import_deploy_url, "POST", import_deploy_params)
        conn.start(method='post', op_url='/cmdb_resource/object/KUBERNETES_DEPLOYMENT/instance/_import',
                   data=import_deploy_params)
        print "---导入KUBERNETES_DEPLOYMENT完成---"
        print "---开始梳理KUBERNETES_DEPLOYMENT旧数据---"
        deploy_url = "/cmdb_resource/object/KUBERNETES_DEPLOYMENT/instance/_search"
        deploy_params = {
            "fields": {
                "name": True
            },
            "query": {
                "name": {"$nin": deploy_name},
                "name": {"$like": "%" + k8s_info['k8s_cluster'] + "%"}
            }
        }
        deploy_code, deploy_datas = get_all(url=deploy_url, method="POST", data=deploy_params)
        if str(deploy_code) == "True":
            datastatus_contact = []
            for deploy_data in deploy_datas:
                datastatus_dict = {}
                datastatus_dict['name'] = deploy_data['name']
                datastatus_dict['datastatus'] = "已下线"
                datastatus_contact.append(datastatus_dict)

            deploy_update_url = "/cmdb_resource/object/KUBERNETES_DEPLOYMENT/instance/_import"
            deploy_update_method = "POST"
            deploy_update_params = {
                "keys": ["name"],
                "datas": datastatus_contact
            }
            conn.start(method="POST", op_url=deploy_update_url, data=deploy_update_params)
        print "---梳理KUBERNETES_DEPLOYMENT旧数据完成---"

        logging.info(u'开始采集service')
        pod = get_service_info(k8s)
        service_list.append(pod)
        # service
        import_service_url = "/object/KUBERNETES_SERVICE/instance/_import"
        service_contact = []
        service_name = []
        for service_datas in service_list:
            if service_datas:
                for service_data in service_datas:
                    service_name.append(service_data['name'])
                    service_contact.append(service_data)
        import_service_params = {
            "keys": ["name"],
            "datas": service_contact
        }

        print "---开始导入KUBERNETES_SERVICE---"
        #     easyops_request(import_service_url, "POST", import_service_params)
        conn.start(method='post', op_url='/cmdb_resource/object/KUBERNETES_SERVICE/instance/_import',
                   data=import_service_params)
        print "---导入KUBERNETES_SERVICE完成---"
        print "---开始梳理KUBERNETES_SERVICE旧数据---"
        service_url = "/cmdb_resource/object/KUBERNETES_SERVICE/instance/_search"
        service_params = {
            "fields": {
                "name": True
            },
            "query": {
                "name": {"$nin": service_name},
                "name": {"$like": "%" + k8s_info['k8s_cluster'] + "%"}
            }
        }
        service_code, service_datas = get_all(url=service_url, method="POST", data=service_params)
        if str(service_code) == "True":
            datastatus_contact = []
            for service_data in service_datas:
                datastatus_dict = {}
                datastatus_dict['name'] = service_data['name']
                datastatus_dict['datastatus'] = "已下线"
                datastatus_contact.append(datastatus_dict)

            service_update_url = "/cmdb_resource/object/KUBERNETES_SERVICE/instance/_import"
            service_update_method = "POST"
            service_update_params = {
                "keys": ["name"],
                "datas": datastatus_contact
            }
            conn.start(method="POST", op_url=service_update_url, data=service_update_params)
        print "---梳理KUBERNETES_SERVICE旧数据完成---"

        logging.info(u'开始采集ingress')
        pod = get_ingress_info(k8s)
        ingress_list.append(pod)
        # ingress
        import_ingress_url = "/object/KUBERNETES_INGRESS/instance/_import"
        ingress_contact = []
        ingress_name = []
        for ingress_datas in ingress_list:
            if ingress_datas:
                for ingress_data in ingress_datas:
                    ingress_name.append(ingress_data['name'])
                    ingress_contact.append(ingress_data)
        import_ingress_params = {
            "keys": ["name"],
            "datas": ingress_contact
        }
        print "---开始导入KUBERNETES_INGRESS---"
        #     easyops_request(import_ingress_url, "POST", import_ingress_params)
        conn.start(method='post', op_url='/cmdb_resource/object/KUBERNETES_INGRESS/instance/_import',
                   data=import_ingress_params)
        print "---导入KUBERNETES_INGRESS完成---"
        print "---开始梳理KUBERNETES_INGRESS旧数据---"
        ingress_url = "/cmdb_resource/object/KUBERNETES_INGRESS/instance/_search"
        ingress_params = {
            "fields": {
                "name": True
            },
            "query": {
                "name": {"$nin": ingress_name},
                "name": {"$like": "%" + k8s_info['k8s_cluster'] + "%"}
            }
        }
        ingress_code, ingressdatas = get_all(url=ingress_url, method="POST", data=ingress_params)
        if str(ingress_code) == "True":
            datastatus_contact = []
            for ingressdata in ingressdatas:
                datastatus_dict = {}
                datastatus_dict['name'] = ingressdata['name']
                datastatus_dict['datastatus'] = "已下线"
                datastatus_contact.append(datastatus_dict)

            ingress_update_url = "/cmdb_resource/object/KUBERNETES_INGRESS/instance/_import"
            ingress_update_method = "POST"
            ingress_update_params = {
                "keys": ["name"],
                "datas": datastatus_contact
            }
            conn.start(method="POST", op_url=ingress_update_url, data=ingress_update_params)
        print "---梳理KUBERNETES_INGRESS旧数据完成---"
