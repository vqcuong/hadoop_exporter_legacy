#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
import re
from prometheus_client.core import GaugeMetricFamily

from hadoop_exporter import utils
from hadoop_exporter.common import MetricCollector, common_metrics_info


class YARNResourceManagerMetricCollector(MetricCollector):

    NODE_STATE = {
        'NEW': 1,
        'RUNNING': 2,
        'UNHEALTHY': 3,
        'DECOMMISSIONED': 4,
        'LOST': 5,
        'REBOOTED': 6,
    }

    COMPONENT = "yarn"
    SERVICE = "resourcemanager"

    def __init__(self, cluster, url):
        MetricCollector.__init__(
            self, cluster, url, self.COMPONENT, self.SERVICE)
        self.logger = utils.get_logger(
            __name__, log_file=f"{self.COMPONENT}_{self.SERVICE}.log")
        self._yarn_resourcemanager_metrics = {}
        for i in range(len(self._file_list)):
            self._yarn_resourcemanager_metrics.setdefault(
                self._file_list[i], {})

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'

        try:
            beans = utils.get_metrics(self._url)
        except:
            self.logger.info(
                "Can't scrape metrics from url: {0}".format(self._url))
            pass
        else:
            # set up all metrics with labels and descriptions.
            self._setup_metrics_labels(beans)

            # add metric value to every metric.
            self._get_metrics(beans)

            # update namenode metrics with common metrics
            common_metrics = common_metrics_info(
                self._cluster, beans, self.COMPONENT, self.SERVICE)
            self._yarn_resourcemanager_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._yarn_resourcemanager_metrics[service]:
                    yield self._yarn_resourcemanager_metrics[service][metric]

    def _setup_rmnminfo_labels(self):
        for metric in self._metrics['RMNMInfo']:
            label = ["cluster", "host", "version", "rack"]
            if 'NumContainers' in metric:
                name = "_".join([self._prefix, 'node_containers_total'])
            elif 'State' in metric:
                name = "_".join([self._prefix, 'node_state'])
            elif 'UsedMemoryMB' in metric:
                name = "_".join([self._prefix, 'node_memory_used'])
            elif 'AvailableMemoryMB' in metric:
                name = "_".join([self._prefix, 'node_memory_available'])
            else:
                pass
            self._yarn_resourcemanager_metrics['RMNMInfo'][metric] = GaugeMetricFamily(name,
                                                                                       self._metrics['RMNMInfo'][metric],
                                                                                       labels=label)

    def _setup_queue_labels(self):
        running_flag = 1
        for metric in self._metrics['QueueMetrics']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if "running_" in metric:
                if running_flag:
                    running_flag = 0
                    label = ["cluster", "elapsed_time"]
                    key = "running_app"
                    name = "running_app_total"
                    descriptions = "Current number of running applications in each elapsed time ( < 60min, 60min < x < 300min, 300min < x < 1440min and x > 1440min )"
                    self._yarn_resourcemanager_metrics['QueueMetrics'][key] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                                descriptions,
                                                                                                labels=label)
                else:
                    continue
            else:
                label = ["cluster"]
                self._yarn_resourcemanager_metrics['QueueMetrics'][metric] = GaugeMetricFamily("_".join([self._prefix, snake_case]),
                                                                                               self._metrics['QueueMetrics'][metric],
                                                                                               labels=label)

    def _setup_cluster_labels(self):
        nm_flag, cm_num_flag, cm_avg_flag = 1, 1, 1
        for metric in self._metrics['ClusterMetrics']:
            if "NMs" in metric:
                if nm_flag:
                    nm_flag = 0
                    label = ["cluster", "status"]
                    key = "NMs"
                    name = "nodemanager_total"
                    descriptions = "Current number of NodeManagers in each status"
                else:
                    continue
            elif "NumOps" in metric:
                if cm_num_flag:
                    cm_num_flag = 0
                    label = ["cluster", "oper"]
                    key = "NumOps"
                    name = "ams_total"
                    descriptions = "Total number of Applications Masters in each operation"
                else:
                    continue
            elif "AvgTime" in metric:
                if cm_avg_flag:
                    cm_avg_flag = 0
                    label = ["cluster", "oper"]
                    key = "AvgTime"
                    name = "average_time_milliseconds"
                    descriptions = "Average time in milliseconds AM spends in each operation"
                else:
                    continue
            else:
                key = metric
                name = metric
                description = self._metrics['ClusterMetrics'][metric]
                label = ["cluster"]
            self._yarn_resourcemanager_metrics['ClusterMetrics'][key] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                          descriptions,
                                                                                          labels=label)

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for bean in beans:
            if 'RMNMInfo' in bean['name']:
                self._setup_rmnminfo_labels()
            if 'QueueMetrics' in self._metrics:
                self._setup_queue_labels()
            if 'ClusterMetrics' in self._metrics:
                self._setup_cluster_labels()

    def _get_rmnminfo_metrics(self, bean):
        for metric in self._metrics['RMNMInfo']:
            live_nm_list = yaml.safe_load(bean['LiveNodeManagers'])
            for j in range(len(live_nm_list)):
                host = live_nm_list[j]['HostName']
                version = live_nm_list[j]['NodeManagerVersion']
                rack = live_nm_list[j]['Rack']
                label = [self._cluster, host, version, rack]
                if 'State' == metric:
                    value = self.NODE_STATE[live_nm_list[j]['State']]
                else:
                    value = live_nm_list[j][metric] if metric in live_nm_list[j] else 0.0
                self._yarn_resourcemanager_metrics['RMNMInfo'][metric].add_metric(
                    label, value)

    def _get_queue_metrics(self, bean):
        for metric in self._metrics['QueueMetrics']:
            label = [self._cluster]
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if "running_0" in metric:
                key = "running_app"
                label.append("0to60")
            elif "running_60" in metric:
                key = "running_app"
                label.append("60to300")
            elif "running_300" in metric:
                key = "running_app"
                label.append("300to1440")
            elif "running_1440" in metric:
                key = "running_app"
                label.append("1440up")
            else:
                key = metric
            self._yarn_resourcemanager_metrics['QueueMetrics'][key].add_metric(label,
                                                                               bean[metric] if metric in bean else 0)

    def _get_cluster_metrics(self, bean):
        for metric in self._metrics['ClusterMetrics']:
            label = [self._cluster]
            if "NMs" in metric:
                label.append(metric.split('NMs')[0].split('Num')[1])
                key = "NMs"
            elif "NumOps" in metric:
                key = "NumOps"
                label.append(metric.split("DelayNumOps")[0].split('AM')[1])
            elif "AvgTime" in metric:
                key = "AvgTime"
                label.append(metric.split("DelayAvgTime")[0].split('AM')[1])
            else:
                continue
            self._yarn_resourcemanager_metrics['ClusterMetrics'][key].add_metric(
                label, bean[metric] if metric in bean else 0)

    def _get_metrics(self, beans):

        for bean in beans:
            if 'RMNMInfo' in bean['name']:
                self._get_rmnminfo_metrics(bean)

            if 'QueueMetrics' in bean['name'] and 'root' == bean['tag.Queue']:
                self._get_queue_metrics(bean)

            if 'ClusterMetrics' in bean['name']:
                self._get_cluster_metrics(bean)
