#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from prometheus_client.core import GaugeMetricFamily

from hadoop_exporter import utils
from hadoop_exporter.common import MetricCollector, common_metrics_info


class HDFSNameNodeMetricCollector(MetricCollector):
    COMPONENT = "hdfs"
    SERVICE = "namenode"

    def __init__(self, cluster, url):
        MetricCollector.__init__(
            self, cluster, url, self.COMPONENT, self.SERVICE)
        self.logger = utils.get_logger(
            __name__, log_file=f"{self.COMPONENT}_{self.SERVICE}.log")
        self._hdfs_namenode_metrics = {}
        for f in self._file_list:
            self._hdfs_namenode_metrics.setdefault(f, {})

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
            self._hdfs_namenode_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hdfs_namenode_metrics[service]:
                    yield self._hdfs_namenode_metrics[service][metric]

    def _setup_nnactivity_labels(self):
        num_namenode_flag, avg_namenode_flag, ops_namenode_flag = 1, 1, 1
        for metric in self._metrics['NameNodeActivity']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            labels = ["cluster", "method"]
            if "NumOps" in metric:
                if num_namenode_flag:
                    key = "MethodNumOps"
                    self._hdfs_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(
                        "_".join([self._prefix, "nnactivity_method_ops_total"]),
                        "Total number of the times the method is called.",
                        labels=labels
                    )
                    num_namenode_flag = 0
                else:
                    continue
            elif "AvgTime" in metric:
                if avg_namenode_flag:
                    key = "MethodAvgTime"
                    self._hdfs_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(
                        "_".join([self._prefix, "nnactivity_method_avg_time_milliseconds"]),
                        "Average turn around time of the method in milliseconds.",
                        labels=labels
                    )
                    avg_namenode_flag = 0
                else:
                    continue
            else:
                if ops_namenode_flag:
                    ops_namenode_flag = 0
                    key = "Operations"
                    self._hdfs_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(
                        "_".join([self._prefix, "nnactivity_operations_total"]),
                        "Total number of each operation.",
                        labels=labels
                    )
                else:
                    continue

    def _setup_startupprogress_labels(self):
        sp_count_flag, sp_elapsed_flag, sp_total_flag, sp_complete_flag = 1, 1, 1, 1
        for metric in self._metrics['StartupProgress']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            labels = []
            if "ElapsedTime" == metric:
                key = "ElapsedTime"
                name = "total_elapsed_time_milliseconds"
                descriptions = "Total elapsed time in milliseconds."
            elif "PercentComplete" == metric:
                key = "PercentComplete"
                name = "complete_rate"
                descriptions = "Current rate completed in NameNode startup progress  (The max value is not 100 but 1.0)."

            elif "Count" in metric:
                if sp_count_flag:
                    sp_count_flag = 0
                    key = "PhaseCount"
                    name = "phase_count"
                    labels = ["cluster", "phase"]
                    descriptions = "Total number of steps completed in the phase."
                else:
                    continue
            elif "ElapsedTime" in metric:
                if sp_elapsed_flag:
                    sp_elapsed_flag = 0
                    key = "PhaseElapsedTime"
                    name = "phase_elapsed_time_milliseconds"
                    labels = ["cluster", "phase"]
                    descriptions = "Total elapsed time in the phase in milliseconds."
                else:
                    continue
            elif "Total" in metric:
                if sp_total_flag:
                    sp_total_flag = 0
                    key = "PhaseTotal"
                    name = "phase_total"
                    labels = ["cluster", "phase"]
                    descriptions = "Total number of steps in the phase."
                else:
                    continue
            elif "PercentComplete" in metric:
                if sp_complete_flag:
                    sp_complete_flag = 0
                    key = "PhasePercentComplete"
                    name = "phase_complete_rate"
                    labels = ["cluster", "phase"]
                    descriptions = "Current rate completed in the phase  (The max value is not 100 but 1.0)."
                else:
                    continue
            else:
                key = metric
                name = snake_case
                labels = ["cluster"]
                descriptions = self._metrics['StartupProgress'][metric]

            self._hdfs_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(
                "_".join([self._prefix, "startup_process", name]),
                descriptions,
                labels=labels)

    def _setup_fsnamesystem_labels(self):
        cap_flag = 1
        for metric in self._metrics['FSNamesystem']:
            if metric.startswith('Capacity'):
                if cap_flag:
                    cap_flag = 0
                    key = "capacity"
                    labels = ["cluster", "mode"]
                    name = "capacity_bytes"
                    descriptions = "Current DataNodes capacity in each mode in bytes"
                else:
                    continue
            else:
                key = metric
                labels = ["cluster"]
                name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                descriptions = self._metrics['FSNamesystem'][metric]
            self._hdfs_namenode_metrics['FSNamesystem'][key] = GaugeMetricFamily(
                "_".join([self._prefix, "fsname_system", name]),
                descriptions,
                labels=labels
            )

    def _setup_fsnamesystem_state_labels(self):
        num_flag = 1
        for metric in self._metrics['FSNamesystemState']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if 'DataNodes' in metric:
                if num_flag:
                    num_flag = 0
                    key = "datanodes_num"
                    labels = ["cluster", "state"]
                    descriptions = "Number of datanodes in each state"
                else:
                    continue
            else:
                key = metric
                labels = ["cluster"]
                descriptions = self._metrics['FSNamesystemState'][metric]
            self._hdfs_namenode_metrics['FSNamesystemState'][key] = GaugeMetricFamily(
                "_".join([self._prefix, "fsname_system", snake_case]),
                descriptions,
                labels=labels
            )

    def _setup_retrycache_labels(self):
        cache_flag = 1
        for metric in self._metrics['RetryCache']:
            if cache_flag:
                cache_flag = 0
                key = "cache"
                labels = ["cluster", "mode"]
                self._hdfs_namenode_metrics['RetryCache'][key] = GaugeMetricFamily(
                    "_".join([self._prefix, "cache_total"]),
                    "Total number of RetryCache in each mode",
                    labels=labels
                )
            else:
                continue

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for bean in beans:
            if 'NameNodeActivity' in bean['name']:
                self._setup_nnactivity_labels()

            if 'StartupProgress' in bean['name']:
                self._setup_startupprogress_labels()

            if 'FSNamesystem' in bean['name']:
                self._setup_fsnamesystem_labels()

            if 'FSNamesystemState' in bean['name']:
                self._setup_fsnamesystem_state_labels()

            if 'RetryCache' in bean['name']:
                self._setup_retrycache_labels()

    def _get_nnactivity_metrics(self, bean):
        for metric in self._metrics['NameNodeActivity']:
            if "NumOps" in metric:
                method = metric.split('NumOps')[0]
                labels = [self._cluster, method]
                key = "MethodNumOps"
            elif "AvgTime" in metric:
                method = metric.split('AvgTime')[0]
                labels = [self._cluster, method]
                key = "MethodAvgTime"
            else:
                if "Ops" in metric:
                    method = metric.split('Ops')[0]
                else:
                    method = metric
                labels = [self._cluster, method]
                key = "Operations"
            self._hdfs_namenode_metrics['NameNodeActivity'][key].add_metric(
                labels, bean[metric] if metric in bean else 0)

    def _get_startupprogress_metrics(self, bean):
        for metric in self._metrics['StartupProgress']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if "Count" in metric:
                key = "PhaseCount"
                phase = metric.split("Count")[0]
                labels = [self._cluster, phase]
            elif "ElapsedTime" in metric and "ElapsedTime" != metric:
                key = "PhaseElapsedTime"
                phase = metric.split("ElapsedTime")[0]
                labels = [self._cluster, phase]
            elif "Total" in metric:
                key = "PhaseTotal"
                phase = metric.split("Total")[0]
                labels = [self._cluster, phase]
            elif "PercentComplete" in metric and "PercentComplete" != metric:
                key = "PhasePercentComplete"
                phase = metric.split("PercentComplete")[0]
                labels = [self._cluster, phase]
            else:
                key = metric
                labels = [self._cluster]
            self._hdfs_namenode_metrics['StartupProgress'][key].add_metric(
                labels, bean[metric] if metric in bean else 0)

    def _get_fsnamesystem_metrics(self, bean):
        for metric in self._metrics['FSNamesystem']:
            key = metric
            if 'HAState' in metric:
                labels = [self._cluster]
                if 'initializing' == bean['tag.HAState']:
                    value = 0.0
                elif 'active' == bean['tag.HAState']:
                    value = 1.0
                elif 'standby' == bean['tag.HAState']:
                    value = 2.0
                elif 'stopping' == bean['tag.HAState']:
                    value = 3.0
                else:
                    value = 9999
                self._hdfs_namenode_metrics['FSNamesystem'][key].add_metric(
                    labels, value)
            elif metric.startswith("Capacity"):
                key = 'capacity'
                mode = metric.split("Capacity")[1]
                labels = [self._cluster, mode]
                self._hdfs_namenode_metrics['FSNamesystem'][key].add_metric(
                    labels, bean[metric] if metric in bean else 0)
            else:
                labels = [self._cluster]
                self._hdfs_namenode_metrics['FSNamesystem'][key].add_metric(
                    labels, bean[metric] if metric in bean else 0)

    def _get_fsnamesystem_state_metrics(self, bean):
        for metric in self._metrics['FSNamesystemState']:
            labels = [self._cluster]
            key = metric
            if 'FSState' in metric:
                if 'Safemode' == bean['FSState']:
                    value = 0.0
                elif 'Operational' == bean['FSState']:
                    value = 1.0
                else:
                    value = 9999
                self._hdfs_namenode_metrics['FSNamesystemState'][key].add_metric(
                    labels, value)
            elif "TotalSyncTimes" in metric:
                self._hdfs_namenode_metrics['FSNamesystemState'][key].add_metric(labels, float(
                    re.sub('\s', '', bean[metric])) if metric in bean and bean[metric] else 0)
            elif "DataNodes" in metric:
                key = 'datanodes_num'
                state = metric.split("DataNodes")[0].split("Num")[1]
                labels = [self._cluster, state]
                self._hdfs_namenode_metrics['FSNamesystemState'][key].add_metric(
                    labels, bean[metric] if metric in bean and bean[metric] else 0)

            else:
                self._hdfs_namenode_metrics['FSNamesystemState'][key].add_metric(
                    labels, bean[metric] if metric in bean and bean[metric] else 0)

    def _get_retrycache_metrics(self, bean):
        for metric in self._metrics['RetryCache']:
            key = "cache"
            labels = [self._cluster, metric.split('Cache')[1]]
            self._hdfs_namenode_metrics['RetryCache'][key].add_metric(
                labels, bean[metric] if metric in bean and bean[metric] else 0)

    def _get_metrics(self, beans):
        for bean in beans:
            if 'NameNodeActivity' in bean['name']:
                self._get_nnactivity_metrics(bean)
            if 'StartupProgress' in bean['name']:
                self._get_startupprogress_metrics(bean)
            if 'FSNamesystem' in bean['name'] and 'FSNamesystemState' not in bean['name']:
                self._get_fsnamesystem_metrics(bean)
            if 'FSNamesystemState' in bean['name']:
                self._get_fsnamesystem_state_metrics(bean)
            if 'RetryCache' in bean['name']:
                self._get_retrycache_metrics(bean)
