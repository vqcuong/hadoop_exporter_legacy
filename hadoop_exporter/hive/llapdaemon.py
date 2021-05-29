#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from prometheus_client.core import GaugeMetricFamily

from hadoop_exporter import utils
from hadoop_exporter.common import MetricCollector, common_metrics_info


class HiveLlapDaemonMetricCollector(MetricCollector):
    COMPONENT = "hive"
    SERVICE = "llapdaemon"

    def __init__(self, cluster, url):
        MetricCollector.__init__(
            self, cluster, url, self.COMPONENT, self.SERVICE)
        self.logger = utils.get_logger(
            __name__, log_file=f"{self.COMPONENT}_{self.SERVICE}.log")
        self._hive_llapdaemon_metrics = {}
        for i in range(len(self._file_list)):
            self._hive_llapdaemon_metrics.setdefault(self._file_list[i], {})

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
            self._setup_labels(beans)

            # add metric value to every metric.
            self._get_metrics(beans)

            # update namenode metrics with common metrics
            common_metrics = common_metrics_info(
                self._cluster, beans, self.COMPONENT, self.SERVICE)
            self._hive_llapdaemon_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hive_llapdaemon_metrics[service]:
                    yield self._hive_llapdaemon_metrics[service][metric]

    def _setup_executor_labels(self, bean, service):
        for metric in self._metrics[service]:
            if metric in bean:
                if "ExecutorThread" in metric:
                    label = ['cluster', 'host', 'cpu']
                else:
                    label = ['cluster', 'host']
                name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()
                self._hive_llapdaemon_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, service.lower(), name]),
                                                                                   self._metrics[service][metric],
                                                                                   labels=label)
            else:
                continue

    def _setup_other_labels(self, bean, service):
        label = ["cluster", "host"]
        for metric in self._metrics[service]:
            if metric in bean:
                name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()
                self._hive_llapdaemon_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, service.lower(), name]),
                                                                                   self._metrics[service][metric],
                                                                                   labels=label)
            else:
                continue

    def _setup_labels(self, beans):
        # The metrics we want to export.
        for service in self._metrics:
            for bean in beans:
                if 'LlapDaemonExecutorMetrics' == service and 'LlapDaemonExecutorMetrics' in bean['name']:
                    self._setup_executor_labels(bean, service)
                elif service in bean['name']:
                    self._setup_other_labels(bean, service)
                else:
                    continue

    def _get_executor_metrics(self, bean, service, host):
        for metric in bean:
            if metric in self._metrics[service]:
                if "ExecutorThread" in metric:
                    cpu = "".join(['cpu', metric.split("_")[1]])
                    self._hive_llapdaemon_metrics[service][metric].add_metric(
                        [self._cluster, host, cpu], bean[metric])
                else:
                    self._hive_llapdaemon_metrics[service][metric].add_metric(
                        [self._cluster, host], bean[metric])
            else:
                continue

    def _get_other_metrics(self, bean, service, host):
        for metric in bean:
            if metric in self._metrics[service]:
                self._hive_llapdaemon_metrics[service][metric].add_metric(
                    [self._cluster, host], bean[metric])
            else:
                continue

    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>
        for bean in beans:
            if 'tag.Hostname' in bean:
                host = bean['tag.Hostname']
                break
            else:
                continue
        for bean in beans:
            for service in self._metrics:
                if 'LlapDaemonExecutorMetrics' == service and 'LlapDaemonExecutorMetrics' in bean['name']:
                    self._get_executor_metrics(bean, service, host)
                elif service in bean['name']:
                    self._get_other_metrics(bean, service, host)
                else:
                    continue
