#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from prometheus_client.core import GaugeMetricFamily

from hadoop_exporter import utils
from hadoop_exporter.common import MetricCollector, common_metrics_info


class YARNNodeManagerMetricCollector(MetricCollector):
    COMPONENT = "yarn"
    SERVICE = "nodemanager"

    def __init__(self, cluster, url):
        MetricCollector.__init__(
            self, cluster, url, self.COMPONENT, self.SERVICE)
        self.logger = utils.get_logger(
            __name__, log_file=f"{self.COMPONENT}_{self.SERVICE}.log")
        self._yarn_nodemanager_metrics = {}
        for i in range(len(self._file_list)):
            self._yarn_nodemanager_metrics.setdefault(self._file_list[i], {})

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
            self._yarn_nodemanager_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._yarn_nodemanager_metrics[service]:
                    yield self._yarn_nodemanager_metrics[service][metric]

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for bean in beans:
            label = ["cluster", "host"]
            for service in self._metrics:
                if service in bean['name']:
                    for metric in self._metrics[service]:
                        name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()
                        self._yarn_nodemanager_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                            self._metrics[service][metric],
                                                                                            labels=label)
                else:
                    continue

    def _get_metrics(self, beans):
        for bean in beans:
            if 'tag.Hostname' in bean:
                host = bean['tag.Hostname']
                label = [self._cluster, host]
                break
            else:
                continue
        for bean in beans:
            for service in self._metrics:
                if service in bean['name']:
                    for metric in bean:
                        if metric in self._metrics[service]:
                            self._yarn_nodemanager_metrics[service][metric].add_metric(
                                label, bean[metric])
                        else:
                            pass
                else:
                    continue
