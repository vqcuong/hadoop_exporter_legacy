#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hadoop_exporter import utils
from hadoop_exporter.common import MetricCollector, common_metrics_info


class MapredJobHistoryMetricCollector(MetricCollector):
    COMPONENT = "mapred"
    SERVICE = "jobhistory"

    def __init__(self, cluster, url):
        MetricCollector.__init__(
            self, cluster, url, self.COMPONENT, self.SERVICE)
        self.logger = utils.get_logger(
            __name__, log_file=f"{self.COMPONENT}_{self.SERVICE}.log")
        self._mapred_jobhistory_metrics = {}
        # for i in range(len(self._file_list)):
        #     self._mapred_jobhistory_metrics.setdefault(self._file_list[i], {})

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
            # self._setup_metrics_labels()

            # add metric value to every metric.
            # self._get_metrics(self._beans)

            # update namenode metrics with common metrics
            common_metrics = common_metrics_info(
                self._cluster, beans, self.COMPONENT, self.SERVICE)
            self._mapred_jobhistory_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._mapred_jobhistory_metrics[service]:
                    yield self._mapred_jobhistory_metrics[service][metric]
