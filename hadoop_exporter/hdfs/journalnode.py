#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from prometheus_client.core import GaugeMetricFamily, HistogramMetricFamily

from hadoop_exporter import utils
from hadoop_exporter.common import MetricCollector, common_metrics_info


class HDFSJournalNodeMetricCollector(MetricCollector):
    COMPONENT = "hdfs"
    SERVICE = "journalnode"

    def __init__(self, cluster, url):
        MetricCollector.__init__(
            self, cluster, url, self.COMPONENT, self.SERVICE)
        self.logger = utils.get_logger(
            __name__, log_file=f"{self.COMPONENT}_{self.SERVICE}.log")
        self._hdfs_journalnode_metrics = {}
        for i in range(len(self._file_list)):
            self._hdfs_journalnode_metrics.setdefault(self._file_list[i], {})

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
            self._hdfs_journalnode_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hdfs_journalnode_metrics[service]:
                    yield self._hdfs_journalnode_metrics[service][metric]

    def _setup_journalprod_labels(self):
        prod_num_flag, a_60_latency_flag, a_300_latency_flag, a_3600_latency_flag = 1, 1, 1, 1
        for metric in self._metrics['Journal-prod']:
            label = ["cluster", "host"]
            if 'Syncs60s' in metric:
                if a_60_latency_flag:
                    a_60_latency_flag = 0
                    key = "Syncs60"
                    name = "_".join(
                        [self._prefix, 'sync60s_latency_microseconds'])
                    descriptions = "The percentile of sync latency in microseconds in 60s granularity"
                    self._hdfs_journalnode_metrics['Journal-prod'][key] = HistogramMetricFamily(name,
                                                                                                descriptions,
                                                                                                labels=label)
                else:
                    continue
            elif 'Syncs300s' in metric:
                if a_300_latency_flag:
                    a_300_latency_flag = 0
                    key = "Syncs300"
                    name = "_".join(
                        [self._prefix, 'sync300s_latency_microseconds'])
                    descriptions = "The percentile of sync latency in microseconds in 300s granularity"
                    self._hdfs_journalnode_metrics['Journal-prod'][key] = HistogramMetricFamily(name,
                                                                                                descriptions,
                                                                                                labels=label)
                else:
                    continue
            elif 'Syncs3600s' in metric:
                if a_3600_latency_flag:
                    a_3600_latency_flag = 0
                    key = "Syncs3600"
                    name = "_".join(
                        [self._prefix, 'sync3600s_latency_microseconds'])
                    descriptions = "The percentile of sync latency in microseconds in 3600s granularity"
                    self._hdfs_journalnode_metrics['Journal-prod'][key] = HistogramMetricFamily(name,
                                                                                                descriptions,
                                                                                                labels=label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])',
                                    r'\1_\2', metric).lower()
                self._hdfs_journalnode_metrics['Journal-prod'][metric] = GaugeMetricFamily("_".join([self._prefix, snake_case]),
                                                                                           self._metrics['Journal-prod'][metric],
                                                                                           labels=label)

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for bean in beans:
            if 'Journal-prod' in bean['name']:
                self._setup_journalprod_labels()

    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>

        for bean in beans:
            if 'Journal-prod' in bean['name']:
                if 'Journal-prod' in self._metrics:
                    host = bean['tag.Hostname']
                    label = [self._cluster, host]

                    a_60_sum, a_300_sum, a_3600_sum = 0.0, 0.0, 0.0
                    a_60_value, a_300_value, a_3600_value = [], [], []
                    a_60_percentile, a_300_percentile, a_3600_percentile = [], [], []

                    for metric in bean:
                        if metric[0].isupper():
                            '''
                            different sync times corresponding to the same percentile
                            for instance:
                                sync = 60, percentile can be [50, 75, 95, 99]
                                sync = 300, percentile still can be [50, 75, 95, 99]
                            Therefore, here is the method to distinguish these metrics from each sync times.
                            '''
                            if "Syncs60s" in metric:
                                if 'NumOps' in metric:
                                    a_60_count = bean[metric]
                                else:
                                    tmp = metric.split("thPercentileLatencyMicros")[
                                        0].split("Syncs")[1].split("s")
                                    a_60_percentile.append(
                                        str(float(tmp[1]) / 100.0))
                                    a_60_value.append(bean[metric])
                                    a_60_sum += bean[metric]
                            elif 'Syncs300' in metric:
                                if 'NumOps' in metric:
                                    a_300_count = bean[metric]
                                else:
                                    tmp = metric.split("thPercentileLatencyMicros")[
                                        0].split("Syncs")[1].split("s")
                                    a_300_percentile.append(
                                        str(float(tmp[1]) / 100.0))
                                    a_300_value.append(bean[metric])
                                    a_300_sum += bean[metric]
                            elif 'Syncs3600' in metric:
                                if 'NumOps' in metric:
                                    a_3600_count = bean[metric]
                                else:
                                    tmp = metric.split("thPercentileLatencyMicros")[
                                        0].split("Syncs")[1].split("s")
                                    a_3600_percentile.append(
                                        str(float(tmp[1]) / 100.0))
                                    a_3600_value.append(bean[metric])
                                    a_3600_sum += bean[metric]
                            else:
                                key = metric
                                self._hdfs_journalnode_metrics['Journal-prod'][key].add_metric(
                                    label, bean[metric])
                    a_60_bucket = zip(a_60_percentile, a_60_value)
                    a_300_bucket = zip(a_300_percentile, a_300_value)
                    a_3600_bucket = zip(a_3600_percentile, a_3600_value)
                    a_60_bucket.sort()
                    a_300_bucket.sort()
                    a_3600_bucket.sort()
                    a_60_bucket.append(("+Inf", a_60_count))
                    a_300_bucket.append(("+Inf", a_300_count))
                    a_3600_bucket.append(("+Inf", a_3600_count))
                    self._hdfs_journalnode_metrics['Journal-prod']['Syncs60'].add_metric(
                        label, buckets=a_60_bucket, sum_value=a_60_sum)
                    self._hdfs_journalnode_metrics['Journal-prod']['Syncs300'].add_metric(
                        label, buckets=a_300_bucket, sum_value=a_300_sum)
                    self._hdfs_journalnode_metrics['Journal-prod']['Syncs3600'].add_metric(
                        label, buckets=a_3600_bucket, sum_value=a_3600_sum)
