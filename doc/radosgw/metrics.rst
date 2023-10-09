=======
Metrics
=======

The Ceph Object Gateway uses :ref:`Perf Counters` to tracks metrics. The counters can be labeled (:ref:`Labeled Perf Counters`). When counters are labeled, they are stored in the Ceph Object Gateway specific caches.

These metrics can be sent to the time series database Prometheus to visualize a cluster wide view of usage data (ex: number of S3 put operations on a specific bucket) over time.

.. contents::

Op Metrics
==========

The following metrics related to S3 or Swift operations are tracked per Ceph Object Gateway.::

     "rgw_op": [
	    {
		"counters": {
		    "put_ops": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "Puts",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "put_b": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "Size of puts",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "put_initial_lat": {
			"type": 5,
			"metric_type": "gauge",
			"value_type": "real-integer-pair",
			"description": "Put latency",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "get_ops": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "Gets",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "get_b": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "Size of gets",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "get_initial_lat": {
			"type": 5,
			"metric_type": "gauge",
			"value_type": "real-integer-pair",
			"description": "Get latency",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "del_obj_ops": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "Delete objects",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "del_obj_bytes": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "Size of delete objects",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "del_obj_lat": {
			"type": 5,
			"metric_type": "gauge",
			"value_type": "real-integer-pair",
			"description": "Delete object latency",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "del_bucket_ops": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "Delete Buckets",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "del_bucket_lat": {
			"type": 5,
			"metric_type": "gauge",
			"value_type": "real-integer-pair",
			"description": "Delete bucket latency",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "copy_obj_ops": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "Copy objects",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "copy_obj_bytes": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "Size of copy objects",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "copy_obj_lat": {
			"type": 5,
			"metric_type": "gauge",
			"value_type": "real-integer-pair",
			"description": "Copy object latency",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "list_obj_ops": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "List objects",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "list_obj_lat": {
			"type": 5,
			"metric_type": "gauge",
			"value_type": "real-integer-pair",
			"description": "List objects latency",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "list_buckets_ops": {
			"type": 10,
			"metric_type": "counter",
			"value_type": "integer",
			"description": "List buckets",
			"nick": "",
			"priority": 5,
			"units": "none"
		    },
		    "list_buckets_lat": {
			"type": 5,
			"metric_type": "gauge",
			"value_type": "real-integer-pair",
			"description": "List buckets latency",
			"nick": "",
			"priority": 5,
			"units": "none"
		    }
		}
	    },
    	]

Ceph Object Gateway op metrics can be seen in the ``rgw_op`` section of the output of the ``counter dump`` command.::

    "rgw_op": [
        {
            "labels": {},
            "counters": {
                "put_ops": 2,
                "put_b": 5327,
                "put_initial_lat": {
                    "avgcount": 2,
                    "sum": 2.818064835,
                    "avgtime": 1.409032417
                },
                "get_ops": 5,
                "get_b": 5325,
                "get_initial_lat": {
                    "avgcount": 2,
                    "sum": 0.003000069,
                    "avgtime": 0.001500034
                },
                "del_obj_ops": 2,
                "del_obj_bytes": 5327,
                "del_obj_lat": {
                    "avgcount": 2,
                    "sum": 0.034000782,
                    "avgtime": 0.017000391
                },
                "del_bucket_ops": 1,
                "del_bucket_lat": {
                    "avgcount": 1,
                    "sum": 0.134003083,
                    "avgtime": 0.134003083
                },
                "copy_obj_ops": 1,
                "copy_obj_bytes": 5033,
                "copy_obj_lat": {
                    "avgcount": 1,
                    "sum": 0.024000553,
                    "avgtime": 0.024000553
                },
                "list_obj_ops": 1,
                "list_obj_lat": {
                    "avgcount": 1,
                    "sum": 0.004000092,
                    "avgtime": 0.004000092
                },
                "list_buckets_ops": 1,
                "list_buckets_lat": {
                    "avgcount": 1,
                    "sum": 0.002300000,
                    "avgtime": 0.002300000
                }
            }
        },
    ]

User & Bucket Labels
--------------------

Op metrics can be labeled with user names or bucket names, enabling cluster admins to see op metrics by bucket or by user.::

    "rgw_op": [
        ...
        {
            "labels": {
                "Bucket": "bucket1"
            },
            "counters": {
                "put_ops": 2,
                "put_b": 5327,
                "put_initial_lat": {
                    "avgcount": 2,
                    "sum": 2.818064835,
                    "avgtime": 1.409032417
                },
                "get_ops": 5,
                "get_b": 5325,
                "get_initial_lat": {
                    "avgcount": 2,
                    "sum": 0.003000069,
                    "avgtime": 0.001500034
                },
                ...
                "list_buckets_ops": 1,
                "list_buckets_lat": {
                    "avgcount": 1,
                    "sum": 0.002300000,
                    "avgtime": 0.002300000
                }
            }
        },
        ...
    ]

To enable op metrics to be labeled by user or bucket, a cache for the user or bucket counters must be enabled.

User & Bucket Counter Caches
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To track op metrics by user the Ceph Object Gateway the config value ``rgw_user_counters_cache`` must be set to ``true``. 

To track op metrics by bucket the Ceph Object Gateway the config value ``rgw_bucket_counters_cache`` must be set to ``true``. 

These config values are set in the Ceph configuration file under the ``[client.rgw.{instance-name}]`` section and must be set before the Ceph Object Gateway is started or restarted to take effect.

Since the op metrics are labeled perf counters, they live in memory. If the Ceph Object Gateway is restarted or crashes, all counters in the Ceph Object Gateway, whether in a cache or not, are lost.

User & Bucket Counter Cache Size & Eviction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Both ``rgw_user_counters_cache_size`` and ``rgw_bucket_counters_cache_size`` can be used to set number of entries in each cache.

Counters are evicted from a cache once the number of counters in the cache are greater than the cache size config variable. The counters that are evicted are the least recently used (LRU). 

For example if the number of buckets exceeded ``rgw_bucket_counters_cache_size`` by 1 and the counters with label ``bucket1`` were the last to be updated, the counters for ``bucket1`` would be evicted from the cache. If S3 operations tracked by the op metrics were done on ``bucket1`` after eviction, all of the metrics in the cache for ``bucket1`` would start at 0.

Cache sizing can depend on a number of factors. These factors include:

#. Number of users in the cluster
#. Number of buckets in the cluster
#. Memory usage of the Ceph Object Gateway
#. Disk and memory usage of Promtheus. 

To help calculate the Ceph Object Gateway's memory usage of a cache, it should be noted that each cache entry, encompassing all of the op metrics, is 1360 bytes. This is an estimate and subject to change if metrics are added or removed from the op metrics list.

Sending Metrics to Prometheus
=============================

To get metrics from a Ceph Object Gateway into the time series database Prometheus, the ceph-exporter daemon must be running and configured to scrape the Radogw's admin socket.::

The ceph-exporter daemon scrapes the Ceph Object Gateway's admin socket at a regular interval, defined by the config variable ``exporter_stats_period``.

Prometheus has a configurable interval in which it scrapes the exporter (see: https://prometheus.io/docs/prometheus/latest/configuration/configuration/).

Config Reference
================
The following rgw op metrics related settings can be added to the Ceph configuration file
(i.e., usually `ceph.conf`) under the ``[client.rgw.{instance-name}]`` section.

.. confval:: rgw_user_counters_cache
.. confval:: rgw_user_counters_cache_size
.. confval:: rgw_bucket_counters_cache
.. confval:: rgw_bucket_counters_cache_size

The following are notable ceph-exporter related settings that can be added under the ``[global]`` section of the Ceph configuration file.

.. confval:: exporter_stats_period
