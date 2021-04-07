# Rabbit FBI Elastic Indexer

This provides the code to read from the rabbit queue and process updates
to the files index.

This indexer uses a conda environment as it has requirements which require non-python
dependencies such as iris and netCDF4

Watched events:
- DEPOSIT/REMOVE

Exposed Queue Consumer Classes:
- `rabbit_fbi_elastic_indexer.queue_consumers.FastFBIQueueConsumer`
- `rabbit_fbi_elastic_indexer.queue_consumers.SlowFBIQueueConsumer`

## Configuration

Configuration is handled using a YAML file. The full configuration options 
are described in the [rabbit_indexer repo](https://github.com/cedadev/rabbit-index-ingest/blob/master/README.md#rabbit_event_indexer)

The required sections for the dbi indexer are:
- rabbit_server
- rabbit_indexer
- logging
- moles
- elasticsearch
- files_index

An example YAML file (secrets noted by ***** ): 

```yaml

---
rabbit_server:
  name: "*****"
  user: "*****"
  password: "*****"
  vhost: "*****"
  source_exchange:
    name: deposit_logs
    type: fanout
  dest_exchange:
    name: fbi_fanout
    type: fanout
  queues:
    - name: elasticsearch_update_queue_slow
      kwargs:
        auto_delete: false
indexer:
  queue_consumer_class: rabbit_fbi_elastic_indexer.queue_consumers.SlowFBSConsumer
logging:
  log_level: info
moles:
  moles_obs_map_url: http://api.catalogue.ceda.ac.uk/api/v2/observations.json/?publicationState__in=citable,published,preview,removed&fields=publicationState,result_field,title,uuid
elasticsearch:
  es_api_key: "*****"
files_index:
  name: ceda-fbi
  calculate_md5: false
  scan_level: 2
```

## Running

The indexer can be run using the helper script provided by [rabbit_indexer repo](https://github.com/cedadev/rabbit-index-ingest/blob/master/README.md#configuration).
This uses an entry script and parses the config file to run your selected queue_consumer_class: 

`rabbit_event_indexer --conf <path_to_configuration_file>`