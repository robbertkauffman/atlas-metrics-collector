# Gets metrics for all nodes of a set of Atlas clusters, filterable by org, 
# project & cluster. Metrics are obtained via the Atlas CLI.

import copy
import json
import numpy
import os
# import requests
import subprocess


CONFIG = {
  'ORG_ID': '64ca747b952bcb462e491b03',
  'TARGET_PROJECTS': [
    'my-atlas-project'
  ],
  'TARGET_CLUSTERS': [
    'my-atlas-cluster'
  ],
  'TARGET_NODETYPES': [
    'REPLICA_PRIMARY',
    'REPLICA_SECONDARY'
  ],
  'METRICS_PERIOD': 'PT24H',
  'METRICS_GRANULARITY': 'PT1M'
  # 'HEADERS': ''
}


CLI_LIST_PROJECTS = ['atlas', 'projects', 'list', '--orgId', '{}', '-o', 'json']
CLI_LIST_CLUSTERS = ['atlas', 'clusters', 'list', '--projectId', '{}', '-o', 'json']
CLI_LIST_PROCESSES = ['atlas', 'processes', 'list', '--projectId', '{}', '-o', 'json', '-c']
CLI_GET_PROCESS_METRICS = ['atlas', 'metrics', 'processes', '{}', '--projectId', '{}', '--period', CONFIG['METRICS_PERIOD'], '--granularity', CONFIG['METRICS_GRANULARITY'], '-o', 'json']
CLI_GET_DISK_METRICS = ['atlas', 'metrics', 'disks', 'describe', '{}', 'data', '--projectId', '{}', '--period', CONFIG['METRICS_PERIOD'], '--granularity', CONFIG['METRICS_GRANULARITY'], '-o', 'json']
FILE_PATH = 'saved-responses/{}-{}.json'
ORG_PREFIX = 'org'
PROJECT_PREFIX = 'project'
CLUSTER_PREFIX = 'cluster'
INSTANCES_PREFIX = 'instances'
PROCESSES_PREFIX = 'processes'
PROCESS_METRICS_PREFIX = 'process-metrics'
DISK_METRICS_PREFIX = 'disk-metrics'

PROCESS_METRICS_NAMES = [
  # "ASSERT_REGULAR",
  # "ASSERT_WARNING",
  # "ASSERT_MSG",
  # "ASSERT_USER",
  # "CACHE_BYTES_READ_INTO",
  # "CACHE_BYTES_WRITTEN_FROM",
  # "CACHE_DIRTY_BYTES",
  # "CACHE_USED_BYTES",
  # "CONNECTIONS",
  # "CURSORS_TOTAL_OPEN",
  # "CURSORS_TOTAL_TIMED_OUT",
  "DB_STORAGE_TOTAL",
  # "DB_DATA_SIZE_TOTAL",
  "DB_DATA_SIZE_TOTAL_WO_SYSTEM",
  "DB_INDEX_SIZE_TOTAL",
  "DOCUMENT_METRICS_RETURNED",
  "DOCUMENT_METRICS_INSERTED",
  "DOCUMENT_METRICS_UPDATED",
  "DOCUMENT_METRICS_DELETED",
  # "EXTRA_INFO_PAGE_FAULTS",
  # "GLOBAL_LOCK_CURRENT_QUEUE_TOTAL",
  # "GLOBAL_LOCK_CURRENT_QUEUE_READERS",
  # "GLOBAL_LOCK_CURRENT_QUEUE_WRITERS",
  # "MEMORY_RESIDENT",
  # "MEMORY_VIRTUAL",
  # "MEMORY_MAPPED",
  # "NETWORK_BYTES_IN",
  # "NETWORK_BYTES_OUT",
  # "NETWORK_NUM_REQUESTS",
  "OPCOUNTER_CMD",
  "OPCOUNTER_QUERY",
  "OPCOUNTER_UPDATE",
  "OPCOUNTER_DELETE",
  # "OPCOUNTER_GETMORE",
  "OPCOUNTER_INSERT",
  # "OPCOUNTER_REPL_CMD",
  # "OPCOUNTER_REPL_UPDATE",
  # "OPCOUNTER_REPL_DELETE",
  # "OPCOUNTER_REPL_INSERT",
  # "OPERATIONS_SCAN_AND_ORDER",
  # "OP_EXECUTION_TIME_READS",
  # "OP_EXECUTION_TIME_WRITES",
  # "OP_EXECUTION_TIME_COMMANDS",
  # "OPLOG_SLAVE_LAG_MASTER_TIME",
  # "OPLOG_REPLICATION_LAG_TIME",
  # "OPLOG_MASTER_TIME",
  # "OPLOG_MASTER_LAG_TIME_DIFF",
  # "OPLOG_RATE_GB_PER_HOUR",
  "QUERY_EXECUTOR_SCANNED",
  "QUERY_EXECUTOR_SCANNED_OBJECTS",
  # "QUERY_TARGETING_SCANNED_PER_RETURNED",
  # "QUERY_TARGETING_SCANNED_OBJECTS_PER_RETURNED",
  # "TICKETS_AVAILABLE_READS",
  # "TICKETS_AVAILABLE_WRITE",
  # "PROCESS_CPU_USER",
  # "PROCESS_CPU_KERNEL",
  # "PROCESS_CPU_CHILDREN_USER",
  # "PROCESS_CPU_CHILDREN_KERNEL",
  # "PROCESS_NORMALIZED_CPU_USER",
  # "PROCESS_NORMALIZED_CPU_KERNEL",
  # "PROCESS_NORMALIZED_CPU_CHILDREN_USER",
  # "PROCESS_NORMALIZED_CPU_CHILDREN_KERNEL",
  # "MAX_PROCESS_CPU_USER",
  # "MAX_PROCESS_CPU_KERNEL",
  # "MAX_PROCESS_CPU_CHILDREN_USER",
  # "MAX_PROCESS_CPU_CHILDREN_KERNEL",
  # "MAX_PROCESS_NORMALIZED_CPU_USER",
  # "MAX_PROCESS_NORMALIZED_CPU_KERNEL",
  # "MAX_PROCESS_NORMALIZED_CPU_CHILDREN_USER",
  # "MAX_PROCESS_NORMALIZED_CPU_CHILDREN_KERNEL",
  # "FTS_PROCESS_RESIDENT_MEMORY",
  # "FTS_PROCESS_VIRTUAL_MEMORY",
  # "FTS_PROCESS_SHARED_MEMORY",
  # "FTS_DISK_USAGE",
  # "FTS_PROCESS_CPU_USER",
  # "FTS_PROCESS_CPU_KERNEL",
  # "FTS_PROCESS_NORMALIZED_CPU_USER",
  # "FTS_PROCESS_NORMALIZED_CPU_KERNEL",
  # "SYSTEM_CPU_USER",
  # "SYSTEM_CPU_KERNEL",
  # "SYSTEM_CPU_NICE",
  # "SYSTEM_CPU_IOWAIT",
  # "SYSTEM_CPU_IRQ",
  # "SYSTEM_CPU_SOFTIRQ",
  # "SYSTEM_CPU_GUEST",
  # "SYSTEM_CPU_STEAL",
  "SYSTEM_NORMALIZED_CPU_USER",
  "SYSTEM_NORMALIZED_CPU_KERNEL",
  "SYSTEM_NORMALIZED_CPU_NICE",
  "SYSTEM_NORMALIZED_CPU_IOWAIT",
  "SYSTEM_NORMALIZED_CPU_IRQ",
  "SYSTEM_NORMALIZED_CPU_SOFTIRQ",
  "SYSTEM_NORMALIZED_CPU_GUEST",
  "SYSTEM_NORMALIZED_CPU_STEAL",
  # "SYSTEM_NETWORK_IN",
  # "SYSTEM_NETWORK_OUT",
  # "SWAP_IO_IN",
  # "SWAP_IO_OUT",
  # "MAX_SYSTEM_CPU_USER",
  # "MAX_SYSTEM_CPU_KERNEL",
  # "MAX_SYSTEM_CPU_NICE",
  # "MAX_SYSTEM_CPU_IOWAIT",
  # "MAX_SYSTEM_CPU_IRQ",
  # "MAX_SYSTEM_CPU_SOFTIRQ",
  # "MAX_SYSTEM_CPU_GUEST",
  # "MAX_SYSTEM_CPU_STEAL",
  "MAX_SYSTEM_NORMALIZED_CPU_USER",
  "MAX_SYSTEM_NORMALIZED_CPU_KERNEL",
  "MAX_SYSTEM_NORMALIZED_CPU_NICE",
  "MAX_SYSTEM_NORMALIZED_CPU_IOWAIT",
  "MAX_SYSTEM_NORMALIZED_CPU_IRQ",
  "MAX_SYSTEM_NORMALIZED_CPU_SOFTIRQ",
  "MAX_SYSTEM_NORMALIZED_CPU_GUEST",
  "MAX_SYSTEM_NORMALIZED_CPU_STEAL",
  # "MAX_SYSTEM_NETWORK_IN",
  # "MAX_SYSTEM_NETWORK_OUT",
  # "MAX_SWAP_IO_IN",
  # "MAX_SWAP_IO_OUT",
  # "SYSTEM_MEMORY_USED",
  # "SYSTEM_MEMORY_AVAILABLE",
  # "SYSTEM_MEMORY_FREE",
  # "SYSTEM_MEMORY_SHARED",
  # "SYSTEM_MEMORY_CACHED",
  # "SYSTEM_MEMORY_BUFFERS",
  # "SWAP_USAGE_USED",
  # "SWAP_USAGE_FREE",
  # "MAX_SYSTEM_MEMORY_USED",
  # "MAX_SYSTEM_MEMORY_AVAILABLE",
  # "MAX_SYSTEM_MEMORY_FREE",
  # "MAX_SYSTEM_MEMORY_SHARED",
  # "MAX_SYSTEM_MEMORY_CACHED",
  # "MAX_SYSTEM_MEMORY_BUFFERS",
  # "MAX_SWAP_USAGE_USED",
  # "MAX_SWAP_USAGE_FREE",
]
DISK_METRICS_NAMES = [
  # "DISK_PARTITION_LATENCY_READ",
  # "DISK_PARTITION_LATENCY_WRITE",
  # "DISK_PARTITION_SPACE_FREE",
  "DISK_PARTITION_SPACE_USED",
  # "DISK_PARTITION_SPACE_PERCENT_FREE",
  # "DISK_PARTITION_SPACE_PERCENT_USED",
  "DISK_PARTITION_IOPS_READ",
  "DISK_PARTITION_IOPS_WRITE",
  "DISK_PARTITION_IOPS_TOTAL",
  "MAX_DISK_PARTITION_IOPS_READ",
  "MAX_DISK_PARTITION_IOPS_WRITE",
  "MAX_DISK_PARTITION_IOPS_TOTAL",
  # "MAX_DISK_PARTITION_LATENCY_READ",
  # "MAX_DISK_PARTITION_LATENCY_WRITE",
  # "MAX_DISK_PARTITION_SPACE_FREE",
  # "MAX_DISK_PARTITION_SPACE_USED",
  # "MAX_DISK_PARTITION_SPACE_PERCENT_FREE",
  # "MAX_DISK_PARTITION_SPACE_PERCENT_USED"
]
SUM_CPU_METRICS_NAMES = [
  "SUM_AVG_CPU_METRICS",
  "SUM_MAX_CPU_METRICS"
]
SUM_AVG_CPU_METRICS = [
  "SYSTEM_NORMALIZED_CPU_USER",
  "SYSTEM_NORMALIZED_CPU_KERNEL",
  "SYSTEM_NORMALIZED_CPU_NICE",
  "SYSTEM_NORMALIZED_CPU_IOWAIT",
  "SYSTEM_NORMALIZED_CPU_IRQ",
  "SYSTEM_NORMALIZED_CPU_SOFTIRQ",
  "SYSTEM_NORMALIZED_CPU_GUEST",
  "SYSTEM_NORMALIZED_CPU_STEAL",
]
SUM_MAX_CPU_METRICS = [
  "MAX_SYSTEM_NORMALIZED_CPU_USER",
  "MAX_SYSTEM_NORMALIZED_CPU_KERNEL",
  "MAX_SYSTEM_NORMALIZED_CPU_NICE",
  "MAX_SYSTEM_NORMALIZED_CPU_IOWAIT",
  "MAX_SYSTEM_NORMALIZED_CPU_IRQ",
  "MAX_SYSTEM_NORMALIZED_CPU_SOFTIRQ",
  "MAX_SYSTEM_NORMALIZED_CPU_GUEST",
  "MAX_SYSTEM_NORMALIZED_CPU_STEAL"
]
CONVERT_TO_GB_METRICS = [
  "CACHE_BYTES_READ_INTO",
  "CACHE_BYTES_WRITTEN_FROM",
  "CACHE_DIRTY_BYTES",
  "CACHE_USED_BYTES",
  "DB_STORAGE_TOTAL",
  "DB_DATA_SIZE_TOTAL",
  "DB_DATA_SIZE_TOTAL_WO_SYSTEM",
  "DB_INDEX_SIZE_TOTAL",
  "MEMORY_RESIDENT",
  "MEMORY_VIRTUAL",
  "MEMORY_MAPPED",
  "FTS_PROCESS_RESIDENT_MEMORY",
  "FTS_PROCESS_VIRTUAL_MEMORY",
  "FTS_PROCESS_SHARED_MEMORY",
  "FTS_DISK_USAGE",
  "SYSTEM_MEMORY_USED",
  "SYSTEM_MEMORY_AVAILABLE",
  "SYSTEM_MEMORY_FREE",
  "SYSTEM_MEMORY_SHARED",
  "SYSTEM_MEMORY_CACHED",
  "SYSTEM_MEMORY_BUFFERS",
  "SWAP_USAGE_USED",
  "SWAP_USAGE_FREE",
  "MAX_SYSTEM_MEMORY_USED",
  "MAX_SYSTEM_MEMORY_AVAILABLE",
  "MAX_SYSTEM_MEMORY_FREE",
  "MAX_SYSTEM_MEMORY_SHARED",
  "MAX_SYSTEM_MEMORY_CACHED",
  "MAX_SYSTEM_MEMORY_BUFFERS",
  "MAX_SWAP_USAGE_USED",
  "MAX_SWAP_USAGE_FREE"
]
LAST_VALUE_METRICS = [
  "DB_STORAGE_TOTAL",
  "DB_DATA_SIZE_TOTAL_WO_SYSTEM",
  "DB_INDEX_SIZE_TOTAL",
  "DISK_PARTITION_SPACE_USED"
]
SKIP_METRICS = [
  "SYSTEM_NORMALIZED_CPU_KERNEL",
  "SYSTEM_NORMALIZED_CPU_NICE",
  "SYSTEM_NORMALIZED_CPU_IOWAIT",
  "SYSTEM_NORMALIZED_CPU_IRQ",
  "SYSTEM_NORMALIZED_CPU_SOFTIRQ",
  "SYSTEM_NORMALIZED_CPU_GUEST",
  "MAX_SYSTEM_NORMALIZED_CPU_KERNEL",
  "MAX_SYSTEM_NORMALIZED_CPU_NICE",
  "MAX_SYSTEM_NORMALIZED_CPU_IOWAIT",
  "MAX_SYSTEM_NORMALIZED_CPU_IRQ",
  "MAX_SYSTEM_NORMALIZED_CPU_SOFTIRQ",
  "MAX_SYSTEM_NORMALIZED_CPU_GUEST"
]


all_metrics_names = PROCESS_METRICS_NAMES
all_metrics_names.extend(SUM_CPU_METRICS_NAMES)
all_metrics_names.extend(DISK_METRICS_NAMES)


def write_response(file_path, response_json):
  parent_folder = file_path.split('/')[0]
  if not os.path.exists(parent_folder):
    os.makedirs(parent_folder)
  with open(file_path, 'w') as fp:
    json.dump(response_json, fp)


def replace_args(command, args):
  args_counter = 0
  for i, arg in enumerate(command):
    if arg == "{}":
      command[i] = args[args_counter]
      args_counter += 1
  return command


# def call_atlas_api(url, args, file_path):
#   url = url.format(*args)
#   api_response = requests.get(url, headers=HEADERS)
#   if api_response.status_code == requests.codes.ok:
#     api_response_json = api_response.json()

#     write_response(file_path, api_response_json)
    
#     return api_response_json
#   else:
#     return {}


def run_cli_command(command, args, file_path):
  command = replace_args(copy.deepcopy(command), args)
  command_response = subprocess.run(command, stdout=subprocess.PIPE)
  if command_response.returncode == 0:
    command_response_json = json.loads(command_response.stdout)

    write_response(file_path, command_response_json)
    return command_response_json
  else:
    return {}


def get_cached_response_or_get_data(action, target, prefix, args):
  file_path = FILE_PATH.format(prefix, args[0])
  if not os.path.exists(file_path):
    if action == 'RUN_CLI_COMMAND':
      return run_cli_command(target, args, file_path)
    # elif action == 'CALL_ATLAS_API':
    #   return call_atlas_api(target, args, file_path)
  else:
    with open(file_path, 'r') as fp:
      return json.load(fp)


def get_metric_data_by_name(metric_name, metrics_data):
  return next(filter(lambda metric: metric['name'] == metric_name, metrics_data['measurements']), None)


def get_metric_aggregations(metric_name, metrics_data):
  metric_data = get_metric_data_by_name(metric_name, metrics_data)
  if metric_data and 'dataPoints' in metric_data:
    vals = [val['value'] for val in metric_data['dataPoints'] if 'value' in val]
    if len(vals) > 0:
      # convert certain metrics from bytes to GBs
      if (metric_name in CONVERT_TO_GB_METRICS):
        return round(vals[-1] / 1024 / 1024 / 1024)
      # show sparkline (mini-chart) for CPU metrics
      elif (metric_name in SUM_AVG_CPU_METRICS or metric_name in SUM_MAX_CPU_METRICS or metric_name in SUM_CPU_METRICS_NAMES):
        vals = vals[:-(len(vals) % 100)]
        reduced_vals = numpy.array(vals).reshape(-1, 100).max(axis=1)
        return ('=SPARKLINE({%s},{"min",0;"max",100})' % list2str(reduced_vals, ','))
      # avg, P80, P95 and max for all other metrics
      else:
        avg_val = round(numpy.average(vals))
        p80_val = round(numpy.percentile(vals, 80, method='nearest'))
        p95_val = round(numpy.percentile(vals, 95, method='nearest'))
        max_val = round(max(vals))
        return f"{avg_val} {p80_val} {p95_val} {max_val}"
  else:
    return ''


# CPU metrics are broken down into several metrics by Atlas. Calculate sum to get overall utilization
def sum_cpu_metrics(sum_metric_name, cpu_metric_names, all_metrics_data):
  cpu_metrics_data = copy.deepcopy(get_metric_data_by_name(cpu_metric_names[0], all_metrics_data))
  cpu_metrics_data['name'] = sum_metric_name
  for cpu_metric_name in cpu_metric_names[1:]:
    next_cpu_metric_data = get_metric_data_by_name(cpu_metric_name, all_metrics_data)
    for i, data_point in enumerate(cpu_metrics_data['dataPoints']):
      if 'value' in data_point:
        cpu_metrics_data['dataPoints'][i]['value'] += next_cpu_metric_data['dataPoints'][i]['value']
  all_metrics_data['measurements'].append(cpu_metrics_data)


def list2str(list, delimiter=' ', values_per_metric=[]):
  out_str = ''
  for i, elm in enumerate(list):
    if i < len(list) and elm not in SKIP_METRICS:
      if len(values_per_metric) > 1 and (elm not in LAST_VALUE_METRICS and elm not in SUM_AVG_CPU_METRICS and elm not in SUM_MAX_CPU_METRICS and elm not in SUM_CPU_METRICS_NAMES):
        # most metrics have multiple values (avg, p95, max), add description and a delimiter for each
        delimiter_str = ''
        for value in values_per_metric:
          delimiter_str += value + delimiter
      else:
        delimiter_str = delimiter
      out_str += str(elm) + (delimiter_str if i < len(list) - 1 else '')
  return out_str


def get_node_metrics(project_name, cluster_name, process_name, process_metrics_data, disk_metrics_data):
  sum_cpu_metrics('SUM_AVG_CPU_METRICS', SUM_AVG_CPU_METRICS, process_metrics_data)
  sum_cpu_metrics('SUM_MAX_CPU_METRICS', SUM_MAX_CPU_METRICS, process_metrics_data)
 
  # combine process & disk metrics data
  all_metrics_data = process_metrics_data
  if disk_metrics_data and 'measurements' in disk_metrics_data:
    all_metrics_data['measurements'].extend(disk_metrics_data['measurements'])
  
  metrics = [process_name, project_name, cluster_name]
  for metric_name in all_metrics_names:
    if metric_name not in SKIP_METRICS:
      metrics.append(get_metric_aggregations(metric_name, all_metrics_data))
  print(list2str(metrics))


def get_metrics_for_org(org_id):
  # print metric names
  print('Process Project Cluster ' + list2str(all_metrics_names, ' ', ['-avg', 'p80', 'p95', 'max']))
  
  projects = get_cached_response_or_get_data('RUN_CLI_COMMAND', CLI_LIST_PROJECTS, ORG_PREFIX, [org_id])
  for project in projects['results']:
    if project['name'] in CONFIG['TARGET_PROJECTS']:
      processes = get_cached_response_or_get_data('RUN_CLI_COMMAND', CLI_LIST_PROCESSES, PROCESSES_PREFIX, [project['id']])
      clusters = get_cached_response_or_get_data('RUN_CLI_COMMAND', CLI_LIST_CLUSTERS, PROJECT_PREFIX, [project['id']])
      for cluster in clusters['results']:
        if cluster['name'] in CONFIG['TARGET_CLUSTERS']:
          for process in processes:
            if process['typeName'] in CONFIG['TARGET_NODETYPES']:
              hostname = process['userAlias']
              process_metrics_data = get_cached_response_or_get_data('RUN_CLI_COMMAND', CLI_GET_PROCESS_METRICS, PROCESS_METRICS_PREFIX, [process['id'], project['id']])
              disk_metrics_data = get_cached_response_or_get_data('RUN_CLI_COMMAND', CLI_GET_DISK_METRICS, DISK_METRICS_PREFIX, [process['id'], project['id']])

              get_node_metrics(project['name'], cluster['name'], hostname, process_metrics_data, disk_metrics_data)


get_metrics_for_org(CONFIG['ORG_ID'])