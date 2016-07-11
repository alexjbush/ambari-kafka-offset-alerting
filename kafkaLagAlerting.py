#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
# Alert for Kafka Lag
#
# Alex Bush <abush@hortonworks.com>
#

import time
import urllib2
import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set.
import logging
import traceback
import subprocess
import datetime
import os.path
import re

from resource_management.libraries.functions.curl_krb_request import curl_krb_request
from resource_management.core.environment import Environment

TOPIC_PATTERN_KEY = 'topic_pattern'
ZK_KEY = 'zk_quorum'

CONSUMER_GROUP_KEY = 'consumer_group'
LAG_TOLERANCE_KEY = 'lag_tolerance'
PREPEND_KAFKA_LIB = 'kafka_lib'

KERBEROS_KEYTAB = '{{cluster-env/smokeuser_keytab}}'
KERBEROS_PRINCIPAL = '{{cluster-env/smokeuser_principal_name}}'
SECURITY_ENABLED_KEY = '{{cluster-env/security_enabled}}'
SMOKEUSER_KEY = "{{cluster-env/smokeuser}}"
EXECUTABLE_SEARCH_PATHS = '{{kerberos-env/executable_search_paths}}'

CONNECTION_TIMEOUT_KEY = 'connection.timeout'
CONNECTION_TIMEOUT_DEFAULT = 5.0

logger = logging.getLogger('ambari_alerts')

def get_tokens():
  """
  Returns a tuple of tokens in the format {{site/property}} that will be used
  to build the dictionary passed into execute
  """
  return ( EXECUTABLE_SEARCH_PATHS, KERBEROS_KEYTAB, KERBEROS_PRINCIPAL, SECURITY_ENABLED_KEY, SMOKEUSER_KEY)

def execute(configurations={}, parameters={}, host_name=None):
  """
  Returns a tuple containing the result code and a pre-formatted result label
  Keyword arguments:
  configurations (dictionary): a mapping of configuration key to value
  parameters (dictionary): a mapping of script parameter key to value
  host_name (string): the name of this host where the alert is running
  """

  if configurations is None:
    return (('UNKNOWN', ['There were no configurations supplied to the script.']))

  # Set configuration settings

  if SMOKEUSER_KEY in configurations:
    smokeuser = configurations[SMOKEUSER_KEY]

  executable_paths = None
  if EXECUTABLE_SEARCH_PATHS in configurations:
    executable_paths = configurations[EXECUTABLE_SEARCH_PATHS]

  security_enabled = False
  if SECURITY_ENABLED_KEY in configurations:
    security_enabled = str(configurations[SECURITY_ENABLED_KEY]).upper() == 'TRUE'

  kerberos_keytab = None
  if KERBEROS_KEYTAB in configurations:
    kerberos_keytab = configurations[KERBEROS_KEYTAB]

  kerberos_principal = None
  if KERBEROS_PRINCIPAL in configurations:
    kerberos_principal = configurations[KERBEROS_PRINCIPAL]
    kerberos_principal = kerberos_principal.replace('_HOST', host_name)

  # parse script arguments
  connection_timeout = CONNECTION_TIMEOUT_DEFAULT
  if CONNECTION_TIMEOUT_KEY in parameters:
    connection_timeout = float(parameters[CONNECTION_TIMEOUT_KEY])

  if TOPIC_PATTERN_KEY in parameters:
    topic_pattern = parameters[TOPIC_PATTERN_KEY]

  if ZK_KEY in parameters:
    zk_quorum = parameters[ZK_KEY]

  if CONSUMER_GROUP_KEY in parameters:
    consumer_group = parameters[CONSUMER_GROUP_KEY]

  if LAG_TOLERANCE_KEY in parameters:
    lag_tolerance = parameters[LAG_TOLERANCE_KEY]

  if PREPEND_KAFKA_LIB in parameters:
    kafka_lib = parameters[PREPEND_KAFKA_LIB]
  else:
    kafka_lib = None

  try:
    # Get list of topics
    command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper "+zk_quorum+" --list"
    topic_list = [ topic.strip() for topic in run_command(command).split("\n") if topic_pattern in topic ]
    # Check if we need to add fixed jar to classpath
    if kafka_lib:
      lib_path = "/var/lib/ambari-agent/cache/host_scripts/"+kafka_lib
      if not os.path.isfile(lib_path):
        raise Exception("Alert is configured to prepend lib to classpath, however jar not found: "+lib_path)
      base_command = "export CLASSPATH=\":"+lib_path+"\"; "
    else:
      base_command = ""
    #Kerberos for Kafka
    if security_enabled:
      base_command+="export KAFKA_CLIENT_KERBEROS_PARAMS=\"-Djava.security.auth.login.config=/etc/kafka/conf/kafka_client_jaas.conf\"; "
      base_command+='kinit -kt '+kerberos_keytab+' '+kerberos_principal+'; '
      # Get list of consumer groups
    command = base_command+"/usr/hdp/current/kafka-broker/bin/kafka-consumer-groups.sh --zookeeper "+zk_quorum+" --list"
    if not consumer_group in run_command(command).split("\n"):
      raise Exception("No consumer group found for ID: "+consumer_group)
    # Get offset per topic list for this ID
    command = base_command+"/usr/hdp/current/kafka-broker/bin/kafka-consumer-groups.sh --zookeeper "+zk_quorum+" --describe --group "+consumer_group+" --security-protocol PLAINTEXTSASL"
    output = run_command(command)
    expression = "^"+consumer_group+", +([^,]+), +([^,]+).* ([^,]+),[^,]+$"
    topic_lag = [ line.groups() for line in [re.match(expression,line) for line in output.split("\n") ] if line ]
    # Find topics that the consumer hasn't picked up
    missing = list(set(topic_list)-set([ tup[0] for tup in topic_lag ]))
    if missing:
      raise Exception("Consumer group "+consumer_group+" not connected to: "+','.join(missing))
    # Find topics that are further than the lag
    greater_than_lag = [ item for item in topic_lag if int(item[2]) > int(lag_tolerance) ]
    if greater_than_lag:
      raise Exception("Consumer group "+consumer_group+" breached lag threshold of "+lag_tolerance+". Topic partition->Lag: "+','.join([item[0]+' '+item[1]+'->'+item[2] for item in greater_than_lag]))
    # Return okay at this point
    return (('OK', ['All topics with pattern of '+topic_pattern+' in lag tolerance of '+lag_tolerance+' for consumer group '+consumer_group]))

  except:
    return (('CRITICAL', [traceback.format_exc()]))

#Utility function for calling commands on the CMD
def run_command(command):
  p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  (output, err) = p.communicate()
  if p.returncode:
    raise Exception('Command: '+command+' returned with non-zero code: '+str(p.returncode)+' stderr: '+err+' stdout: '+output)
  else:
    return output

#Wrapper for curl to make it security agnostic
def curl_request(url,connection_timeout,kerberos_keytab,security_enabled,kerberos_principal,executable_paths,user,tmp_dir='/tmp/'):
  # Kerberos curl
  if kerberos_principal is not None and kerberos_keytab is not None and security_enabled:
  # curl requires an integer timeout
    curl_connection_timeout = int(connection_timeout)
    summary_response, error_msg, time_millis = curl_krb_request('/tmp/', kerberos_keytab, kerberos_principal, url, "kafka_alert", executable_paths, False, "Kafka Mirrormaker Alert", user, connection_timeout=curl_connection_timeout)
  # Non-kerberos curl
  else:
    req = urllib2.Request(rest_api_request_summary)
    response = urllib2.urlopen(req)
    summary_response = response.read()
  return summary_response
