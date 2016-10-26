# Ambari Kafka Consumer Group Lag Alerting

Ambari Alerts triggered from Kafka Consumer Group Lag on topics

Works in both Kerberised and non-Kerberised environments (no SSL support).

Checks if a consumer group is registered for a given topics, and checks the lag of the consumer group on a topic is within a threshold.

## Alert definition

The alert takes four variables for configuration, and another optional variable for a fix (see below).

1. The whitelist pattern to filter Kafka topics you would like to be monitored.
2. The Zookeeper quorum through which the Kafka Brokers coordinate.
3. The consumer group you wish to monitor the lag of
4. The lag tolerance (number of messages begin on the topics) above which alerts will be generated.
5. (Optional) Kafka library distributed by Ambari to place on the front of the classpath. See below.

```javascript
      {
        "name": "topic_list",
        "display_name": "Topic Name list",
        "value": "my_topic1,my_topic2",
        "type": "STRING",
        "description": "Comma separated list of topics to monitor"
      },
      {
        "name": "zk_quorum",
        "display_name": "Zookeeper Quorum",
        "value": "zk1.cluster1.host:2181,zk2.cluster1.host:2181,zk3.cluster1.host:2181",
        "type": "STRING",
        "description": "Comma separated zookeeper addresses of cluster"
      },
      {
        "name": "consumer_group",
        "display_name": "Consumer Group to Monitor",
        "value": "my_consumer",
        "type": "STRING",
        "description": "The Consumer Group to monitor"
      },
      {
        "name": "lag_tolerance",
        "display_name": "Consumer group lag tolerance",
        "value": 100,
        "type": "STRING",
        "description": "The maximum number of messages the Consumer can lag behind on the topic"
      },
      {
      "name": "kafka_lib",
      "display_name": "Optional Kafka lib",
      "value": "kafka_2.10-0.9.1.0-SNAPSHOT.jar",
      "type": "STRING",
      "description": "Optional Kafka lib to prepend to classpath"
    }
```

## Kafka Kerberos Fix

Due to a Kerberos bug in Kafka [https://community.hortonworks.com/questions/27192/getting-kafka-consumer-offsets-getting-null-for-bo.html](https://community.hortonworks.com/questions/27192/getting-kafka-consumer-offsets-getting-null-for-bo.html) for HDP releases before July 2016 you will have to use a patched Kafka library to access offset information. Please copy the the Kafka jar [kafka_2.10-0.9.1.0-SNAPSHOT.jar](kafka_2.10-0.9.1.0-SNAPSHOT.jar) into the same location as the python script (`/var/lib/ambari-server/resources/host_scripts/`) and the alert will automatically add the library to the classpath when running alert commands.

## Alert installation

> Taken from [https://github.com/monolive/ambari-custom-alerts](https://github.com/monolive/ambari-custom-alerts)

Push the new alert via Ambari REST API.

```sh
curl -u admin:admin -i -H 'X-Requested-By: ambari' -X POST -d @alerts.json http://ambari.cloudapp.net:8080/api/v1/clusters/hdptest/alert_definitions
```
You will also need to copy the python script in /var/lib/ambari-server/resources/host_scripts and restart the ambari-server. After restart the script will be pushed in /var/lib/ambari-agent/cache/host_scripts on the different hosts.

You can find the ID of your alerts by running
```sh
curl -u admin:admin -i -H 'X-Requested-By: ambari' -X GET http://ambari.cloudapp.net:8080/api/v1/clusters/hdptest/alert_definitions
```

If we assume, that your alert is id 103. You can force the alert to run by
```sh
curl -u admin:admin -i -H 'X-Requested-By: ambari' -X PUT  http://ambari.cloudapp.net:8080/api/v1/clusters/hdptest/alert_definitions/103?run_now=true
```

## [License](LICENSE)

Copyright (c) 2016 Alex Bush.
Licensed under the [Apache License](LICENSE).
