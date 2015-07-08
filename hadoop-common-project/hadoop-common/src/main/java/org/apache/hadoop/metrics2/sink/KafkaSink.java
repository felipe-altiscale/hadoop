/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.sink;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

/**
 * A metrics sink that writes to a Kafka broker.
 * This requires you to configure a broker_list and a topic
 * in the metrics2 configuration file. The broker_list must contain
 * a comma-separated list of kafka broker host and ports.
 * The topic will only contain 1 topic name.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KafkaSink implements MetricsSink, Closeable {
private static final Log LOG = LogFactory.getLog(KafkaSink.class);
static final String BROKER_LIST = "broker_list";
static final String TOPIC = "topic";

private String brokerList = null;
private String[] topics = null;
private Producer<Integer, byte[]> producer = null;

@Override
public void init(SubsetConfiguration conf) {
  Properties props = new Properties();
  brokerList = conf.getString(BROKER_LIST);
  LOG.info("Broker list " + brokerList);
  props.put("metadata.broker.list", brokerList);
  if (LOG.isDebugEnabled()) {
    LOG.debug("Kafka brokers: " + brokerList);
  }
  String topic = conf.getString(TOPIC);
  if (LOG.isDebugEnabled()) {
    LOG.debug("Kafka topic " + topic);
  }
  if (Strings.isNullOrEmpty(topic)) {
    throw new MetricsException("Kafka topic can not be null");
  }
  topics = topic.split(",");
  props.put("serializer.class", "kafka.serializer.DefaultEncoder");
  props.put("request.required.acks", "0");

  ProducerConfig config = new ProducerConfig(props);

  producer = new Producer<Integer, byte[]>(config);
}

@Override
public void putMetrics(MetricsRecord record) {
  if (producer == null) {
    throw new MetricsException("Producer in KafkaSink is null!");
  }
  StringBuilder jsonLines = new StringBuilder();
  String hostname = new String("null");
  try {
    hostname = InetAddress.getLocalHost().getHostName();
  } catch (Exception e) {
    LOG.warn("Error getting Hostname, going to continue");
  }
  Long timestamp = record.timestamp();
  Calendar cal = Calendar.getInstance();
  cal.setTimeInMillis(timestamp);
  String date = cal.get(Calendar.YEAR) + "-"
          + cal.get(Calendar.MONTH) + "-"
          + cal.get(Calendar.DAY_OF_MONTH);
  String time = cal.get(Calendar.HOUR_OF_DAY)
          + ":" + cal.get(Calendar.MINUTE)
          + ":" + cal.get(Calendar.SECOND);

  jsonLines.append("{\"hostname\": \"" + hostname);
  jsonLines.append("\", \"timestamp\": " + timestamp);
  jsonLines.append(", \"date\": \"" + date);
  jsonLines.append("\",\"time\": \"" + time);
  jsonLines.append("\",\"name\": \"" + record.name() + "\" ");
  for (MetricsTag tag : record.tags()) {
    jsonLines.append(", \""
            + tag.name().toString().replaceAll("[\\p{Cc}]", "") + "\": ");
    jsonLines.append(" \""
            + tag.value().toString() + "\"");
  }
  for (AbstractMetric metric : record.metrics()) {
    jsonLines.append(", \""
            + metric.name().toString().replaceAll("[\\p{Cc}]", "") + "\": ");
    jsonLines.append(" \""
            + metric.value().toString() + "\"");
  }
  jsonLines.append("}");
  if(LOG.isInfoEnabled()) {
    LOG.info("kafka message: " + jsonLines.toString());
  }
  List<KeyedMessage<Integer, byte[]>> data = Lists.newArrayList();
  for (String topic: topics) {
    KeyedMessage<Integer, byte[]> datum
            = new KeyedMessage<Integer, byte[]>(
            topic,
            jsonLines.toString().getBytes());
    data.add(datum);
  }
  producer.send(data);
  jsonLines.setLength(0);
}

@Override
public void flush() {
  LOG.warn("Kafka seems not to have any flush() mechanism!");
}

@Override
public void close() throws IOException {
  try {
    producer.close();
    producer = null;
  } catch (RuntimeException e) {
    throw new MetricsException("Error closing producer", e);
  }
}

void setProducer(Producer<Integer, byte[]> producer) {
  this.producer = producer;
}

}
