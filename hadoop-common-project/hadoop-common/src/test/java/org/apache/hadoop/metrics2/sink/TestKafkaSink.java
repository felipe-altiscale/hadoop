/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.sink;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.MetricsVisitor;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metric.Type;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.impl.TestMetricsConfig;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.sink.ganglia.GangliaSink30;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
public class TestKafkaSink {
private static final Log LOG = LogFactory.getLog(TestKafkaSink.class);
enum KafkaMetricsInfo implements MetricsInfo {
  KafkaMetrics("Kafka related metrics etc."),
  KafkaCounter("Kafka counter."),
  KafkaTag("Kafka tag.");
  // metrics


  private final String desc;

  KafkaMetricsInfo(String desc) { this.desc = desc; }

  @Override public String description() { return desc; }

  @Override public String toString() {
    return Objects.toStringHelper(this)
            .add("name", name()).add("description", desc)
            .toString();
  }
}

@Test
public void testPutMetrics() throws Exception {
  MetricsRecord record = mock(MetricsRecord.class);
  when(record.tags()).thenReturn(Lists.newArrayList(
          new MetricsTag(KafkaMetricsInfo.KafkaTag, "test_tag")));
  when(record.timestamp()).thenReturn(System.currentTimeMillis());
  AbstractMetric metric = new AbstractMetric(KafkaMetricsInfo.KafkaCounter) {
    @Override
    public Number value() {
      return new Integer(123);
    }

    @Override
    public MetricType type() {
      return null;
    }

    @Override
    public void visit(MetricsVisitor visitor) {

    }
  };
  Iterable<AbstractMetric> metrics = Lists.newArrayList(metric);
  when(record.name()).thenReturn("Kafka record name");
  when(record.metrics()).thenReturn(metrics);
  SubsetConfiguration conf = mock(SubsetConfiguration.class);
  when(conf.getString(KafkaSink.BROKER_LIST)).thenReturn("localhost:9092");
  when(conf.getString(KafkaSink.TOPIC)).thenReturn("myTestKafkaTopic");
  KafkaSink kafkaSink = new KafkaSink();
  kafkaSink.init(conf);
  Producer<Integer, byte[]> mockProducer = mock(Producer.class);
  kafkaSink.setProducer(mockProducer);
  kafkaSink.putMetrics(record);
  StringBuilder jsonLines =  new StringBuilder();
  Long timestamp = record.timestamp();
  Calendar cal = Calendar.getInstance();
  cal.setTimeInMillis(timestamp);
  String date = cal.get(Calendar.YEAR) + "-"
          + cal.get(Calendar.MONTH) + "-"
          + cal.get(Calendar.DAY_OF_MONTH);
  String time = cal.get(Calendar.HOUR_OF_DAY)
          + ":" + cal.get(Calendar.MINUTE)
          + ":" + cal.get(Calendar.SECOND);
  String hostname = new String("null");
  try {
    hostname = InetAddress.getLocalHost().getHostName();
  } catch (Exception e) {
    LOG.warn("Error getting Hostname, going to continue");
  }
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
  for (AbstractMetric m : record.metrics()) {
    jsonLines.append(", \""
            + m.name().toString().replaceAll("[\\p{Cc}]", "") + "\": ");
    jsonLines.append(" \""
            + m.value().toString() + "\"");
  }
  jsonLines.append("}");
  if(LOG.isInfoEnabled()) {
    LOG.info("kafka message: "+ jsonLines.toString());
  }

  ArgumentCaptor<List> argument
          = ArgumentCaptor.forClass(List.class);
  verify(mockProducer).send(argument.capture());
  List<KeyedMessage<Integer, byte[]>> list =
          (List<KeyedMessage<Integer, byte[]>>)
          (argument.getValue());
  String jsonResult
          = new String(list.get(0).message());
  if(LOG.isInfoEnabled()) {
    LOG.info("kafka result: "+ jsonResult);
  }
  assertEquals(jsonLines.toString(), jsonResult);
  }
}
