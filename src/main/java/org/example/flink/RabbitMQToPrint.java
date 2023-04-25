package org.example.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMQToPrint {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
      .setHost("host.com")
      .setVirtualHost("/")
      .setPort(5672)
      .setUserName("user-name")
      .setPassword("password")
      .build();

    env.addSource(new RMQSource<String>(
        connectionConfig,
        "<queue-name>",
        false,
        new SimpleStringSchema()))
      .setParallelism(1)
      .print();

    env.execute("Read from rabbit.");
  }
}
