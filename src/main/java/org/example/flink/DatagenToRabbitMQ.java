package org.example.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class DatagenToRabbitMQ {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 创建DataGeneratorSource。传入上面自定义的数据生成器

    DataStream<String> input = env.addSource(new SimpleStringGenerator()).disableChaining();


    RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
      .setHost("passowrd")
      .setPort(5672)
      .setVirtualHost("/")
      .setUserName("user-name")
      .setPassword("password")
      .build();

    input.addSink(new RMQSink<>(
      connectionConfig,
      "<queue-name>",
      new SimpleStringSchema()));

    env.execute("Write into rabbit.");
  }

  public static class SimpleStringGenerator implements SourceFunction<String> {
    private static final long serialVersionUID = 2174904787118597074L;
    boolean running = true;
    int i = 0;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
      while (running) {
        int temp = i++;
        sourceContext.collect("OpenPlatform QA-Test Sink To ES " + temp);
        Thread.sleep(1000);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }
}
