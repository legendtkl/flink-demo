package org.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

/**
 * usage: --topic <topic-name> --bootstrap.servers <hostname>:9092 --group.id myGroup12 --path tos://<bucket>/<path>/
 */
public class Kafka2TosAsParquet {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    String brokers = parameterTool.getRequired("bootstrap.servers");
    String topic = parameterTool.getRequired("topic");
    String groupId = parameterTool.getRequired("group.id");
    String path = parameterTool.getRequired("path");

    env.enableCheckpointing(300000, CheckpointingMode.EXACTLY_ONCE);

    // Event: kafka message schema
    KafkaSource<Event> source = KafkaSource.<Event>builder()
      .setBootstrapServers(brokers)
      .setTopics(topic)
      .setGroupId(groupId)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Event.class))
      .build();

    DataStream<Event> messageStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

    messageStream.sinkTo(FileSink
      .forBulkFormat(new Path(path), AvroParquetWriters.forReflectRecord(Event.class))
      .withRollingPolicy(
        OnCheckpointRollingPolicy.build())
      .build());

    env.execute("Kafka To Tos Example");
  }
}
