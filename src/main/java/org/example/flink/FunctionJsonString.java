package org.example.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;

public class FunctionJsonString {
  public static void main(String[] args) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .build();

    TableEnvironment tableEnv = TableEnvironmentImpl.create(settings);

    tableEnv.executeSql(
      "CREATE table datagen_source(\n" +
        "  name VARCHAR\n" +
        ") WITH (\n" +
        "  'connector' = 'datagen',\n" +
        " 'rows-per-second'='1'\n" +
        ")");

    tableEnv.executeSql(
      "CREATE table print_sink(\n" +
        "  name VARCHAR\n" +
        ") with (\n" +
        "  'connector' = 'print'\n" +
        ")"
    );

    TableResult result = tableEnv.executeSql("SELECT JSON_VALUE('{\"a.b\": [0.998,0.996]}','$.[\"a.c\"][0]')");
    result.print();
    //tableEnv.executeSql("INSERT INTO print_sink SELECT * from datagen_source");
  }
}
