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

package megaphone.dynamicrules.sources;

import megaphone.config.Config;
import megaphone.dynamicrules.KafkaUtils;
import megaphone.dynamicrules.Transaction;
import megaphone.dynamicrules.functions.JsonDeserializer;
import megaphone.dynamicrules.functions.JsonGeneratorWrapper;
import megaphone.dynamicrules.functions.TimeStamper;
import megaphone.dynamicrules.functions.TransactionsGenerator;
import megaphone.config.Parameters;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class TransactionsSource {

  public static SourceFunction<String> createTransactionsSource(Config config) {

    String sourceType = config.get(Parameters.TRANSACTIONS_SOURCE);
    Type transactionsSourceType =
        Type.valueOf(sourceType.toUpperCase());

    int transactionsPerSecond = config.get(Parameters.RECORDS_PER_SECOND);

    switch (transactionsSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String transactionsTopic = config.get(Parameters.DATA_TOPIC);
        FlinkKafkaConsumer011<String> kafkaConsumer =
            new FlinkKafkaConsumer011<>(transactionsTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
      default:
        return new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));
    }
  }

  public static DataStream<Transaction> stringsStreamToTransactions(
      DataStream<String> transactionStrings) {
    return transactionStrings
        .flatMap(new JsonDeserializer<Transaction>(Transaction.class))
        .returns(Transaction.class)
        .flatMap(new TimeStamper<Transaction>())
        .returns(Transaction.class)
        .name("Transactions Deserialization");
  }

  public enum Type {
    GENERATOR("Transactions Source (generated locally)"),
    KAFKA("Transactions Source (Kafka)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
