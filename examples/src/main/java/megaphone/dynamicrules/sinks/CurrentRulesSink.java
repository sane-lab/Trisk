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

package megaphone.dynamicrules.sinks;

import megaphone.config.Config;
import megaphone.dynamicrules.KafkaUtils;
import megaphone.dynamicrules.ControlMessage;
import megaphone.dynamicrules.functions.JsonSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.io.IOException;
import java.util.Properties;

import static megaphone.config.Parameters.*;

public class CurrentRulesSink {

  public static SinkFunction<String> createRulesSink(Config config) throws IOException {

    String sinkType = config.get(RULES_EXPORT_SINK);
    Type currentRulesSinkType =
        Type.valueOf(sinkType.toUpperCase());

    switch (currentRulesSinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(config);
        String alertsTopic = config.get(RULES_EXPORT_TOPIC);
        return new FlinkKafkaProducer011<>(alertsTopic, new SimpleStringSchema(), kafkaProps);
//      case PUBSUB:
//        return PubSubSink.<String>newBuilder()
//            .withSerializationSchema(new SimpleStringSchema())
//            .withProjectName(config.get(GCP_PROJECT_NAME))
//            .withTopicName(config.get(GCP_PUBSUB_RULES_SUBSCRIPTION))
//            .build();
      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + currentRulesSinkType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public static DataStream<String> rulesStreamToJson(DataStream<ControlMessage> alerts) {
    return alerts.flatMap(new JsonSerializer<>(ControlMessage.class)).name("Rules Deserialization");
  }

  public enum Type {
    KAFKA("Current Rules Sink (Kafka)"),
    PUBSUB("Current Rules Sink (Pub/Sub)"),
    STDOUT("Current Rules Sink (Std. Out)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
