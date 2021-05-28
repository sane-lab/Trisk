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
import megaphone.dynamicrules.ControlMessage;
import megaphone.dynamicrules.KafkaUtils;
import megaphone.dynamicrules.functions.RuleDeserializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static megaphone.config.Parameters.*;

public class ControlMessageSource {

  private static final int RULES_STREAM_PARALLELISM = 1;

  public static SourceFunction<String> createControlMessageSource(Config config) throws IOException {

    String sourceType = config.get(RULES_SOURCE);
    Type rulesSourceType = Type.valueOf(sourceType.toUpperCase());

    switch (rulesSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String rulesTopic = config.get(RULES_TOPIC);
        FlinkKafkaConsumer011<String> kafkaConsumer =
            new FlinkKafkaConsumer011<>(rulesTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
//      case PUBSUB:
//        return PubSubSource.<String>newBuilder()
//            .withDeserializationSchema(new SimpleStringSchema())
//            .withProjectName(config.get(GCP_PROJECT_NAME))
//            .withSubscriptionName(config.get(GCP_PUBSUB_RULES_SUBSCRIPTION))
//            .build();
      case SOCKET:
        return new SocketTextStreamFunction("localhost", config.get(SOCKET_PORT), "\n", -1);
      case CUSTOM:
        return new MySource();
      default:
        throw new IllegalArgumentException(
            "Source \"" + rulesSourceType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public static DataStream<ControlMessage> stringsStreamToRules(DataStream<String> ruleStrings) {
    return ruleStrings
        .flatMap(new RuleDeserializer())
        .name("ControlMessage Deserialization")
        .setParallelism(RULES_STREAM_PARALLELISM)
        .assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<ControlMessage>(Time.of(0, TimeUnit.MILLISECONDS)) {
              @Override
              public long extractTimestamp(ControlMessage element) {
                // Prevents connected data+update stream watermark stalling.
                return Long.MAX_VALUE;
              }
            });
  }

  public enum Type {
    KAFKA("Rules Source (Kafka)"),
    PUBSUB("Rules Source (Pub/Sub)"),
    SOCKET("Rules Source (Socket)"),
    CUSTOM("Custom source");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private static class MySource implements SourceFunction<String> {

    private volatile boolean isRunning = true;


    @Override
    public void run(SourceContext<String> ctx) {
//      String ruleString = "{ \"ruleId\": 1, " +
//              "\"ruleState\": \"ACTIVE\", " +
//              "\"groupingKeyNames\": [\"paymentType\"], " +
//              "\"unique\": [], " +
//              "\"aggregateFieldName\": \"paymentAmount\", " +
//              "\"aggregatorFunctionType\": \"SUM\"," +
//              "\"limitOperatorType\": \"GREATER\"," +
//              "\"limit\": 500, " +
//              "\"windowMinutes\": 20}";

//      String payload1 =
//              "{\"ruleId\":\"1\","
//                      + "\"aggregateFieldName\":\"paymentAmount\","
//                      + "\"aggregatorFunctionType\":\"SUM\","
//                      + "\"groupingKeyNames\":[\"payeeId\", \"beneficiaryId\"],"
//                      + "\"limit\":\"20000000\","
//                      + "\"limitOperatorType\":\"GREATER\","
//                      + "\"ruleState\":\"ACTIVE\","
//                      + "\"windowMinutes\":\"43200\"}";
//
//      String payload2 =
//              "{\"ruleId\":\"2\","
//                      + "\"aggregateFieldName\":\"COUNT_FLINK\","
//                      + "\"aggregatorFunctionType\":\"SUM\","
//                      + "\"groupingKeyNames\":[\"paymentType\"],"
//                      + "\"limit\":\"300\","
//                      + "\"limitOperatorType\":\"LESS\","
//                      + "\"ruleState\":\"PAUSE\","
//                      + "\"windowMinutes\":\"1440\"}";
//
//      String payload3 =
//              "{\"ruleId\":\"3\","
//                      + "\"aggregateFieldName\":\"paymentAmount\","
//                      + "\"aggregatorFunctionType\":\"SUM\","
//                      + "\"groupingKeyNames\":[\"beneficiaryId\"],"
//                      + "\"limit\":\"10000000\","
//                      + "\"limitOperatorType\":\"GREATER_EQUAL\","
//                      + "\"ruleState\":\"ACTIVE\","
//                      + "\"windowMinutes\":\"1440\"}";
//
//
//      String payload4 =
//              "{\"ruleId\":\"4\","
//                      + "\"aggregateFieldName\":\"COUNT_WITH_RESET_FLINK\","
//                      + "\"aggregatorFunctionType\":\"SUM\","
//                      + "\"groupingKeyNames\":[\"paymentType\"],"
//                      + "\"limit\":\"100\","
//                      + "\"limitOperatorType\":\"GREATER_EQUAL\","
//                      + "\"ruleState\":\"ACTIVE\","
//                      + "\"windowMinutes\":\"1440\"}";
//
//      ctx.collect(payload1);
//      ctx.collect(payload2);
//      ctx.collect(payload3);
//      ctx.collect(payload4);

//    String keyToKeyGroupMapStr = "A87=87, A88=88, A89=89, A108=108, A109=109, A106=106, A107=107, A104=104, A105=105, A102=102, " +
//            "A103=103, A100=100, A0=0, A101=101, A1=1, A2=2, A3=3, A4=4, A5=5, A6=6, A7=7, A8=8, A9=9, A90=90, A91=91, A92=92, " +
//            "A93=93, A94=94, A95=95, A96=96, A97=97, A10=10, A98=98, A11=11, A99=99, A12=12, A13=13, A14=14, A15=15, A16=16, " +
//            "A119=119, A17=17, A18=18, A117=117, A19=19, A118=118, A115=115, A116=116, A113=113, A114=114, A111=111, A112=112, " +
//            "A110=110, A20=20, A21=21, A22=22, A23=23, A24=24, A25=25, A26=26, A27=27, A28=28, A29=29, A126=126, A127=127, " +
//            "A124=124, A125=125, A122=122, A123=123, A120=120, A121=121, A30=30, A31=31, A32=32, A33=33, A34=34, A35=35, A36=36, " +
//            "A37=37, A38=38, A39=39, A40=40, A41=41, A42=42, A43=43, A44=44, A45=45, A46=46, A47=47, A48=48, A49=49, A50=50, A51=51, " +
//            "A52=52, A53=53, A54=54, A55=55, A56=56, A57=57, A58=58, A59=59, A60=60, A61=61, A62=62, A63=63, A64=64, A65=65, A66=66, " +
//            "A67=67, A68=68, A69=69, A70=70, A71=71, A72=72, A73=73, A74=74, A75=75, A76=76, A77=77, A78=78, A79=79, A80=80, A81=81, " +
//            "A82=82, A83=83, A84=84, A85=85, A86=86";

//      while (true) {
        try {
          Thread.sleep(30000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        StringBuilder keyToKeyGroupMapStr = new StringBuilder();
        for (int i = 0; i < 128; i++) {
          String key = "A" + i;
          keyToKeyGroupMapStr.append(String.format("%s=%d, ", key, i));
        }
        System.out.println("++++++ new key mapping1");
        ctx.collect(keyToKeyGroupMapStr.substring(0, keyToKeyGroupMapStr.length() - 2));
//      }
//      try {
//        Thread.sleep(5000);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
//
//      keyToKeyGroupMapStr = new StringBuilder();
//      for (int i=0; i<128; i++) {
//        String key = "A" + i;
//        int keyGroup = 80;
//        keyToKeyGroupMapStr.append(String.format("%s=%d, ", key, keyGroup));
//      }
//      System.out.println("++++++ new key mapping1");
//      ctx.collect(keyToKeyGroupMapStr.substring(0, keyToKeyGroupMapStr.length()-2));
//
//      try {
//        Thread.sleep(5000);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
//
//      keyToKeyGroupMapStr = new StringBuilder();
//      for (int i=0; i<128; i++) {
//        String key = "A" + i;
//        int keyGroup = 0;
//        keyToKeyGroupMapStr.append(String.format("%s=%d, ", key, keyGroup));
//      }
//      System.out.println("++++++ new key mapping2");
//      ctx.collect(keyToKeyGroupMapStr.substring(0, keyToKeyGroupMapStr.length()-2));
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
