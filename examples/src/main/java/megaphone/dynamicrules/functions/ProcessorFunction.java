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

package megaphone.dynamicrules.functions;

import lombok.extern.slf4j.Slf4j;
import megaphone.dynamicrules.Alert;
import megaphone.dynamicrules.Keyed;
import megaphone.dynamicrules.MegaphoneEvaluator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

import static common.Util.delay;

/** Implements main rule evaluation and alerting logic. */
@Slf4j
public class ProcessorFunction
    extends KeyedBroadcastProcessFunction<
        String, Keyed<Tuple3<String, String, Long>, String, String>, String, Alert> {

  private final Set<String> observedKeys = new HashSet<>();
  // local State is stored in the granularity of per key group.
//  private final Map<Integer, String> localState = new HashMap<>();
  private final Map<String, Integer> localState = new HashMap<>();

  String keyGroupToKeyMapStr = "0=A12, 1=A28, 2=A14, 3=A19, 4=A42, 5=A133, 6=A214, 7=A364, 8=A20, 9=A23, " +
          "10=A9, 11=A203, 12=A145, 13=A163, 14=A234, 15=A7, 16=A33, 17=A175, 18=A40, 19=A164, " +
          "20=A24, 21=A41, 22=A72, 23=A60, 24=A36, 25=A293, 26=A105, 27=A57, 28=A281, 29=A11, " +
          "30=A137, 31=A5, 32=A442, 33=A197, 34=A77, 35=A382, 36=A46, 37=A170, 38=A169, 39=A26, " +
          "40=A168, 41=A139, 42=A108, 43=A179, 44=A62, 45=A4, 46=A249, 47=A48, 48=A147, 49=A64, " +
          "50=A327, 51=A125, 52=A208, 53=A2, 54=A8, 55=A6, 56=A3, 57=A50, 58=A86, 59=A78, " +
          "60=A103, 61=A245, 62=A63, 63=A101, 64=A419, 65=A83, 66=A221, 67=A17, 68=A136, 69=A397, " +
          "70=A226, 71=A0, 72=A94, 73=A354, 74=A44, 75=A156, 76=A10, 77=A148, 78=A55, 79=A106, " +
          "80=A45, 81=A32, 82=A52, 83=A142, 84=A153, 85=A263, 86=A87, 87=A49, 88=A135, 89=A209, " +
          "90=A31, 91=A222, 92=A195, 93=A25, 94=A93, 95=A117, 96=A61, 97=A151, 98=A286, 99=A279, " +
          "100=A67, 101=A18, 102=A1, 103=A251, 104=A229, 105=A227, 106=A21, 107=A357, 108=A91, 109=A76, " +
          "110=A291, 111=A358, 112=A99, 113=A15, 114=A692, 115=A51, 116=A73, 117=A29, 118=A111, 119=A38, " +
          "120=A56, 121=A39, 122=A150, 123=A636, 124=A230, 125=A22, 126=A233, 127=A66";
  Map<String, Integer> KeyToKeyGroupMap = new HashMap<>();


  private final String TOPIC = "megaphone_state";	// status topic, used to do recovery.
  private final String servers = "localhost:9092";
  private KafkaProducer<String, String> producer;
  private final String uniqueID = UUID.randomUUID().toString();

  private void initKakfaProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", servers);
    props.put("client.id", uniqueID);
    // serializer is very important, which might result in expcetions if incorrect type was set.
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producer = new KafkaProducer<>(props);
  }

  @Override
  public void open(Configuration parameters) {
    Meter alertMeter = new MeterView(60);
    getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
    for (String kvStr : keyGroupToKeyMapStr.split(", ")) {
      String[] kv = kvStr.split("=");
      KeyToKeyGroupMap.put(kv[1], Integer.parseInt(kv[0]));
    }
    initKakfaProducer();
  }

  @Override
  public void processElement(
      Keyed<Tuple3<String, String, Long>, String, String> tuple, ReadOnlyContext ctx, Collector<Alert> out) {
//    observedKeys.add(KeyToKeyGroupMap.get(tuple.getKey()));
    observedKeys.add(tuple.getWrapped().f0);
//    int keyGroup = KeyToKeyGroupMap.get(tuple.getKey());
    String tupleKey = tuple.getWrapped().f0;
    String tupleValue = tuple.getWrapped().f1;
    long tupleTs = tuple.getWrapped().f2;

    if (tuple.getState() != null) {
      // new reconfig key received
      System.out.println("++++++ received state: " + tuple.getState());
      localState.put(tupleKey, Integer.parseInt(tuple.getState())+1);
    } else {
      localState.put(tupleKey, localState.getOrDefault(tupleKey, 0)+1);
    }

    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, tupleKey, String.format("%d|%d", localState.get(tupleKey), tupleTs));
    producer.send(newRecord);
    producer.flush();

//    delay();

    long threadId = Thread.currentThread().getId();
//    System.out.println("job: " + threadId + " - " + tupleKey + " : " + localState.get(tupleKey));
    System.out.println("thread: " + threadId
            + " routing latency: " + (tupleTs - Long.parseLong(tupleValue))
            + " processing time: " + (System.currentTimeMillis() - tupleTs)
            + " endToEnd latency: " + (System.currentTimeMillis() - Long.parseLong(tupleValue)));
    out.collect(new Alert<>(0, tuple.getKey(), tuple.getWrapped(), 0));
  }

  @Override
  public void processBroadcastElement(String controlMessage, Context ctx, Collector<Alert> out)
      throws Exception {
    log.info("{}", controlMessage);
    BroadcastState<String, Integer> broadcastState =
        ctx.getBroadcastState(MegaphoneEvaluator.Descriptors.rulesDescriptor);
    for (String kvStr : controlMessage.split(", ")) {
      String[] kv = kvStr.split("=");
      broadcastState.put(kv[0], Integer.parseInt(kv[1]));
    }
    observedKeys.clear();
//    localState.clear();
  }
}
