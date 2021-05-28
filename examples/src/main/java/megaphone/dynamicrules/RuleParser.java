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

package megaphone.dynamicrules;

import megaphone.dynamicrules.ControlMessage.AggregatorFunctionType;
import megaphone.dynamicrules.ControlMessage.LimitOperatorType;
import megaphone.dynamicrules.ControlMessage.RuleState;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RuleParser {

  private final ObjectMapper objectMapper = new ObjectMapper();

  public ControlMessage fromString(String line) throws IOException {
    if (line.length() > 0 && '{' == line.charAt(0)) {
      return parseJson(line);
    } else {
      return parsePlain(line);
    }
  }

  private ControlMessage parseJson(String ruleString) throws IOException {
    return objectMapper.readValue(ruleString, ControlMessage.class);
  }

  private static ControlMessage parsePlain(String ruleString) throws IOException {
    List<String> tokens = Arrays.asList(ruleString.split(","));
    if (tokens.size() != 9) {
      throw new IOException("Invalid controlMessage (wrong number of tokens): " + ruleString);
    }

    Iterator<String> iter = tokens.iterator();
    ControlMessage controlMessage = new ControlMessage();

    controlMessage.setRuleId(Integer.parseInt(stripBrackets(iter.next())));
    controlMessage.setRuleState(RuleState.valueOf(stripBrackets(iter.next()).toUpperCase()));
    controlMessage.setGroupingKeyNames(getNames(iter.next()));
    controlMessage.setUnique(getNames(iter.next()));
    controlMessage.setAggregateFieldName(stripBrackets(iter.next()));
    controlMessage.setAggregatorFunctionType(
        AggregatorFunctionType.valueOf(stripBrackets(iter.next()).toUpperCase()));
    controlMessage.setLimitOperatorType(LimitOperatorType.fromString(stripBrackets(iter.next())));
    controlMessage.setLimit(new BigDecimal(stripBrackets(iter.next())));
    controlMessage.setWindowMinutes(Integer.parseInt(stripBrackets(iter.next())));

    return controlMessage;
  }

  private static String stripBrackets(String expression) {
    return expression.replaceAll("[()]", "");
  }

  private static List<String> getNames(String expression) {
    String keyNamesString = expression.replaceAll("[()]", "");
    if (!"".equals(keyNamesString)) {
      String[] tokens = keyNamesString.split("&", -1);
      return Arrays.asList(tokens);
    } else {
      return new ArrayList<>();
    }
  }
}
