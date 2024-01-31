/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.stats;

import java.io.Serializable;
import java.util.*;

public class StatisticsDTO implements Serializable {
  private final Map<String, List<Long>> results = new HashMap<>();

  public Collection<String> getOperations() {
    return results.keySet();
  }

  public List<Long> getResults(String operationName) {
    return results.get(operationName);
  }

  public synchronized void add(String operationName, boolean success, long elapsed) {
    if (!results.containsKey(operationName)) {
      results.put(operationName, new ArrayList<>());
    }
    long result = (success ? 1 : -1) * elapsed;
    results.get(operationName).add(result);
  }

  public synchronized void add(StatisticsDTO other) {
    for (String operationName : other.getOperations()) {
      if (!results.containsKey(operationName)) {
        results.put(operationName, new ArrayList<>());
      }
      results.get(operationName).addAll(other.getResults(operationName));
    }
  }

  public void add(Collection<StatisticsDTO> others) {
    for (StatisticsDTO other : others) {
      add(other);
    }
  }

  public synchronized void clear() {
    results.clear();
  }
}