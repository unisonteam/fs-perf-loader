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

  /**
   * Returns a thread-safe deep copy of the current statistics.
   * The returned DTO is independent and safe to read without synchronization.
   */
  public synchronized StatisticsDTO snapshot() {
    StatisticsDTO copy = new StatisticsDTO();
    for (Map.Entry<String, List<Long>> entry : results.entrySet()) {
      copy.results.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    return copy;
  }

  /**
   * Returns a compact summary: operation name to [totalCount, failedCount].
   */
  public synchronized Map<String, long[]> getSummary() {
    Map<String, long[]> summary = new HashMap<>();
    for (Map.Entry<String, List<Long>> entry : results.entrySet()) {
      long total = entry.getValue().size();
      long failed = 0;
      for (Long val : entry.getValue()) {
        if (val <= 0) {
          failed++;
        }
      }
      summary.put(entry.getKey(), new long[]{total, failed});
    }
    return summary;
  }
}