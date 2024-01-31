/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class Confs {
  private static final Map<String, Map<String, String>> CONF_MAP = new ConcurrentHashMap<>();

  static void set(Map<String, Map<String, String>> confMap) {
    CONF_MAP.putAll(confMap);
  }

  static Map<String, String> get(String confName) {
    if (confName == null) {
      return null;
    }
    return CONF_MAP.get(confName);
  }
}