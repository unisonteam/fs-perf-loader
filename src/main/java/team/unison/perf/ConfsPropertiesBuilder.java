/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.perf;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

final class ConfsPropertiesBuilder {
  private ConfsPropertiesBuilder() {
  }

  static void build(Properties properties) {
    Set<String> confNames = properties.stringPropertyNames().stream().filter(s -> s.startsWith("conf.")).map(s -> s.split("\\.")[1])
            .collect(Collectors.toSet());

    Map<String, Map<String, String>> confs = new HashMap<>();
    for (String confName : confNames) {
      String prefix = "conf." + confName + ".";
      Map<String, String> conf = new HashMap<>();
      Set<String> confProperties = properties.stringPropertyNames().stream().filter(s -> s.startsWith(prefix)).collect(Collectors.toSet());
      for (String confProperty : confProperties) {
        conf.put(confProperty.replace(prefix, ""), properties.getProperty(confProperty));
      }
      confs.put(confName, conf);
    }

    Confs.set(confs);
  }
}