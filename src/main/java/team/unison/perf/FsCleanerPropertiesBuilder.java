/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf;

import team.unison.perf.cleaner.FsCleaner;
import team.unison.perf.cleaner.FsCleanerBuilder;
import team.unison.remote.ClientFactory;
import team.unison.remote.SshConnectionBuilder;

import java.util.*;
import java.util.stream.Collectors;

final class FsCleanerPropertiesBuilder {
  private FsCleanerPropertiesBuilder() {
  }

  static List<FsCleaner> build(Properties props, SshConnectionBuilder sshConnectionBuilder) {
    List<FsCleaner> fsCleaners = new ArrayList<>();
    Set<String> fsCleanerNames = props.stringPropertyNames().stream().filter(s -> s.startsWith("fscleaner.")).map(s -> s.split("\\.")[1])
            .collect(Collectors.toSet());

    for (String fsCleanerName : fsCleanerNames) {
      String prefix = "fscleaner." + fsCleanerName + ".";

      String host = props.getProperty(prefix + "host");

      if (host == null || host.isEmpty()) {
        throw new IllegalArgumentException("Property" + prefix + "host" + " is not set");
      }

      FsCleaner fsCleaner = new FsCleanerBuilder()
              .name(fsCleanerName)
              .conf(Confs.get(props.getProperty(prefix + "conf")))
              .genericWorkerBuilder(ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilder.host(host)))
              .paths(PerfLoaderUtils.parseTemplate(props.getProperty(prefix + "paths", "")))
              .suffixes(Arrays.asList(props.getProperty(prefix + "suffixes", "").split(",")))
              .threads(Integer.parseInt(props.getProperty(prefix + "threads", "8")))
              .createFsCleaner();

      fsCleaners.add(fsCleaner);
    }
    return fsCleaners;
  }
}
