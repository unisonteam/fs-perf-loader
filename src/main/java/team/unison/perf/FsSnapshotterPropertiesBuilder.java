/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf;

import team.unison.perf.snapshot.FsSnapshotter;
import team.unison.perf.snapshot.FsSnapshotterBuilder;
import team.unison.remote.ClientFactory;
import team.unison.remote.GenericWorkerBuilder;
import team.unison.remote.SshConnectionBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

final class FsSnapshotterPropertiesBuilder {
  private FsSnapshotterPropertiesBuilder() {
  }

  static List<FsSnapshotter> build(Properties props, SshConnectionBuilder sshConnectionBuilder) throws IOException {
    List<FsSnapshotter> fsSnapshotters = new ArrayList<>();
    Set<String> fsSnapshotterNames = props.stringPropertyNames().stream().filter(s -> s.startsWith("snapshot.")).map(s -> s.split("\\.")[1])
            .collect(Collectors.toSet());

    for (String fsLoaderName : fsSnapshotterNames) {
      String prefix = "snapshot." + fsLoaderName + ".";
      List<String> hosts = PerfLoaderUtils.parseTemplate(props.getProperty(prefix + "hosts").replace(" ", ""));
      List<GenericWorkerBuilder> genericWorkerBuilders = new ArrayList<>();
      for (String host : hosts) {
        genericWorkerBuilders.add(ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilder.host(host)));
      }
      FsSnapshotter fsSnapshotter = new FsSnapshotterBuilder()
              .name(fsLoaderName)
              .conf(Confs.get(props.getProperty(prefix + "conf")))
              .threads(Integer.parseInt(props.getProperty(prefix + "threads", "8")))
              .snapshotName(props.getProperty(prefix + "name", "snapshot"))
              .paths(PerfLoaderUtils.parseTemplate(props.getProperty(prefix + "paths", "")))
              .genericWorkerBuilders(genericWorkerBuilders)
              .period(Duration.ofSeconds(Long.parseLong(props.getProperty(prefix + "period", "60"))))
              .pathsInBatch(Integer.parseInt(props.getProperty(prefix + "batch.path.count", "10")))
              .subdirsDepth(Integer.parseInt(props.getProperty(prefix + "subdirs.depth", "2")))
              .subdirsWidth(Integer.parseInt(props.getProperty(prefix + "subdirs.width", "2")))
              .subdirsFormat(props.getProperty(prefix + "subdirs.format", "%d"))
              .createFsSnapshotter();

      fsSnapshotters.add(fsSnapshotter);
    }
    return fsSnapshotters;
  }
}
