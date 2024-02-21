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

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static team.unison.perf.PerfLoaderUtils.getProperty;

final class FsSnapshotterPropertiesBuilder {
  private FsSnapshotterPropertiesBuilder() {
  }

  static List<FsSnapshotter> build(Properties props, SshConnectionBuilder sshConnectionBuilder) {
    List<FsSnapshotter> fsSnapshotters = new ArrayList<>();
    Set<String> fsSnapshotterNames = props.stringPropertyNames().stream().filter(s -> s.startsWith("snapshot.")).map(s -> s.split("\\.")[1])
            .collect(Collectors.toSet());

    for (String fsSnapshotterName : fsSnapshotterNames) {
      String prefix = "snapshot." + fsSnapshotterName + ".";
      List<String> hosts = PerfLoaderUtils.parseTemplate(getProperty(props, prefix, "hosts").replace(" ", ""));
      List<GenericWorkerBuilder> genericWorkerBuilders = new ArrayList<>();
      for (String host : hosts) {
        genericWorkerBuilders.add(ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilder.host(host)));
      }
      FsSnapshotter fsSnapshotter = new FsSnapshotterBuilder()
              .name(fsSnapshotterName)
              .conf(Confs.get(getProperty(props, prefix, "conf")))
              .threads(Integer.parseInt(getProperty(props, prefix, "threads", "8")))
              .actions(Objects.requireNonNull(getProperty(props, prefix, "actions"), "Actions not set for snapshotter " + fsSnapshotterName))
              .paths(PerfLoaderUtils.parseTemplate(getProperty(props, prefix, "paths", "")))
              .genericWorkerBuilders(genericWorkerBuilders)
              .period(Duration.ofSeconds(Long.parseLong(getProperty(props, prefix, "period", "60"))))
              .pathsInBatch(Integer.parseInt(getProperty(props, prefix, "batch.path.count", "10")))
              .subdirsDepth(Integer.parseInt(getProperty(props, prefix, "subdirs.depth", "2")))
              .subdirsWidth(Integer.parseInt(getProperty(props, prefix, "subdirs.width", "2")))
              .subdirsFormat(getProperty(props, prefix, "subdirs.format", "%d"))
              .createFsSnapshotter();

      fsSnapshotters.add(fsSnapshotter);
    }
    return fsSnapshotters;
  }
}
