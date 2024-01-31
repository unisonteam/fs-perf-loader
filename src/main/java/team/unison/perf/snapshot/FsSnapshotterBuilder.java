/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.snapshot;

import team.unison.remote.GenericWorkerBuilder;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FsSnapshotterBuilder {
  private String name;
  private Map<String, String> conf;
  private Collection<GenericWorkerBuilder> genericWorkerBuilders;
  private int threads = 8;
  private List<String> paths;
  private int subdirsWidth = 2;
  private int subdirsDepth = 2;
  private int pathsInBatch = 100;
  private String subdirsFormat = "%d";
  private Duration period = Duration.ofSeconds(60);
  private String snapshotName = "snapshot";

  public FsSnapshotterBuilder name(String name) {
    this.name = name;
    return this;
  }

  public FsSnapshotterBuilder conf(Map<String, String> conf) {
    this.conf = (conf == null) ? null : new HashMap<>(conf);

    return this;
  }

  public FsSnapshotterBuilder genericWorkerBuilders(Collection<GenericWorkerBuilder> genericWorkerBuilders) {
    this.genericWorkerBuilders = genericWorkerBuilders;
    return this;
  }

  public FsSnapshotterBuilder threads(int threads) {
    this.threads = threads;
    return this;
  }

  public FsSnapshotterBuilder paths(List<String> paths) {
    this.paths = paths;
    return this;
  }

  public FsSnapshotterBuilder pathsInBatch(int pathsInBatch) {
    this.pathsInBatch = pathsInBatch;
    return this;
  }

  public FsSnapshotterBuilder subdirsWidth(int subdirsWidth) {
    this.subdirsWidth = subdirsWidth;
    return this;
  }

  public FsSnapshotterBuilder subdirsDepth(int subdirsDepth) {
    this.subdirsDepth = subdirsDepth;
    return this;
  }

  public FsSnapshotterBuilder subdirsFormat(String subdirsFormat) {
    this.subdirsFormat = subdirsFormat;
    return this;
  }

  public FsSnapshotterBuilder period(Duration period) {
    this.period = period;
    return this;
  }

  public FsSnapshotterBuilder snapshotName(String snapshotName) {
    this.snapshotName = snapshotName;
    return this;
  }

  public FsSnapshotter createFsSnapshotter() {
    return new FsSnapshotter(name, conf, genericWorkerBuilders, threads, paths, subdirsWidth, subdirsDepth, subdirsFormat,
            period, pathsInBatch, snapshotName);
  }

  @Override
  public String toString() {
    return "FsLoaderBuilder{"
            + "name=" + name
            + ", threads=" + threads + ", paths=" + paths + ", subdirsWidth=" + subdirsWidth
            + ", subdirsDepth=" + subdirsDepth + ", subdirsFormat=" + subdirsFormat
            + ", period=" + period
            + ", pathsInBatch=" + pathsInBatch
            + '\'' + '}';
  }
}