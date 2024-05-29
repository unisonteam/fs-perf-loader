/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.loader;

import team.unison.remote.GenericWorkerBuilder;

import java.time.Duration;
import java.util.*;

public class FsLoaderBuilder {
  private String name;
  private Map<String, String> conf;
  private Collection<GenericWorkerBuilder> genericWorkerBuilders;
  private int threads = 8;
  private List<String> paths;
  private List<Map<String, String>> workload = new ArrayList<>();
  private int subdirsWidth = 2;
  private int subdirsDepth = 2;
  private String subdirsFormat = "%d";
  private int batches = 2;
  private boolean useTmpFile = true;
  private String fill = "random";
  private Duration loadDelay = Duration.ZERO;
  private Duration commandDelay = Duration.ofSeconds(25);
  private int count = 1;
  private Duration period = Duration.ZERO;
  private int filesInBatch = 100;
  private String filesSizesDistribution;
  private String filesSuffixesDistribution;
  private Duration batchTimeout = Duration.ofMinutes(10);
  private Random random = new Random();
  private FsLoader.Type type = FsLoader.Type.WINDOW;

  public FsLoaderBuilder name(String name) {
    this.name = name;
    return this;
  }

  public FsLoaderBuilder conf(Map<String, String> conf) {
    this.conf = (conf == null) ? new HashMap<>() : new HashMap<>(conf);
    return this;
  }

  public FsLoaderBuilder genericWorkerBuilders(Collection<GenericWorkerBuilder> genericWorkerBuilders) {
    this.genericWorkerBuilders = genericWorkerBuilders;
    return this;
  }

  public FsLoaderBuilder threads(int threads) {
    this.threads = threads;
    return this;
  }

  public FsLoaderBuilder paths(List<String> paths) {
    this.paths = paths;
    return this;
  }

  public FsLoaderBuilder workload(List<Map<String, String>> workload) {
    this.workload = workload;
    return this;
  }

  public FsLoaderBuilder subdirsWidth(int subdirsWidth) {
    this.subdirsWidth = subdirsWidth;
    return this;
  }

  public FsLoaderBuilder subdirsDepth(int subdirsDepth) {
    this.subdirsDepth = subdirsDepth;
    return this;
  }

  public FsLoaderBuilder subdirsFormat(String subdirsFormat) {
    this.subdirsFormat = subdirsFormat;
    return this;
  }

  public FsLoaderBuilder batches(int batches) {
    this.batches = batches;
    return this;
  }

  /**
   * Write data to a temporary _COPYING_ file and rename to the final destination at the end.
   */
  public FsLoaderBuilder useTmpFile(boolean useTmpFile) {
    this.useTmpFile = useTmpFile;
    return this;
  }

  /**
   * Delay after loading each file.
   */
  public FsLoaderBuilder loadDelay(Duration loadDelay) {
    this.loadDelay = loadDelay;
    return this;
  }

  /**
   * Delay between commands in workload.
   */
  public FsLoaderBuilder commandDelay(Duration commandDelay) {
    this.commandDelay = commandDelay;
    return this;
  }

  public FsLoaderBuilder count(int count) {
    this.count = count;
    return this;
  }

  public FsLoaderBuilder period(Duration period) {
    this.period = period;
    return this;
  }

  public FsLoaderBuilder filesInBatch(int filesInBatch) {
    this.filesInBatch = filesInBatch;
    return this;
  }

  public FsLoaderBuilder filesSizesDistribution(String filesSizesDistribution) {
    this.filesSizesDistribution = filesSizesDistribution;
    return this;
  }

  public FsLoaderBuilder filesSuffixesDistribution(String filesSuffixesDistribution) {
    this.filesSuffixesDistribution = filesSuffixesDistribution;
    return this;
  }

  public FsLoaderBuilder fill(String fill) {
    this.fill = fill;
    return this;
  }

  public FsLoaderBuilder batchTimeout(Duration batchTimeout) {
    this.batchTimeout = batchTimeout;
    return this;
  }

  public FsLoaderBuilder random(Random random) {
    this.random = random;
    return this;
  }

  public FsLoaderBuilder type(FsLoader.Type type) {
    this.type = type;
    return this;
  }

  public FsLoader createFsLoader() {
    return new FsLoader(name, conf, genericWorkerBuilders, threads, paths, workload, subdirsWidth, subdirsDepth, subdirsFormat, batches,
            useTmpFile, loadDelay, commandDelay, count, period, filesInBatch, filesSizesDistribution, filesSuffixesDistribution,
            fill,
            random, type);
  }

  @Override
  public String toString() {
    return "FsLoaderBuilder{"
            + "name=" + name
            + ", threads=" + threads + ", paths=" + paths + ", subdirsWidth=" + subdirsWidth
            + ", subdirsDepth=" + subdirsDepth + ", subdirsFormat=" + subdirsFormat
            + ", type=" + type + ", batches=" + batches
            + ", useTmpFile=" + useTmpFile + ", loadDelay=" + loadDelay + ", commandDelay=" + commandDelay
            + ", count=" + count + ", period=" + period
            + ", filesInBatch=" + filesInBatch + ", batchTimeout=" + batchTimeout + ", filesSizesDistribution='" + filesSizesDistribution + '\''
            + ", filesSuffixesDistribution='" + filesSuffixesDistribution
            + ", fill='" + fill
            + '\'' + '}';
  }
}