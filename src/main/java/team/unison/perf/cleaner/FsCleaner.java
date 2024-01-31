/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.cleaner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PerfLoaderUtils;
import team.unison.remote.GenericWorker;
import team.unison.remote.GenericWorkerBuilder;
import team.unison.remote.WorkerException;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class FsCleaner implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(FsCleaner.class);

  private final String name;
  private final Map<String, String> conf;
  private final GenericWorker genericWorker;
  private final List<String> paths;
  private final int threads;
  private final List<String> suffixes;
  private long[] results;

  public FsCleaner(String name, Map<String, String> conf, GenericWorkerBuilder genericWorkerBuilder, List<String> paths, int threads,
                   Collection<String> suffixes) {
    this.name = name;
    this.conf = conf;
    this.genericWorker = genericWorkerBuilder.get();
    this.paths = new ArrayList<>(paths);
    this.threads = threads;
    this.suffixes = new ArrayList<>(suffixes);

    validate();
  }

  private void validate() {
    if (paths.isEmpty()) {
      throw new IllegalArgumentException("Paths not set");
    }

    if (threads <= 0) {
      throw new IllegalArgumentException("Number of threads is zero or negative : " + threads);
    }
  }

  @Override
  public void run() {
    Instant before = Instant.now();
    log.info("Start cleaner {} at host {}", name, genericWorker.getHost());
    try {
      results = genericWorker.getAgent().clean(conf, paths, suffixes, threads);
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    }
    log.info("End cleaner {} at host {}, cleaner took {}", name, genericWorker.getHost(), Duration.between(before, Instant.now()));
    printSummary(Duration.between(before, Instant.now()));
  }

  @Override
  public String toString() {
    return "FsCleaner{" + "paths=" + paths + ", threads=" + threads + ", suffixes=" + suffixes + ", genericWorker=" + genericWorker + '}';
  }

  public void printSummary(Duration duration) {
    String header = "Cleaner: " + name;

    PerfLoaderUtils.printStatistics(header, "delete", conf == null ? null : conf.get("s3.uri"), threads, 0, results, duration);
  }
}
