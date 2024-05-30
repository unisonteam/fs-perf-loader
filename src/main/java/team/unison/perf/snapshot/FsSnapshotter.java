/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PerfLoaderUtils;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.GenericWorker;
import team.unison.remote.GenericWorkerBuilder;
import team.unison.remote.Utils;
import team.unison.remote.WorkerException;
import team.unison.transfer.FsWrapperDataForOperation;
import team.unison.transfer.FsSnapshotterDataForOperation;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static team.unison.perf.PerfLoaderUtils.initSubdirs;

public final class FsSnapshotter implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(FsSnapshotter.class);

  private final String name;
  private final Map<String, String> conf;
  private final Collection<GenericWorkerBuilder> genericWorkerBuilders;
  private final int threads;
  private final Duration period;
  private final List<String> paths;
  private final List<String> snapshotDirs;
  private final int pathsInBatch;
  private final String actions;
  private final AtomicLong totalNanos = new AtomicLong();

  private static final StatisticsDTO SNAPSHOT_RESULTS = new StatisticsDTO();

  FsSnapshotter(String name, Map<String, String> conf, Collection<GenericWorkerBuilder> genericWorkerBuilders,
                int threads, List<String> paths, int subdirsWidth, int subdirsDepth, String subdirsFormat,
                Duration period, int pathsInBatch, String actions) {
    this.name = name;
    this.conf = conf;
    this.genericWorkerBuilders = new ArrayList<>(genericWorkerBuilders);
    this.threads = threads;
    this.paths = new ArrayList<>(paths);
    snapshotDirs = initSubdirs(paths, subdirsWidth, subdirsDepth, subdirsFormat);
    this.period = period;
    this.pathsInBatch = pathsInBatch;
    this.actions = actions;

    validate();
  }

  private void validate() {
    if (genericWorkerBuilders == null || genericWorkerBuilders.isEmpty()) {
      throw new IllegalArgumentException("Empty genericWorkerBuilders");
    }

    if (paths == null || paths.isEmpty()) {
      throw new IllegalArgumentException("Empty paths");
    }
  }

  @Override
  public void run() {
    // split all paths by chunks
    List<List<String>> pathChunks = new ArrayList<>();
    for (int startIndex = 0; startIndex < snapshotDirs.size(); startIndex += pathsInBatch) {
      int lastIndex = Math.min(startIndex + pathsInBatch, snapshotDirs.size());
      pathChunks.add(new ArrayList<>(snapshotDirs.subList(startIndex, lastIndex)));
    }

    List<GenericWorker> genericWorkers = genericWorkerBuilders.parallelStream()
            .map(this::initAgent)
            .collect(Collectors.toList());

    Runtime.getRuntime().addShutdownHook(new Thread(
            () -> PerfLoaderUtils.printStatistics("Snapshotter: " + name, null, threads, 0, SNAPSHOT_RESULTS,
                    Duration.ofMillis(totalNanos.get() / 1_000_000))
    ));

    while (true) {
      log.info("Start snapshot {}", name);
      runSingle(genericWorkers, pathChunks);

      log.info("Waiting " + period + " between snapshots");
      Utils.sleep(period.toMillis());
    }
  }

  private void runSingle(List<GenericWorker> genericWorkers, List<List<String>> pathChunks) {
    ExecutorService executorService = Executors.newFixedThreadPool(genericWorkers.size());
    List<GenericWorker> workersCopy = new ArrayList<>(genericWorkers);

    try {
      List<Callable<StatisticsDTO>> callables = pathChunks.parallelStream()
              .map(batch -> ((Callable<StatisticsDTO>) () -> snapshot(workersCopy, batch)))
              .collect(Collectors.toList());
      long nanosBefore = System.nanoTime();
      List<Future<StatisticsDTO>> futures = executorService.invokeAll(callables);
      List<StatisticsDTO> commandResults = futures.stream().map(f -> {
        try {
          return f.get();
        } catch (Exception e) {
          throw WorkerException.wrap(e);
        }
      }).collect(Collectors.toList());
      totalNanos.addAndGet(System.nanoTime() - nanosBefore);
      SNAPSHOT_RESULTS.add(commandResults);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      executorService.shutdownNow();
    }
  }

  private StatisticsDTO snapshot(List<GenericWorker> workersCopy, List<String> paths) {
    GenericWorker genericWorker;
    StatisticsDTO stats;
    synchronized (workersCopy) {
      genericWorker = workersCopy.remove(workersCopy.size() - 1);
    }
    try {
      log.info("Start snapshot at host {}", genericWorker.getHost());
      Instant before = Instant.now();
      try {
        stats = genericWorker.getAgent().snapshot(name, paths);
      } catch (Exception e) {
        log.warn("Error running load", e);
        throw WorkerException.wrap(e);
      }
      log.info("End snapshot at host {}, took {}",
              genericWorker.getHost(), Duration.between(before, Instant.now()));
    } finally {
      synchronized (workersCopy) {
        workersCopy.add(genericWorker);
      }
    }

    return stats;
  }

  private @Nonnull GenericWorker initAgent(@Nonnull GenericWorkerBuilder workerBuilder) {
    GenericWorker genericWorker = workerBuilder.get();
    try {
      FsWrapperDataForOperation
          fsWrapperDataForOperation = new FsSnapshotterDataForOperation(threads, name, conf, actions);
      genericWorker.getAgent().setupAgent(fsWrapperDataForOperation);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return genericWorker;
  }

  @Override
  public String toString() {
    return "FsSnapshotter{" + "threads=" + threads + ", paths=" + paths
            + ", snapshot dirs count=" + snapshotDirs.size()
            + ", snapshot dirs=" + snapshotDirs.subList(0, Math.min(snapshotDirs.size(), 20))
            + ", period=" + period
            + ", actions=" + actions
            + ", pathsInBatch=" + pathsInBatch
            + '}';
  }

  public Duration getPeriod() {
    return period;
  }
}