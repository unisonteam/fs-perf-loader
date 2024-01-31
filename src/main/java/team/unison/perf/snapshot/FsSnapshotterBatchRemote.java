/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.snapshot;

import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.WorkerException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FsSnapshotterBatchRemote {
  private static final Logger log = LoggerFactory.getLogger(FsSnapshotterBatchRemote.class);

  private static final AtomicInteger FS_WRAPPER_COUNTER = new AtomicInteger();
  private static final ConcurrentHashMap<String, Boolean> SNAPSHOT_PATHS = new ConcurrentHashMap<>();

  public static StatisticsDTO snapshot(Map<String, String> conf, List<String> paths, FsSnapshotterOperationConf opConf) {
    StatisticsDTO stats = new StatisticsDTO();
    if (paths == null || paths.isEmpty()) {
      return stats;
    }

    ExecutorService executorService = Executors.newFixedThreadPool(opConf.getThreadCount());
    String randomPath = paths.get(0);
    List<FsWrapper> fsWrappers = FsWrapperFactory.get(randomPath, conf);

    List<Callable<Object>> callables = paths.stream()
            .map(path -> Executors.callable(
                    () -> {
                      try {
                        FsWrapper fsWrapper = randomFsWrapper(fsWrappers);
                        long startFirst = System.nanoTime();
                        if (SNAPSHOT_PATHS.containsKey(path)) {
                          boolean success = deleteSnapshot(fsWrapper, path, opConf.getSnapshotName());
                          long elapsedFirst = System.nanoTime() - startFirst;
                          PrometheusUtils.record(stats, "delete_snapshot", -1, success, elapsedFirst);
                        } else {
                          SNAPSHOT_PATHS.put(path, Boolean.TRUE);
                          boolean success = allowSnapshot(fsWrapper, path);
                          long elapsedFirst = System.nanoTime() - startFirst;
                          PrometheusUtils.record(stats, "make_snapshottable", -1, success, elapsedFirst);
                        }
                        long startCreate = System.nanoTime();
                        try {
                          boolean success = createSnapshot(fsWrapper, path, opConf.getSnapshotName());
                          long elapsedCreate = System.nanoTime() - startCreate;
                          PrometheusUtils.record(stats, "create_snapshot", -1, success, elapsedCreate);
                        } catch (SnapshotException e) {
                          log.warn("Error creating snapshot for path {}: {}", path, e.getMessage());
                        }
                      } catch (Exception e) {
                        log.warn("Error creating snapshot for path {}", path, e);
                        throw WorkerException.wrap(e);
                      }
                    }
            ))
            .collect(Collectors.toList());
    try {
      executorService.invokeAll(callables);
    } catch (InterruptedException e) {
      throw WorkerException.wrap(e);
    } finally {
      executorService.shutdownNow();
    }
    return stats;
  }

  private static boolean allowSnapshot(FsWrapper fsWrapper, String path) {
    return fsWrapper.allowSnapshot(path);
  }

  private static boolean createSnapshot(FsWrapper fsWrapper, String path, String snapshotName) throws IOException {
    return fsWrapper.createSnapshot(path, snapshotName);
  }

  private static boolean deleteSnapshot(FsWrapper fsWrapper, String path, String snapshotName) {
    return fsWrapper.deleteSnapshot(path, snapshotName);
  }

  private static FsWrapper randomFsWrapper(List<FsWrapper> fsWrappers) {
    return fsWrappers.get(FS_WRAPPER_COUNTER.getAndIncrement() % fsWrappers.size());
  }
}