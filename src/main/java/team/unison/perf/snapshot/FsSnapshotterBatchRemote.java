/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.WorkerException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class FsSnapshotterBatchRemote {
  private static final Logger log = LoggerFactory.getLogger(FsSnapshotterBatchRemote.class);

  private static final String MAKE_SNAPSHOTTABLE_OPERATION = "make_snapshottable";
  private static final String CREATE_SNAPSHOT_OPERATION = "create_snapshot";
  private static final String DELETE_SNAPSHOT_OPERATION = "delete_snapshot";
  private static final String RENAME_SNAPSHOT_OPERATION = "rename_snapshot";

  private static final ConcurrentHashMap<String, Boolean> SNAPSHOT_PATHS = new ConcurrentHashMap<>();

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

  public static StatisticsDTO snapshot(Map<String, String> conf, List<String> paths, FsSnapshotterOperationConf opConf) {
    StatisticsDTO stats = new StatisticsDTO();
    if (paths == null || paths.isEmpty()) {
      return stats;
    }

    ExecutorService executorService = Executors.newFixedThreadPool(opConf.getThreadCount());
    String randomPath = paths.get(0);

    List<Callable<Object>> callables = paths.stream()
            .map(path -> Executors.callable(
                    () -> {
                      FsWrapper fsWrappers = FsWrapperFactory.get(randomPath, conf);
                      snapshotActions(fsWrappers, path, opConf, stats);
                    }
            )).collect(Collectors.toList());
    try {
      executorService.invokeAll(callables);
    } catch (InterruptedException e) {
      throw WorkerException.wrap(e);
    } finally {
      executorService.shutdownNow();
    }
    return stats;
  }

  private static void snapshotActions(FsWrapper fsWrapper, String path, FsSnapshotterOperationConf opConf, StatisticsDTO stats) {
    SNAPSHOT_PATHS.computeIfAbsent(path, p ->
            PrometheusUtils.runAndRecord(stats, MAKE_SNAPSHOTTABLE_OPERATION, () -> fsWrapper.allowSnapshot(path)));

    for (String snapshotOp : opConf.getActions().split(",")) {
      if(snapshotOp.contains("{DATETIME}")) {
        snapshotOp = snapshotOp.replace("{DATETIME}", DATE_TIME_FORMATTER.format(LocalDateTime.now()));
      }
      String[] snapshotOpParts = snapshotOp.split(":");
      switch (snapshotOpParts[0]) {
        case "rotate":
          rotate(stats, fsWrapper, path, snapshotOpParts[1], Integer.parseInt(snapshotOpParts[2]));
          break;
        case "rename":
          PrometheusUtils.runAndRecord(stats, RENAME_SNAPSHOT_OPERATION, () -> fsWrapper.renameSnapshot(path, snapshotOpParts[1], snapshotOpParts[2]));
          break;
        default:
          log.warn("Unsupported snapshot action: " + opConf.getActions());
          throw new IllegalArgumentException("Unsupported snapshot action : " + opConf.getActions());
      }
    }
  }

  private static void rotate(StatisticsDTO stats, FsWrapper fsWrapper, String path, String snapshotName, int numberOfSnapshots) {
    if(numberOfSnapshots <= 0 || numberOfSnapshots >= 1_000_000) {
      throw new IllegalArgumentException("Number of snapshots to rotate should be positive and less than 1 mln");
    }

    String oldestSnapshot = snapshotName + "_" + numberOfSnapshots;
    PrometheusUtils.runAndRecord(stats, DELETE_SNAPSHOT_OPERATION, () -> fsWrapper.deleteSnapshot(path, oldestSnapshot));

    for(int i = numberOfSnapshots - 1 ; i > 0; i--) {
      String fromName = snapshotName + "_" + i;
      String toName = snapshotName + "_" + (i + 1);
      PrometheusUtils.runAndRecord(stats, RENAME_SNAPSHOT_OPERATION, () -> fsWrapper.renameSnapshot(path, fromName, toName));
    }

    String newShapshot = snapshotName + "_1";
    PrometheusUtils.runAndRecord(stats, CREATE_SNAPSHOT_OPERATION, () -> fsWrapper.createSnapshot(path, newShapshot));
  }
}