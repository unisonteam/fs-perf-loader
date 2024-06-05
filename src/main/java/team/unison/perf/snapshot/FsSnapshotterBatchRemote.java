/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.FsRemoteWrapper;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.WorkerException;
import team.unison.transfer.FsSnapshotterDataForOperation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class FsSnapshotterBatchRemote extends FsRemoteWrapper {
  private static final Logger log = LoggerFactory.getLogger(FsSnapshotterBatchRemote.class);

  private static final String MAKE_SNAPSHOTTABLE_OPERATION = "make_snapshottable";
  private static final String CREATE_SNAPSHOT_OPERATION = "create_snapshot";
  private static final String DELETE_SNAPSHOT_OPERATION = "delete_snapshot";
  private static final String RENAME_SNAPSHOT_OPERATION = "rename_snapshot";

  private static final ConcurrentHashMap<String, Boolean> SNAPSHOT_PATHS = new ConcurrentHashMap<>();

  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
  private final FsSnapshotterDataForOperation data;

  public FsSnapshotterBatchRemote(@Nonnull FsSnapshotterDataForOperation data) {
    super(data.threadCount);
    this.data = data;
  }

  public StatisticsDTO snapshot(@Nullable List<String> paths) {
    StatisticsDTO stats = new StatisticsDTO();
    if (paths == null || paths.isEmpty()) {
      return stats;
    }

    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (String path : paths) {
      futures.add(CompletableFuture.runAsync(() -> {
        FsWrapper fsWrapper = FsWrapperFactory.get(data.conf);
        snapshotActions(fsWrapper, path, stats);
      }, executor));
    }
    try {
      CompletableFuture.allOf(new CompletableFuture[futures.size()]).join();
    } catch (Exception e) {
      throw WorkerException.wrap(e);
    }
    return stats;
  }

  private void snapshotActions(FsWrapper fsWrapper, String path, StatisticsDTO stats) {
    SNAPSHOT_PATHS.computeIfAbsent(path, p ->
            PrometheusUtils.runAndRecord(stats, MAKE_SNAPSHOTTABLE_OPERATION, () -> fsWrapper.allowSnapshot(path)));

    for (String snapshotOp : data.actions.split(",")) {
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
          log.warn("Unsupported snapshot action: " + data.actions);
          throw new IllegalArgumentException("Unsupported snapshot action : " + data.actions);
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