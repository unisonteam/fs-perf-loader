/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.loader;

import team.unison.perf.FsRemoteWrapper;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.Utils;
import team.unison.remote.WorkerException;
import team.unison.transfer.FsLoaderDataForOperation;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class FsLoaderBatchRemote extends FsRemoteWrapper {
  private static final int MAX_FILE_POOL_SIZE = 1_000_000; // approximate
  private static final int WRITE_DATA_ARRAY_SIZE = 1024 * 1024;
  private final byte[] writableData;
  private final FsLoaderDataForOperation data;

  public FsLoaderBatchRemote(@Nonnull FsLoaderDataForOperation data) {
    super(data.threadCount);
    this.data = data;
    writableData = generateData(data.fill);
  }

  public StatisticsDTO runCommand(
      @Nonnull Map<String, Long> batch,
      @Nonnull Map<String, String> command
  ) {
    StatisticsDTO stats = new StatisticsDTO();
    if (batch.isEmpty()) {
      return stats;
    }

    List<CompletableFuture<Void>> futureList = new ArrayList<>();
    for (Map.Entry<String, Long> entry : batch.entrySet()) {
      futureList.add(CompletableFuture.runAsync(
          () -> {
            long start = System.nanoTime();
            FsWrapper fsWrapper = FsWrapperFactory.get(data.conf);
            boolean success = runCommand(fsWrapper, entry.getKey(), entry.getValue(), writableData, data.useTmpFile, command);
            long elapsed = System.nanoTime() - start;
            PrometheusUtils.record(stats, command.get("operation"), elapsed, success, entry.getValue());
            if (data.loadDelayInMillis > 0) {
              Utils.sleep(data.loadDelayInMillis);
            }
          },
          executor
      ));
    }

    try {
      CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
    } catch (Exception e) {
      throw WorkerException.wrap(e);
    }
    return stats;
  }

  public StatisticsDTO runMixedWorkload(
      @Nonnull Map<String, Long> batch,
      @Nonnull List<Map<String, String>> workload
  ) {
    StatisticsDTO stats = new StatisticsDTO();
    if (batch.isEmpty()) {
      return stats;
    }

    AtomicLong pos = new AtomicLong();
    List<String> filePool = Collections.synchronizedList(new ArrayList<>());

    List<CompletableFuture<StatisticsDTO>> futures = new ArrayList<>();
    for (Map.Entry<String, Long> entry : batch.entrySet()) {
      futures.add(CompletableFuture.supplyAsync(() -> {
            FsWrapper fsWrapper = FsWrapperFactory.get(data.conf);
            return runWorkloadForSingleFile(fsWrapper, entry.getKey(), entry.getValue(), workload, pos, filePool);
          }, executor
      ));
    }

    try {
      List<StatisticsDTO> batchResult = futures.stream().map(f -> {
        try {
          return f.get();
        } catch (Exception e) {
          throw WorkerException.wrap(e);
        }
      }).collect(Collectors.toList());
      for (StatisticsDTO singleBatchResult : batchResult) {
        stats.add(singleBatchResult);
      }
    } catch (Exception e) {
      throw WorkerException.wrap(e);
    }

    return stats;
  }

  private StatisticsDTO runWorkloadForSingleFile(
      FsWrapper fsWrapper,
      String fileName,
      long fileSize,
      List<Map<String, String>> workload,
      AtomicLong seq,
      List<String> filePool
  ) {
    // json converts all numbers to double
    int firstCommandRatio = (int) Double.parseDouble(workload.get(0).get("ratio"));
    long commandSeq = seq.incrementAndGet();
    StatisticsDTO stats = new StatisticsDTO();

    for (Map<String, String> command : workload) {
      int commandRatio = (int) Double.parseDouble(command.get("ratio"));
      boolean randomRead = Boolean.parseBoolean(command.get("randomRead"));
      if ("put".equals(command.get("operationType")) && filePool.size() < MAX_FILE_POOL_SIZE) {
        filePool.add(fileName);
      } else if ("delete".equals(command.get("operationType"))) {
        filePool.remove(fileName);
      } else if (randomRead) {
        synchronized (filePool) {
          // don't use latest files - files may be deleted before read causing errors
          if (filePool.size() > data.threadCount) {
            fileName = filePool.get(ThreadLocalRandom.current().nextInt(filePool.size() - data.threadCount));
          }
        }
      }
      int baseRunCount = commandRatio / firstCommandRatio;
      int variableRunCount = commandSeq % firstCommandRatio < commandRatio % firstCommandRatio ? 1 : 0;
      int timesToRunCommand = baseRunCount + variableRunCount;
      for (int i = 0; i < timesToRunCommand; i++) {
        long start = System.nanoTime();
        boolean success = runCommand(fsWrapper, fileName, fileSize, writableData, data.useTmpFile, command);
        long elapsed = System.nanoTime() - start;
        PrometheusUtils.record(stats, command.get("operationType"), elapsed, success, fileSize);
        if (!success) {
          filePool.remove(fileName);
        }
        if (data.loadDelayInMillis > 0) {
          Utils.sleep(data.loadDelayInMillis);
        }
      }
    }

    return stats;
  }


  private static boolean runCommand(FsWrapper fsWrapper, String path, long size, byte[] data, boolean useTmpFile,
                                    Map<String, String> command) {
    String op = command.containsKey("operation") ? command.get("operation") : command.get("operationType");
    if ("put".equalsIgnoreCase(op)) {
      return fsWrapper.create(command.get("bucket"), path, size, data, useTmpFile);
    } else if ("copy".equalsIgnoreCase(op)) {
      return fsWrapper.copy(command.get("copy-source-bucket"), command.get("bucket"), path);
    } else if ("get".equalsIgnoreCase(op)) {
      return fsWrapper.get(command.get("bucket"), path);
    } else if ("head".equalsIgnoreCase(op)) {
      return fsWrapper.head(command.get("bucket"), path);
    } else if ("delete".equalsIgnoreCase(op)) {
      return fsWrapper.delete(command.get("bucket"), path);
    }
    throw new IllegalArgumentException("command " + op + " is not supported");
  }

  private static @Nonnull byte[] generateData(@Nonnull String fillValue) {
    long fillChar = ("random".equalsIgnoreCase(fillValue) ? -1 : Long.parseLong(fillValue));
    byte[] data = new byte[WRITE_DATA_ARRAY_SIZE];

    // 0 - no action - leave array filled with zeroes
    if (fillChar < 0) {
      ThreadLocalRandom.current().nextBytes(data);
    } else if (fillChar > 0) {
      Arrays.fill(data, (byte) fillChar);
    }
    return data;
  }
}