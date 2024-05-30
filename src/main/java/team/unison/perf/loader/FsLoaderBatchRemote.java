/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.loader;

import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.Utils;
import team.unison.remote.WorkerException;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class FsLoaderBatchRemote {
  private static final int MAX_FILE_POOL_SIZE = 1_000_000; // approximate
  private static final int WRITE_DATA_ARRAY_SIZE = 1024 * 1024;
  private final byte[] writableData = new byte[WRITE_DATA_ARRAY_SIZE];

  public StatisticsDTO runCommand(
      @Nonnull ExecutorService executorService,
      @Nonnull Map<Thread, FsWrapper> threadToFsWrapperMap,
      @Nonnull Map<String, Long> batch,
      @Nonnull Map<String, String> command,
      @Nonnull FsLoaderOperationConf opConf
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
            FsWrapper fsWrapper = threadToFsWrapperMap.get(Thread.currentThread());
            boolean success = runCommand(fsWrapper, entry.getKey(), entry.getValue(), writableData, opConf.isUsetmpFile(),
                command);
            long elapsed = System.nanoTime() - start;
            PrometheusUtils.record(stats, command.get("operation"), elapsed, success, entry.getValue());
            if (opConf.getLoadDelayInMillis() > 0) {
              Utils.sleep(opConf.getLoadDelayInMillis());
            }
          },
          executorService
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
      ExecutorService executorService,
      Map<Thread, FsWrapper> fsWrapperList,
      Map<String, Long> batch,
      List<Map<String, String>> workload,
      FsLoaderOperationConf opConf
  ) {
    StatisticsDTO stats = new StatisticsDTO();
    if (batch.isEmpty()) {
      return stats;
    }

    AtomicLong pos = new AtomicLong();
    List<String> filePool = Collections.synchronizedList(new ArrayList<>());

    List<Callable<StatisticsDTO>> callables = batch.entrySet().stream()
            .map(entry -> ((Callable<StatisticsDTO>) () -> {
              FsWrapper fsWrapper = fsWrapperList.get(Thread.currentThread());
              return runWorkloadForSingleFile(fsWrapper, entry.getKey(), entry.getValue(), writableData, opConf,
                  workload, pos, filePool);
            }
            )).collect(Collectors.toList());

    try {
      List<Future<StatisticsDTO>> futures = executorService.invokeAll(callables);
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
    } catch (InterruptedException e) {
      throw WorkerException.wrap(e);
    } finally {
      executorService.shutdownNow();
    }

    return stats;
  }

  private static StatisticsDTO runWorkloadForSingleFile(
      FsWrapper fsWrapper,
      String fileName,
      long fileSize,
      byte[] writableData,
      FsLoaderOperationConf opConf,
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
          if (filePool.size() > opConf.getThreadCount()) {
            fileName = filePool.get(ThreadLocalRandom.current().nextInt(filePool.size() - opConf.getThreadCount()));
          }
        }
      }
      int baseRunCount = commandRatio / firstCommandRatio;
      int variableRunCount = commandSeq % firstCommandRatio < commandRatio % firstCommandRatio ? 1 : 0;
      int timesToRunCommand = baseRunCount + variableRunCount;
      for (int i = 0; i < timesToRunCommand; i++) {
        long start = System.nanoTime();
        boolean success = runCommand(fsWrapper, fileName, fileSize, writableData, opConf.isUsetmpFile(), command);
        long elapsed = System.nanoTime() - start;
        PrometheusUtils.record(stats, command.get("operationType"), elapsed, success, fileSize);
        if (!success) {
          filePool.remove(fileName);
        }
        if (opConf.getLoadDelayInMillis() > 0) {
          Utils.sleep(opConf.getLoadDelayInMillis());
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

  public void fillData(@Nonnull String fill) {
    long fillChar = ("random".equalsIgnoreCase(fill) ? -1 : Long.parseLong(fill));

    // 0 - no action - leave array filled with zeroes
    if (fillChar < 0) {
      ThreadLocalRandom.current().nextBytes(writableData);
    } else if (fillChar > 0) {
      Arrays.fill(writableData, (byte) fillChar);
    }
  }
}