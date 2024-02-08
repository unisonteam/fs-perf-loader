/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.Utils;
import team.unison.remote.WorkerException;

public class FsLoaderBatchRemote {
  private static final int WRITE_DATA_ARRAY_SIZE = 1024 * 1024;
  private static final AtomicInteger FS_WRAPPER_COUNTER = new AtomicInteger();
  private static final int MAX_FILE_POOL_SIZE = 1_000_000; // approximate

  public static StatisticsDTO runCommand(Map<String, String> conf, Map<String, Long> batch, Map<String, String> command,
                                         FsLoaderOperationConf opConf) {
    StatisticsDTO stats = new StatisticsDTO();
    if (batch == null || batch.isEmpty()) {
      return stats;
    }

    byte[] barr = getData(opConf.getFillChar());

    ExecutorService executorService = Executors.newFixedThreadPool(opConf.getThreadCount());
    String randomPath = batch.keySet().stream().findFirst().get();
    List<FsWrapper> fsWrappers = FsWrapperFactory.get(randomPath, conf);

    List<Callable<Object>> callables = batch.entrySet().stream()
        .map(entry -> Executors.callable(
            () -> {
              long start = System.nanoTime();
              boolean success = runCommand(randomFsWrapper(fsWrappers), entry.getKey(), entry.getValue(), barr, opConf.isUsetmpFile(),
                                           command);
              long elapsed = System.nanoTime() - start;
              PrometheusUtils.record(stats, command.get("operation"), entry.getValue(), success, elapsed);
              if (opConf.getLoadDelayInMillis() > 0) {
                Utils.sleep(opConf.getLoadDelayInMillis());
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

  public static StatisticsDTO runMixedWorkload(Map<String, String> conf, Map<String, Long> batch, List<Map<String, String>> workload,
                                               FsLoaderOperationConf opConf) {
    StatisticsDTO stats = new StatisticsDTO();
    if (batch.isEmpty()) {
      return stats;
    }

    byte[] barr = getData(opConf.getFillChar());

    AtomicLong pos = new AtomicLong();
    ExecutorService executorService = Executors.newFixedThreadPool(opConf.getThreadCount());
    String randomPath = batch.keySet().stream().findFirst().get();
    List<FsWrapper> fsWrappers = FsWrapperFactory.get(randomPath, conf);
    List<String> filePool = Collections.synchronizedList(new ArrayList<>());

    List<Callable<StatisticsDTO>> callables = batch.entrySet().stream()
        .map(entry -> ((Callable<StatisticsDTO>) () ->
            runWorkloadForSingleFile(randomFsWrapper(fsWrappers), entry.getKey(), entry.getValue(), barr, opConf, workload, pos, filePool)
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

  private static StatisticsDTO runWorkloadForSingleFile(FsWrapper fsWrapper, String fileName, Long fileSize, byte[] barr,
                                                        FsLoaderOperationConf opConf, List<Map<String, String>> workload, AtomicLong seq,
                                                        List<String> filePool) {

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
        boolean success = runCommand(fsWrapper, fileName, fileSize, barr, opConf.isUsetmpFile(), command);
        long elapsed = System.nanoTime() - start;
        PrometheusUtils.record(stats, command.get("operationType"), fileSize, success, elapsed);
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

  private static byte[] getData(long fill) {
    byte[] barr = new byte[WRITE_DATA_ARRAY_SIZE];

    // 0 - no action - leave array filled with zeroes
    if (fill < 0) {
      ThreadLocalRandom.current().nextBytes(barr);
    } else if (fill > 0) {
      Arrays.fill(barr, (byte) fill);
    }

    return barr;
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

  private static FsWrapper randomFsWrapper(List<FsWrapper> fsWrappers) {
    return fsWrappers.get(FS_WRAPPER_COUNTER.getAndIncrement() % fsWrappers.size());
  }
}