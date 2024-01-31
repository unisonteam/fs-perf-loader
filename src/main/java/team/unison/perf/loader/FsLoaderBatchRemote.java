/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.loader;

import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.remote.Utils;
import team.unison.remote.WorkerException;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class FsLoaderBatchRemote {
  private static final int WRITE_DATA_ARRAY_SIZE = 1024 * 1024;
  private static final AtomicInteger FS_WRAPPER_COUNTER = new AtomicInteger();
  private static final int MAX_FILE_POOL_SIZE = 1_000_000; // approximate

  // these filenames are not valid - use them to pass extra control metadata
  static final String THREAD_NUMBER_KEY = ":";
  // C style: 0 is false, else is true
  static final String USE_TMP_FILE_KEY = "::";
  static final String LOAD_DELAY_IN_MILLIS_KEY = ":::";
  static final String FILL_KEY = "::::";

  public static long[] runCommand(Map<String, String> conf, Map<String, Long> batch, Map<String, String> command, FsLoaderOperationConf opConf) {
    if (batch.isEmpty()) {
      return new long[0];
    }

    long fill = batch.remove(FILL_KEY);
    byte[] barr = getData(fill);

    long[] ret = new long[batch.size()];
    AtomicInteger pos = new AtomicInteger();

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
                      PrometheusUtils.record(command.get("operation"), entry.getValue(), success, elapsed / 1_000_000);
                      ret[pos.getAndIncrement()] = elapsed * (success ? 1 : -1);
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
    return ret;
  }

  public static List<long[]> runMixedWorkload(Map<String, String> conf, Map<String, Long> batch, List<Map<String, String>> workload, FsLoaderOperationConf opConf) {
    if (batch.isEmpty()) {
      return Collections.EMPTY_LIST;
    }

    long fill = batch.remove(FILL_KEY);
    byte[] barr = getData(fill);

    List<List<long[]>> batchesResults = new ArrayList<>();
    for (Map<String, String> map : workload) {
      batchesResults.add(new ArrayList<>());
    }

    AtomicLong pos = new AtomicLong();
    ExecutorService executorService = Executors.newFixedThreadPool(opConf.getThreadCount());
    String randomPath = batch.keySet().stream().findFirst().get();
    List<FsWrapper> fsWrappers = FsWrapperFactory.get(randomPath, conf);
    List<String> filePool = Collections.synchronizedList(new ArrayList<>());

    List<Callable<List<long[]>>> callables = batch.entrySet().stream()
            .map(entry -> ((Callable<List<long[]>>) () ->
                    runWorkloadForSingleFile(randomFsWrapper(fsWrappers), entry.getKey(), entry.getValue(), barr, opConf, workload, pos, filePool)
            )).collect(Collectors.toList());

    try {
      List<Future<List<long[]>>> futures = executorService.invokeAll(callables);
      List<List<long[]>> batchResult = futures.stream().map(f -> {
        try {
          return f.get();
        } catch (Exception e) {
          throw WorkerException.wrap(e);
        }
      }).collect(Collectors.toList());
      for (List<long[]> singleBatchResult : batchResult) {
        for (int i = 0; i < singleBatchResult.size(); i++) {
          batchesResults.get(i).add(singleBatchResult.get(i));
        }
      }
    } catch (InterruptedException e) {
      throw WorkerException.wrap(e);
    } finally {
      executorService.shutdownNow();
    }

    List<long[]> ret = new ArrayList<>();
    for (List<long[]> batchesResult : batchesResults) {
      long[] commandResult = batchesResult.stream().flatMapToLong(Arrays::stream).toArray();
      ret.add(commandResult);
    }

    return ret;
  }

  // returns results (time in nanoseconds) for commands in workload - if workload contains 3 commands - there will be list of 3 arrays,
  // each array contains
  // execution time for each command
  // for example: commands:put, get, delete - return data will be
  // [100]
  // [10, 20, 10, 20, 10, 50]
  // [30]
  private static List<long[]> runWorkloadForSingleFile(FsWrapper fsWrapper, String fileName, Long fileSize, byte[] barr,
                                                       FsLoaderOperationConf opConf, List<Map<String, String>> workload, AtomicLong seq,
                                                       List<String> filePool) {

    // json converts all numbers to double
    int firstCommandRatio = (int) Double.parseDouble(workload.get(0).get("ratio"));
    List<long[]> ret = new ArrayList<>();
    long commandSeq = seq.incrementAndGet();

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
      long[] commandResults = new long[timesToRunCommand];
      ret.add(commandResults);
      for (int i = 0; i < timesToRunCommand; i++) {
        long start = System.nanoTime();
        boolean success = runCommand(fsWrapper, fileName, fileSize, barr, opConf.isUsetmpFile(), command);
        long elapsed = System.nanoTime() - start;
        PrometheusUtils.record(command.get("operationType"), fileSize, success, elapsed / 1_000_000);
        if (!success) {
          filePool.remove(fileName);
        }
        commandResults[i] = elapsed * (success ? 1 : -1);
        if (opConf.getLoadDelayInMillis() > 0) {
          Utils.sleep(opConf.getLoadDelayInMillis());
        }
      }
    }

    return ret;
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