package team.unison.perf.loader;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PerfLoaderUtils;
import team.unison.remote.GenericWorker;
import team.unison.remote.GenericWorkerBuilder;
import team.unison.remote.Utils;
import team.unison.remote.WorkerException;

public final class FsLoader implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(FsLoader.class);

  /**
   * loader may fill paths one by one - one batch - one path (type='window') or
   * spread load so every batch contains files for different paths (type='spread').
   * default type is 'window'
   */
  public enum Type {
    WINDOW, SPREAD
  }

  private final String name;
  private final Map<String, String> conf;
  private final Collection<GenericWorkerBuilder> genericWorkerBuilders;
  private final int threads;
  private final boolean useTmpFile;
  private final Duration loadDelay;
  private final Duration commandDelay;
  private final int count;
  private final Duration period;
  private final List<String> paths;
  private final List<String> subdirs;
  private final String filesSizesDistribution;
  private final String filesSuffixesDistribution;
  private final String fill;
  private final int totalBatches;
  private final int filesInBatch;
  private final List<Map<String, Long>> filesInBatches;

  private final List<Long> filesSizes = new ArrayList<>();
  private final List<String> filesSuffixes = new ArrayList<>();
  private final List<List<long[]>> loadResults = new ArrayList<>();
  private final List<Duration> loadDurations = new ArrayList<>();
  private final List<Map<String, String>> workload;

  private final Random random;

  private final Type type;

  FsLoader(String name, Map<String, String> conf, Collection<GenericWorkerBuilder> genericWorkerBuilders,
           int threads, List<String> paths, List<Map<String, String>> workload,
           int subdirsWidth, int subdirsDepth, String subdirsFormat, int batches, boolean useTmpFile,
           Duration loadDelay, Duration commandDelay,
           int count, Duration period,
           int filesInBatch, String filesSizesDistribution, String filesSuffixesDistribution, String fill,
           Random random, Type type) {
    this.name = name;
    this.conf = conf;
    this.genericWorkerBuilders = new ArrayList<>(genericWorkerBuilders);
    this.threads = threads;
    this.paths = new ArrayList<>(paths);
    this.type = type;
    subdirs = initSubdirs(Collections.singletonList(""), subdirsWidth, subdirsDepth, subdirsFormat);
    this.totalBatches = batches * paths.size();
    this.useTmpFile = useTmpFile;
    this.loadDelay = loadDelay;
    this.commandDelay = commandDelay;
    this.count = count;
    this.period = period;
    this.filesInBatch = filesInBatch;
    // for 'toString'
    this.filesSizesDistribution = filesSizesDistribution;
    this.filesSuffixesDistribution = filesSuffixesDistribution;
    this.fill = fill;
    this.random = random;

    if (workload == null || workload.isEmpty()) {
      Map<String, String> defaultCommand = new HashMap<>();
      defaultCommand.put("operation", "put");
      this.workload = Collections.singletonList(defaultCommand);
    } else {
      this.workload = new ArrayList<>(workload);
    }

    List<Long> filesSizesPartial = distributionToList(filesSizesDistribution).stream().map(FsLoader::toSize).collect(Collectors.toList());
    List<String> filesSuffixesPartial = distributionToList(filesSuffixesDistribution);

    // If we have 100 subdirs and 100 suffixes - suffixes will be distributed not evenly but subdir 1 will always have suffix 1, subdir 2
    // - suffix 2, etc. Shuffling suffixes once doesn't help - all files in a subdirectory will have one suffix.
    // To prevent this let's create more even distribution, every segment of it will have required distribution of suffixes
    for (int i = 0; i < 100; i++) {
      Collections.shuffle(filesSizesPartial, random);
      filesSizes.addAll(filesSizesPartial);
      Collections.shuffle(filesSuffixesPartial, random);
      filesSuffixes.addAll(filesSuffixesPartial);
    }

    filesInBatches = generateFilesInBatches();

    validate();
  }

  private List<Map<String, Long>> generateFilesInBatches() {
    List<Map<String, Long>> ret = new ArrayList<>(totalBatches);
    for (int batchNo = 0; batchNo < totalBatches; batchNo++) {
      Map<String, Long> batchFiles = new HashMap<>();
      for (int i = 0; i < filesInBatch; i++) {
        long globalNo = ((long) batchNo) * filesInBatch + i;
        String path = (type == Type.WINDOW) ? paths.get(batchNo % paths.size()) : paths.get((int) (globalNo % paths.size()));
        String subdir = subdirs.get((int) (globalNo % subdirs.size()));
        // same suffix and size for all paths in case of SPREAD load to make sure that each path has the same set of suffixes in the end
        // and each path in the target FS has the same size if suffixes are used for exclusion
        long sizeSuffixNo = (type == Type.WINDOW) ? globalNo : globalNo / paths.size();
        String suffix = filesSuffixes.isEmpty() ? "" : filesSuffixes.get((int) (sizeSuffixNo % filesSuffixes.size()));
        long fileSize = filesSizes.get((int) (sizeSuffixNo % filesSizes.size()));
        String fileName = randomFileName(random);

        String fullFileName = path + subdir + "/" + fileName + suffix;
        batchFiles.put(fullFileName, fileSize);
      }
      ret.add(batchFiles);
    }
    return ret;
  }

  private List<String> distributionToList(String distribution) {
    if (distribution == null || distribution.isEmpty()) {
      return new ArrayList<>();
    }
    Map<String, Integer> keyValue = new HashMap<>();
    Arrays.stream(distribution.split(",")).map(s -> s.trim().split(":")).forEach(
        a -> keyValue.put(a.length > 1 ? a[1].trim() : "", Integer.parseInt(a[0].trim())));

    // 50:50 distribution is effectively equal to 1:1 - find GCD for all values
    BigInteger gcd = null;
    for (Integer count : keyValue.values()) {
      gcd = (gcd == null) ? BigInteger.valueOf(count) : gcd.gcd(BigInteger.valueOf(count));
    }

    // reduce all values by GCD
    for (Map.Entry<String, Integer> entry : keyValue.entrySet()) {
      entry.setValue(entry.getValue() / gcd.intValue());
    }

    // now convert a = x, b = y to a,a,...,a (x times),b,b,...,b (y times)
    List<String> list = new ArrayList<>();

    for (Map.Entry<String, Integer> entry : keyValue.entrySet()) {
      IntStream.range(0, entry.getValue()).forEach(i -> list.add(entry.getKey()));
    }
    return list;
  }

  private List<String> initSubdirs(List<String> paths, int subdirsWidth, int subdirsDepth, String subdirsFormat) {
    if (subdirsDepth <= 0) {
      return paths;
    }

    List<String> subdirs = new ArrayList<>();
    for (String path : paths) {
      for (int i = 0; i < subdirsWidth; i++) {
        subdirs.add(path + "/" + String.format(subdirsFormat, i));
      }
    }

    return initSubdirs(subdirs, subdirsWidth, subdirsDepth - 1, subdirsFormat);
  }

  private void validate() {
    if (genericWorkerBuilders == null || genericWorkerBuilders.isEmpty()) {
      throw new IllegalArgumentException("Empty genericWorkerBuilders");
    }

    if (paths == null || paths.isEmpty()) {
      throw new IllegalArgumentException("Empty paths");
    }

    if (filesSizes.isEmpty()) {
      throw new IllegalArgumentException("Empty filesSizes");
    }
  }

  @Override
  public void run() {
    List<GenericWorker> genericWorkers = genericWorkerBuilders.parallelStream()
        .map(GenericWorkerBuilder::get)
        .collect(Collectors.toList());

    for (int i = 0; i < count; i++) {
      if (i != 0) {
        log.info("Waiting " + period + " between loads");
        Utils.sleep(period.toMillis());
      }
      log.info("Start load {}{}", name, (count == 1) ? "" : ": " + (i + 1));
      runSingle(genericWorkers);
      printSummary();
    }
    log.info("Load {} ended", name);
  }

  private void runSingle(List<GenericWorker> genericWorkers) {
    ExecutorService executorService = Executors.newFixedThreadPool(genericWorkers.size());
    List<GenericWorker> workersCopy = new ArrayList<>(genericWorkers);
    loadResults.clear();

    try {
      try {
        if (workload.get(0).containsKey("operationType")) { // mixed workload
          List<Callable<List<long[]>>> callables = filesInBatches.stream()
              .map(batch -> ((Callable<List<long[]>>) () -> runMixedWorkload(workersCopy, batch)))
              .collect(Collectors.toList());
          List<Future<List<long[]>>> futures = executorService.invokeAll(callables);
          List<List<long[]>> commandResults = futures.stream().map(f -> {
            try {
              return f.get();
            } catch (Exception e) {
              throw WorkerException.wrap(e);
            }
          }).collect(Collectors.toList());
          for (List<long[]> commandResult : commandResults) {
            for (int i = 0; i < commandResult.size(); i++) {
              if (loadResults.size() <= i) {
                loadResults.add(new ArrayList<>());
              }
              loadResults.get(i).add(commandResult.get(i));
            }
          }
        } else { // regular workload
          for (Map<String, String> command : workload) {
            Instant before = Instant.now();
            List<Callable<long[]>> callables = filesInBatches.stream()
                .map(batch -> ((Callable<long[]>) () -> runCommand(workersCopy, command, batch)))
                .collect(Collectors.toList());
            List<Future<long[]>> futures = executorService.invokeAll(callables);
            List<long[]> commandResults = futures.stream().map(f -> {
              try {
                return f.get();
              } catch (Exception e) {
                throw WorkerException.wrap(e);
              }
            }).collect(Collectors.toList());
            loadResults.add(commandResults);
            loadDurations.add(Duration.between(before, Instant.now()));
            genericWorkers.parallelStream().forEach(gw -> {
              try {
                gw.getAgent().clearStatistics();
              } catch (IOException e) {
                log.warn("Error clearing statistics in agent at host {}", gw.getHost());
              }
            });
            boolean lastCommand = workload.indexOf(command) == workload.size() - 1;
            if (!commandDelay.isZero() && !lastCommand) {
              log.info("Waiting {} seconds between commands", commandDelay.getSeconds());
              Thread.sleep(commandDelay.toMillis());
            }
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } finally {
      executorService.shutdownNow();
    }
  }

  private List<long[]> runMixedWorkload(List<GenericWorker> workersCopy, Map<String, Long> batch) {
    GenericWorker genericWorker;
    List<long[]> loadResult;
    synchronized (workersCopy) {
      genericWorker = workersCopy.remove(workersCopy.size() - 1);
    }
    try {
      batch.put(FsLoaderBatchRemote.THREAD_NUMBER_KEY, (long) threads);
      batch.put(FsLoaderBatchRemote.USE_TMP_FILE_KEY, useTmpFile ? 1 : 0L);
      batch.put(FsLoaderBatchRemote.LOAD_DELAY_IN_MILLIS_KEY, loadDelay.toMillis());
      batch.put(FsLoaderBatchRemote.FILL_KEY, "random".equalsIgnoreCase(fill) ? -1 : Long.parseLong(fill));

      log.info("Start mixed workload at host {}", genericWorker.getHost());
      Instant before = Instant.now();
      loadResult = genericWorker.getAgent().runMixedWorkload(conf, batch, workload);
      log.info("End mixed workload at host {}, batch took {}", genericWorker.getHost(), Duration.between(before, Instant.now()));
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    } finally {
      synchronized (workersCopy) {
        workersCopy.add(genericWorker);
      }
    }

    return loadResult;
  }

  private long[] runCommand(List<GenericWorker> workersCopy, Map<String, String> command, Map<String, Long> batch) {
    GenericWorker genericWorker;
    long[] commandResult;
    synchronized (workersCopy) {
      genericWorker = workersCopy.remove(workersCopy.size() - 1);
    }
    try {
      batch.put(FsLoaderBatchRemote.THREAD_NUMBER_KEY, (long) threads);
      batch.put(FsLoaderBatchRemote.USE_TMP_FILE_KEY, useTmpFile ? 1 : 0L);
      batch.put(FsLoaderBatchRemote.LOAD_DELAY_IN_MILLIS_KEY, loadDelay.toMillis());
      batch.put(FsLoaderBatchRemote.FILL_KEY, "random".equalsIgnoreCase(fill) ? -1 : Long.parseLong(fill));

      log.info("Start batch for command '{}' at host {}", command.get("operation"), genericWorker.getHost());
      Instant before = Instant.now();
      try {
        commandResult = genericWorker.getAgent().runCommand(conf, batch, command);
      } catch (IOException e) {
        throw WorkerException.wrap(e);
      }
      log.info("End batch for command '{}' at host {}, batch took {}", command.get("operation"),
               genericWorker.getHost(), Duration.between(before, Instant.now()));
    } finally {
      synchronized (workersCopy) {
        workersCopy.add(genericWorker);
      }
    }

    return commandResult;
  }

  //  Concurrency: 20
  //  Total number of requests: 200
  //  Failed requests: 200
  //  Total elapsed time: 7.434925027s
  //  Average request time: 722.208458ms
  //  Minimum request time: 622.72ms
  //  Maximum request time: 889.07ms
  //  Nominal requests/s: 27.7
  //  Actual requests/s: 26.9
  //  Content throughput: 0.000000 MB/s
  //  Average Object Size: 0
  //  Total Object Size: 0
  //  Response Time Percentiles
  //    50     :   718.67 ms
  //    75     :   756.07 ms
  //    90     :   778.67 ms
  //    95     :   804.23 ms
  //    99     :   843.83 ms
  //    99.9   :   889.07 ms

  public void printSummary() {
    String header = "Loader: " + name;
    for (int cmdNo = 0; cmdNo < workload.size(); cmdNo++) {
      Map<String, String> command = workload.get(cmdNo);
      long[] globalResults = loadResults.get(cmdNo).stream().flatMapToLong(Arrays::stream).toArray();
      Duration duration = loadDurations.size() < cmdNo ? null : loadDurations.get(cmdNo);
      // TODO: proper counting of duration!!!
      // see PrometheusUtils.PUSH_PERIOD_SECONDS
      if (duration != null && cmdNo != 0) {
        duration.minus(22500, ChronoUnit.MILLIS);
      }
      long averageObjectSize = 0;
      String op = command.containsKey("operation") ? command.get("operation") : command.get("operationType");
      if ("put".equalsIgnoreCase(op) || "get".equalsIgnoreCase(op)) {
        averageObjectSize = (long) filesSizes.stream().mapToLong(l -> l).average().orElse(0);
      }

      PerfLoaderUtils.printStatistics(header, op, conf == null ? null : conf.get("s3.uri"),
                                      threads * genericWorkerBuilders.size(), averageObjectSize, globalResults, duration);
    }
  }

  private static long toSize(String s) {
    long unit = 1;
    switch (s.charAt(s.length() - 1)) {
      case 'K':
      case 'k':
        unit = 1024;
        break;
      case 'M':
      case 'm':
        unit = 1024 * 1024;
        break;
      case 'G':
      case 'g':
        unit = 1024 * 1024 * 1024;
        break;
      default:
    }

    return unit * Long.parseLong(unit == 1 ? s : s.substring(0, s.length() - 1));
  }

  private static String randomFileName(Random rnd) {
    char[] arr = new char[10];

    for (int i = 0; i < arr.length; i++) {
      arr[i] = (char) ('a' + rnd.nextInt('z' + 1 - 'a'));
    }
    return new String(arr);
  }

  @Override
  public String toString() {
    return "FsLoader{" + "threads=" + threads + ", paths=" + paths
        + ", subdirs count=" + subdirs.size()
        + ", subdirs=" + subdirs.subList(0, Math.min(subdirs.size(), 20))
        + ", totalBatches=" + totalBatches + ", useTmpFile=" + useTmpFile + ", loadDelay=" + loadDelay
        + ", count=" + count + ", period=" + period
        + ", filesInBatch=" + filesInBatch + ", filesSizes=" + filesSizesDistribution + ", filesSuffixes=" + filesSuffixesDistribution
        + ", fill=" + fill
        + '}';
  }
}