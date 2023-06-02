package team.unison.perf.loader;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private final int count;
  private final Duration period;
  private final List<String> paths;
  private final List<String> subdirs;
  private final String filesSizesDistribution;
  private final String filesSuffixesDistribution;
  private final String fill;
  private volatile int batchesLeft;
  private final int totalBatches;
  private final int filesInBatch;

  private final List<Long> filesSizes = new ArrayList<>();
  private final List<String> filesSuffixes = new ArrayList<>();
  private final List<long[]> loadResults = new ArrayList<>();
  private final List<Map<String, String>> workload;

  private final Random random;

  private final Type type;

  FsLoader(String name, Map<String, String> conf, Collection<GenericWorkerBuilder> genericWorkerBuilders,
           int threads, List<String> paths, List<Map<String, String>> workload,
           int subdirsWidth, int subdirsDepth, String subdirsFormat, int batches, boolean useTmpFile, Duration loadDelay,
           int count, Duration period,
           int filesInBatch, String filesSizesDistribution, String filesSuffixesDistribution, String fill,
           Random random, Type type) {
    this.name = name;
    this.conf = conf;
    this.genericWorkerBuilders = new ArrayList<>(genericWorkerBuilders);
    this.threads = threads;
    this.paths = new ArrayList<>(paths);
    this.workload = new ArrayList<>(workload);
    this.type = type;
    subdirs = initSubdirs(Collections.singletonList(""), subdirsWidth, subdirsDepth, subdirsFormat);
    this.totalBatches = batches * paths.size();
    this.batchesLeft = totalBatches;
    this.useTmpFile = useTmpFile;
    this.loadDelay = loadDelay;
    this.count = count;
    this.period = period;
    this.filesInBatch = filesInBatch;
    // for 'toString'
    this.filesSizesDistribution = filesSizesDistribution;
    this.filesSuffixesDistribution = filesSuffixesDistribution;
    this.fill = fill;
    this.random = random;

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

    validate();
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
        batchesLeft = totalBatches;
      }
      log.info("Start load {}{}", name, (count == 1) ? "" : ": " + (i + 1));
      runSingle(genericWorkers);
    }
    log.info("Load {} ended", name);
  }

  private void runSingle(List<GenericWorker> genericWorkers) {
    List<Callable<Object>> callables = genericWorkers.stream()
        .map(fw -> Executors.callable(
            () -> runBatches(fw))
        )
        .collect(Collectors.toList());

    ExecutorService executorService = Executors.newFixedThreadPool(genericWorkers.size());

    try {
      try {
        executorService.invokeAll(callables);
      } catch (InterruptedException e) {
        throw new RuntimeException(e); // NOPMD
      }
    } finally {
      executorService.shutdownNow();
    }
  }

  private void runBatches(GenericWorker fw) {
    while (true) {
      Map<String, Long> batch = getBatch();
      if (batch == null) {
        return;
      }
      // need to pass number of threads and other control information as well
      batch.put(FsLoaderBatchRemote.THREAD_NUMBER_KEY, (long) threads);
      batch.put(FsLoaderBatchRemote.USE_TMP_FILE_KEY, useTmpFile ? 1 : 0L);
      batch.put(FsLoaderBatchRemote.LOAD_DELAY_IN_MILLIS_KEY, loadDelay.toMillis());
      batch.put(FsLoaderBatchRemote.FILL_KEY, "random".equalsIgnoreCase(fill) ? -1 : Long.parseLong(fill));

      log.info("Start loader batch for host {}", fw.getHost());
      Instant before = Instant.now();
      try {
        long[] loadResult = fw.getAgent().load(conf, batch, workload);
        synchronized (loadResults) {
          loadResults.add(loadResult);
        }
      } catch (IOException e) {
        throw WorkerException.wrap(e);
      }
      log.info("End loader batch for host {}, batch took {}", fw.getHost(), Duration.between(before,
                                                                                             Instant.now()));
    }
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
    int totalRequests = count * totalBatches * filesInBatch;
    long[] globalResults = new long[totalRequests];
    synchronized (loadResults) {
      int pos = 0;
      for (long[] loadResult : loadResults) {
        System.arraycopy(loadResult, 0, globalResults, pos, loadResult.length);
        pos += loadResult.length;
      }
    }

    long failedRequests = Arrays.stream(globalResults).filter(l -> l <= 0).count();
    for (int i = 0; i < globalResults.length; i++) {
      globalResults[i] = Math.abs(globalResults[i]);
    }

    System.out.printf("Loader: %s%n", name);
    System.out.printf("Concurrency: %d%n", threads * genericWorkerBuilders.size());
    System.out.printf("Total number of requests: %d%n", totalRequests);
    System.out.printf("Failed requests: %d%n", failedRequests);
    System.out.printf("Total elapsed time: %fs%n", ((double) Arrays.stream(globalResults).sum()) / 1_000_000_000);
    System.out.printf("Average request time: %fms%n", (Arrays.stream(globalResults).average().orElse(0)) / 1_000_000);
    System.out.printf("Minimum request time: %fms%n", ((double) Arrays.stream(globalResults).min().orElse(0)) / 1_000_000);
    System.out.printf("Maximum request time: %fms%n", ((double) Arrays.stream(globalResults).max().orElse(0)) / 1_000_000);

    long averageObjectSize = (long) filesSizes.stream().mapToLong(l -> l).average().orElse(0);

    System.out.printf("Average Object Size: %d%n", averageObjectSize);
    System.out.printf("Total Object Size: %d%n", averageObjectSize * totalRequests);
    System.out.printf("Response Time Percentiles%n");

    Arrays.sort(globalResults);
    long[] parr = new long[]{500,
                             750,
                             900,
                             950,
                             990,
                             999};
    for (long l : parr) {
      System.out.printf("  %.1f : %.2f ms %n", (float) l / 10, getPercentile(globalResults, ((double) l) / 1000) / 1_000_000);
    }
  }

  public static double getPercentile(long[] arr, double percentile) {
    int index = (int) Math.ceil(percentile * (double) arr.length);
    return arr[index - 1];
  }

  private synchronized Map<String, Long> getBatch() {
    if (batchesLeft <= 0) {
      return null;
    }

    batchesLeft--;

    Map<String, Long> ret = new HashMap<>();
    for (int i = 0; i < filesInBatch; i++) {
      long globalNo = ((long) batchesLeft) * filesInBatch + i;
      String path = (type == Type.WINDOW) ? paths.get(batchesLeft % paths.size()) : paths.get((int) (globalNo % paths.size()));
      String subdir = subdirs.get((int) (globalNo % subdirs.size()));
      // same suffix and size for all paths in case of SPREAD load to make sure that each path has the same set of suffixes in the end
      // and each path in the target FS has the same size if suffixes are used for exclusion
      long sizeSuffixNo = (type == Type.WINDOW) ? globalNo : globalNo / paths.size();
      String suffix = filesSuffixes.isEmpty() ? "" : filesSuffixes.get((int) (sizeSuffixNo % filesSuffixes.size()));
      long fileSize = filesSizes.get((int) (sizeSuffixNo % filesSizes.size()));
      String fileName = randomFileName(random);

      String fullFileName = path + subdir + "/" + fileName + suffix;
      ret.put(fullFileName, fileSize);
    }
    return ret;
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
        + ", batchesLeft=" + batchesLeft + ", useTmpFile=" + useTmpFile + ", loadDelay=" + loadDelay
        + ", count=" + count + ", period=" + period
        + ", filesInBatch=" + filesInBatch + ", filesSizes=" + filesSizesDistribution + ", filesSuffixes=" + filesSuffixesDistribution
        + ", fill=" + fill
        + '}';
  }
}
