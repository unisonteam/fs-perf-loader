package team.unison.perf.loader;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.remote.Utils;
import team.unison.remote.WorkerException;

public class FsLoaderBatchRemote {
  private static final Logger log = LoggerFactory.getLogger(FsLoaderBatchRemote.class);
  // these filenames are not valid - use them to pass extra control metadata
  static final String THREAD_NUMBER_KEY = ":";
  // C style: 0 is false, else is true
  static final String USE_TMP_FILE_KEY = "::";
  static final String LOAD_DELAY_IN_MILLIS_KEY = ":::";
  static final String FILL_KEY = "::::";

  public static long[] apply(Map<String, String> conf, Map<String, Long> arg, Map<String, String> command) {
    if (arg.isEmpty()) {
      return new long[0];
    }

    final byte[] barr = new byte[128 * 1024 * 1024];
    long fill = arg.remove(FILL_KEY);
    // 0 - no action - leave array filled with zeroes
    if (fill < 0) {
      new Random().nextBytes(barr);
    } else if (fill > 0) {
      Arrays.fill(barr, (byte) fill);
    }

    int threads = arg.remove(THREAD_NUMBER_KEY).intValue();
    // C style: 0 is false, else is true
    boolean useTmpFile = arg.remove(USE_TMP_FILE_KEY) != 0;
    long delayInMillis = arg.remove(LOAD_DELAY_IN_MILLIS_KEY);

    long[] ret = new long[arg.size()];
    AtomicInteger pos = new AtomicInteger();

    ExecutorService executorService = Executors.newFixedThreadPool(threads);
    String randomPath = arg.keySet().stream().findFirst().get();
    FsWrapper fsWrapper = FsWrapperFactory.get(randomPath, conf);

    List<Callable<Object>> callables = arg.entrySet().stream()
        .map(entry -> Executors.callable(
            () -> {
              long start = System.nanoTime();
              boolean success = runWorkload(fsWrapper, entry.getKey(), entry.getValue(), barr, useTmpFile, command);
              long elapsed = System.nanoTime() - start;
              PrometheusUtils.record(command.get("operation"), entry.getValue(), success, elapsed / 1_000);
              ret[pos.getAndIncrement()] = elapsed * (success ? 1 : -1);
              if (delayInMillis > 0) {
                Utils.sleep(delayInMillis);
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

  private static boolean runWorkload(FsWrapper fsWrapper, String path, long size, byte[] data, boolean useTmpFile,
                                     Map<String, String> command) {
    String op = command.get("operation");
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
}
