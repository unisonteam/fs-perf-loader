package team.unison.perf.cleaner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.remote.WorkerException;

public class FsCleanerRemote {
  private static final Logger log = LoggerFactory.getLogger(FsCleanerRemote.class);

  public static long[] apply(Map<String, String> conf, List<String> paths, List<String> suffixes, int threads) {
    FsWrapper fsWrapper = FsWrapperFactory.get(paths.get(0), conf);

    ExecutorService executorService = Executors.newFixedThreadPool(threads);

    List<Long> ret = new ArrayList<>();

    try {
      for (String path : paths) {
        // directories are left after removal of objects - call clean several times to purge all
        for (int i = 0; i < 1000; i++) {
          List<String> subPaths = fsWrapper.list(null, path);
          if (subPaths.isEmpty()) {
            break;
          }

          List<Callable<Object>> rmCalls = new ArrayList<>();
          for (String subPath : subPaths) {
            Optional<String> fileSuffix = suffixes.stream().filter(subPath::endsWith).findAny();

            if (!fileSuffix.isPresent()) {
              continue;
            }

            rmCalls.add(() -> deleteAndRecord(fsWrapper, subPath));
          }
          List<Future<Object>> futures = executorService.invokeAll(rmCalls);
          List<Long> batchResult = futures.stream().map(f -> {
            try {
              return (Long) f.get();
            } catch (Exception e) {
              throw WorkerException.wrap(e);
            }
          }).collect(Collectors.toList());
          ret.addAll(batchResult);
        }
      }
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw WorkerException.wrap(e);
    }

    for (String path : paths) {
      Optional<String> fileSuffix = suffixes.stream().filter(path::endsWith).findAny();

      if (!fileSuffix.isPresent()) {
        continue;
      }
      ret.add(deleteAndRecord(fsWrapper, path));
    }

    return ret.stream().mapToLong(o -> o).toArray();
  }

  private static long deleteAndRecord(FsWrapper fsWrapper, String path) {
    long start = System.nanoTime();
    boolean success = fsWrapper.delete(null, path);
    long elapsed = System.nanoTime() - start;
    PrometheusUtils.record("delete", 0, success, elapsed / 1_000_000);
    return elapsed * (success ? 1 : -1);
  }
}