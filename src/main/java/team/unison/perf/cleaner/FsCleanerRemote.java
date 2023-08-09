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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.perf.fswrapper.S3FsWrapper;
import team.unison.remote.WorkerException;

public class FsCleanerRemote {
  private static final AtomicInteger FS_WRAPPER_COUNTER = new AtomicInteger();

  public static long[] apply(Map<String, String> conf, List<String> paths, List<String> suffixes, int threads) {
    List<FsWrapper> fsWrappers = FsWrapperFactory.get(paths.get(0), conf);

    ExecutorService executorService = Executors.newFixedThreadPool(threads);

    List<Long> ret = new ArrayList<>();

    try {
      for (String path : paths) {
        String bucket = S3FsWrapper.toBucketAndKey(null, path)[0];
        // directories are left after removal of objects - call clean several times to purge all
        for (int i = 0; i < 1000; i++) {
          List<String> subPaths = randomFsWrapper(fsWrappers).list(null, path);
          if (subPaths.isEmpty()) {
            break;
          }

          List<Callable<Object>> rmCalls = new ArrayList<>();
          for (String subPath : subPaths) {
            Optional<String> fileSuffix = suffixes.stream().filter(subPath::endsWith).findAny();

            if (!fileSuffix.isPresent()) {
              continue;
            }

            rmCalls.add(() -> deleteAndRecord(randomFsWrapper(fsWrappers), bucket, subPath));
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
      ret.add(deleteAndRecord(randomFsWrapper(fsWrappers), null, path));
    }

    return ret.stream().mapToLong(o -> o).toArray();
  }

  private static long deleteAndRecord(FsWrapper fsWrapper, String bucket, String path) {
    long start = System.nanoTime();
    boolean success = fsWrapper.delete(bucket, path);
    long elapsed = System.nanoTime() - start;
    PrometheusUtils.record("delete", 0, success, elapsed / 1_000_000);
    return elapsed * (success ? 1 : -1);
  }

  private static FsWrapper randomFsWrapper(List<FsWrapper> fsWrappers) {
    return fsWrappers.get(FS_WRAPPER_COUNTER.getAndIncrement() % fsWrappers.size());
  }
}