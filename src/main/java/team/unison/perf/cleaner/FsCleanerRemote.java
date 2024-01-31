/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.cleaner;

import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.perf.fswrapper.S3FsWrapper;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.WorkerException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FsCleanerRemote {
  private static final AtomicInteger FS_WRAPPER_COUNTER = new AtomicInteger();

  public static StatisticsDTO apply(Map<String, String> conf, List<String> paths, List<String> suffixes, int threads) {
    List<FsWrapper> fsWrappers = FsWrapperFactory.get(paths.get(0), conf);

    ExecutorService executorService = Executors.newFixedThreadPool(threads);

    StatisticsDTO stats = new StatisticsDTO();

    try {
      for (String path : paths) {
        String bucket = S3FsWrapper.toBucketAndKey(null, path)[0];
        // directories are left after removal of objects - call clean several times to purge all
        for (int i = 0; i < 1000; i++) {
          List<String> subPaths = randomFsWrapper(fsWrappers).list(null, path);
          if (subPaths.isEmpty()) {
            break;
          }

          List<Callable<Void>> rmCalls = new ArrayList<>();
          for (String subPath : subPaths) {
            Optional<String> fileSuffix = suffixes.stream().filter(subPath::endsWith).findAny();

            if (!fileSuffix.isPresent()) {
              continue;
            }

            rmCalls.add(() -> deleteAndRecord(randomFsWrapper(fsWrappers), bucket, subPath, stats));
          }
          List<Future<Void>> futures = executorService.invokeAll(rmCalls);
          List<Void> batchResult = futures.stream().map(f -> {
            try {
              return f.get();
            } catch (Exception e) {
              throw WorkerException.wrap(e);
            }
          }).collect(Collectors.toList());
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
      deleteAndRecord(randomFsWrapper(fsWrappers), null, path, stats);
    }

    return stats;
  }

  private static Void deleteAndRecord(FsWrapper fsWrapper, String bucket, String path, StatisticsDTO stats) {
    long start = System.nanoTime();
    boolean success = fsWrapper.delete(bucket, path);
    long elapsed = System.nanoTime() - start;
    PrometheusUtils.record(stats, "delete", 0, success, elapsed);
    return null;
  }

  private static FsWrapper randomFsWrapper(List<FsWrapper> fsWrappers) {
    return fsWrappers.get(FS_WRAPPER_COUNTER.getAndIncrement() % fsWrappers.size());
  }
}