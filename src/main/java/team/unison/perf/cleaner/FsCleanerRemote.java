/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.cleaner;

import team.unison.perf.PrometheusUtils;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.S3FsWrapper;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.WorkerException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

public class FsCleanerRemote {
  public static StatisticsDTO apply(
      @Nonnull ExecutorService executorService,
      @Nonnull Map<Thread, FsWrapper> threadToFsWrapperMap,
      @Nonnull List<String> paths,
      @Nonnull List<String> suffixes
  ) {
    StatisticsDTO stats = new StatisticsDTO();

    try {
      for (String path : paths) {
        String bucket = S3FsWrapper.toBucketAndKey(null, path)[0];
        // directories are left after removal of objects - call clean several times to purge all
        for (int i = 0; i < 1000; i++) {
          List<String> subPaths = executorService.submit(
              () -> threadToFsWrapperMap.get(Thread.currentThread()).list(null, path)
          ).get();

          if (subPaths.isEmpty()) {
            break;
          }

          List<CompletableFuture<Void>> futures = new ArrayList<>();
          for (String subPath : subPaths) {
            if (hasFileSuffix(suffixes, subPath)) {
              continue;
            }

            futures.add(CompletableFuture.runAsync(
                () -> {
                  FsWrapper fsWrapper = threadToFsWrapperMap.get(Thread.currentThread());
                  PrometheusUtils.runAndRecord(stats, "delete", () -> fsWrapper.delete(bucket, subPath));
                },
                executorService));
          }
          try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
          } catch (Exception e) {
            throw WorkerException.wrap(e);
          }
        }
      }

      for (String path : paths) {
        if (hasFileSuffix(suffixes, path)) {
          continue;
        }
        executorService.submit(
            () -> {
              FsWrapper fsWrapper = threadToFsWrapperMap.get(Thread.currentThread());
              PrometheusUtils.runAndRecord(stats, "delete", () -> fsWrapper.delete(null, path));
            }
        ).get();
      }
    } catch (Exception e) {
      throw WorkerException.wrap(e);
    }
    return stats;
  }

  private static boolean hasFileSuffix(List<String> suffixes, String path) {
    Optional<String> fileSuffix = suffixes.stream().filter(path::endsWith).findAny();
    return !fileSuffix.isPresent();
  }
}