/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Utils {
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  // threads prevent VMs from shutting down if they are not daemon
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool(new ThreadFactory() {
    private final AtomicInteger threadCounter = new AtomicInteger();

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.setName("timeout-exec-" + threadCounter.incrementAndGet());
      return t;
    }
  });

  private Utils() {
  }

  public static <T> T timeout(Callable<T> callable, Duration duration) {
    Future<T> future = EXECUTOR_SERVICE.submit(callable);
    try {
      return future.get(duration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      log.error("Execution timeout " + duration.toMillis() + " TimeUnit.MILLISECONDS", e);
      throw new RuntimeException("Execution timeout " + duration, e);
    } catch (InterruptedException e) {
      log.error("Execution was interrupted: ", e);
      throw new RuntimeException("Execution was interrupted", e);
    } catch (ExecutionException e) {
      log.error("Exception in execution: ", e);
      throw new RuntimeException("Exception in execution", e);
    }
  }

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      log.error("InterruptedException", e);
      throw new RuntimeException(e);
    }
  }
}
