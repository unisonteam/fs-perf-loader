package team.unison.remote;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
