package team.unison.perf.mover;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.remote.GenericWorker;
import team.unison.remote.GenericWorkerBuilder;
import team.unison.remote.WorkerException;

public final class FsMover implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(FsMover.class);

  private final String name;
  private final GenericWorker genericWorker;
  private final String source;
  private final String target;
  private final int threads;
  private final List<String> suffixes;
  private final Duration delay;
  private final Duration period;
  private final Duration timeout;
  private final boolean removeSuffix;
  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  public FsMover(String name, GenericWorkerBuilder genericWorkerBuilder, String source, String target, int threads,
                 Collection<String> suffixes,
                 Duration delay, Duration period, Duration timeout, boolean removeSuffix) {
    this.name = name;
    this.genericWorker = genericWorkerBuilder.get();
    this.source = source;
    this.target = target;
    this.threads = threads;
    this.suffixes = new ArrayList<>(suffixes);
    this.delay = delay;
    this.period = period;
    this.timeout = timeout;
    this.removeSuffix = removeSuffix;

    validate();
  }

  private void validate() {
    if (source == null || source.isEmpty()) {
      throw new IllegalArgumentException("Source not set");
    }

    if (!removeSuffix && (target == null || target.isEmpty())) {
      throw new IllegalArgumentException("Neither removeSuffix nor target are set");
    }

    if (threads <= 0) {
      throw new IllegalArgumentException("Number of threads is zero or negative : " + threads);
    }

    if (timeout.getSeconds() <= 0) {
      throw new IllegalArgumentException("Timeout in seconds is zero or negative : " + timeout);
    }
  }

  public void start() {
    if (delay.getSeconds() < 0) {
      throw new IllegalArgumentException("Delay in seconds is negative : " + delay);
    }
    if (period.getSeconds() <= 0) {
      throw new IllegalArgumentException("Period in seconds is zero or negative : " + period);
    }
    log.info("Scheduling mover with delay {}, period {} seconds", delay.getSeconds(), period.getSeconds());
    scheduledExecutorService.scheduleAtFixedRate(this, delay.getSeconds(), period.getSeconds(), TimeUnit.SECONDS);
  }

  public void stop() {
    stop(true);
  }

  /**
   * Stop scheduled mover.
   *
   * @param runAfterStop run mover loop last time after stop to make sure all files that were created before this method was
   *                     called are moved if applicable.
   */
  public void stop(boolean runAfterStop) {
    if (scheduledExecutorService.isShutdown()) {
      return;
    }
    log.info("Stopping mover");
    scheduledExecutorService.shutdownNow();
    try {
      scheduledExecutorService.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e); // NOPMD
    }
    if (runAfterStop) {
      run();
    }
  }

  @Override
  public void run() {
    // log.info("Start mover {} batch for host {}", name, genericWorker.sshConnectionBuilder().getHost());

    List<String> arg = new ArrayList<>();
    arg.add(target == null ? source : target);
    arg.add(Integer.toString(threads));
    arg.add(Boolean.toString(removeSuffix));
    arg.addAll(suffixes);

    Instant before = Instant.now();
    try {
      genericWorker.getAgent().move(source, arg);
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    }
    log.info("End mover {} batch for host {}, batch took {}", name, genericWorker.getHost(), Duration.between(before, Instant.now()));
  }

  @Override
  public String toString() {
    return "FsMover{" + "source='" + source + '\'' + ", target='" + target + '\''
        + ", threads=" + threads + ", removeSuffix='" + removeSuffix + '\''
        + ", suffixes=" + suffixes + ", delay=" + delay + ", period=" + period
        + ", timeout=" + timeout + ", genericWorker=" + genericWorker + '}';
  }
}
