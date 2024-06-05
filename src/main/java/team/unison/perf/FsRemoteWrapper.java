package team.unison.perf;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FsRemoteWrapper {
  protected final ExecutorService executor;

  protected FsRemoteWrapper(int threadCount) {
    this.executor = Executors.newFixedThreadPool(threadCount);
  }

  public final void shutdown() {
    executor.shutdownNow();
  }
}
