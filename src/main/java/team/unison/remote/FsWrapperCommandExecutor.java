package team.unison.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.transfer.FsWrapperDataForOperation;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

public class FsWrapperCommandExecutor {
  private static final Logger log = LoggerFactory.getLogger(FsWrapperCommandExecutor.class);
  private final Map<Thread, FsWrapper> threadToFsWrapper = new ConcurrentHashMap<>();
  private final ExecutorService executor;

  public FsWrapperCommandExecutor(@Nonnull FsWrapperDataForOperation data) {
    executor = Executors.newFixedThreadPool(data.threadCount, runnable -> {
      Thread thread = new Thread(runnable);
      threadToFsWrapper.computeIfAbsent(thread, t -> FsWrapperFactory.get(data.conf));
      return thread;
    });
  }

  public @Nonnull CompletableFuture<Void> runAsync(@Nonnull Consumer<FsWrapper> fsWrapperConsumer) {
    return CompletableFuture.runAsync(
        () -> fsWrapperConsumer.accept(threadToFsWrapper.get(Thread.currentThread())), executor);
  }

  public @Nonnull <T> CompletableFuture<T> supplyAsync(@Nonnull Function<FsWrapper, T> fsWrapperConsumer) {
    return CompletableFuture.supplyAsync(
        () -> fsWrapperConsumer.apply(threadToFsWrapper.get(Thread.currentThread())), executor);
  }

  public <T> @Nonnull T submitAndGet(@Nonnull Function<FsWrapper, T> wrapperTFunction)
      throws ExecutionException, InterruptedException
  {
    return executor.submit(() -> wrapperTFunction.apply(threadToFsWrapper.get(Thread.currentThread()))).get();
  }

  public void shutdownNow() {
    threadToFsWrapper.clear();
    log.info("\tClear threadToFsWrapper map");
    executor.shutdownNow();
    log.info("\tShutdown executor");
  }
}

