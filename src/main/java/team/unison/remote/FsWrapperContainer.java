package team.unison.remote;

import team.unison.transfer.FsWrapperDataForOperation;

import javax.annotation.Nonnull;
import java.util.function.Function;

public final class FsWrapperContainer {
  public final FsWrapperCommandExecutor executor;
  public final FsWrapperDataForOperation data;
  public final Object worker;

  FsWrapperContainer(
      @Nonnull FsWrapperDataForOperation data,
      @Nonnull Function<FsWrapperDataForOperation, Object> workerProvider
  ) {
    this.executor = new FsWrapperCommandExecutor(data);
    this.data = data;
    this.worker = workerProvider.apply(data);
  }
}
