package team.unison.remote;

import team.unison.perf.loader.FsLoaderBatchRemote;
import team.unison.perf.snapshot.FsSnapshotterBatchRemote;
import team.unison.transfer.FsCleanerDataForOperation;
import team.unison.transfer.FsLoaderDataForOperation;
import team.unison.transfer.FsSnapshotterDataForOperation;
import team.unison.transfer.FsWrapperDataForOperation;

import javax.annotation.Nonnull;

final class FsWrapperContainerFactory {
  public static @Nonnull FsWrapperContainer get(FsWrapperDataForOperation data) {
    if (data instanceof FsLoaderDataForOperation) {
      return new FsWrapperContainer(data,
          wrapperData -> new FsLoaderBatchRemote((FsLoaderDataForOperation) wrapperData));
    } else if (data instanceof FsSnapshotterDataForOperation) {
      return new FsWrapperContainer(data,
          wrapperData -> new FsSnapshotterBatchRemote((FsSnapshotterDataForOperation) wrapperData));
    } else if (data instanceof FsCleanerDataForOperation) {
      return new FsWrapperContainer(data, (wrapperData) -> null);
    } else {
      throw new IllegalStateException("Wrong data passed for agent initialization");
    }
  }
}
