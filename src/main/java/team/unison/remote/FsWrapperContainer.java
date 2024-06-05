package team.unison.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.FsRemoteWrapper;
import team.unison.perf.cleaner.FsCleanerRemote;
import team.unison.perf.loader.FsLoaderBatchRemote;
import team.unison.perf.snapshot.FsSnapshotterBatchRemote;
import team.unison.transfer.FsCleanerDataForOperation;
import team.unison.transfer.FsLoaderDataForOperation;
import team.unison.transfer.FsSnapshotterDataForOperation;
import team.unison.transfer.FsWrapperDataForOperation;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FsWrapperContainer {
  private static final Logger log = LoggerFactory.getLogger(FsWrapperContainer.class);
  private static final Map<String, FsRemoteWrapper> FS_WRAPPER_CONTAINER_MAP = new ConcurrentHashMap<>();

  public static void createWrapperByName(@Nonnull FsWrapperDataForOperation data) {
    FS_WRAPPER_CONTAINER_MAP.putIfAbsent(data.fsWrapperName, initializeFsWrapper(data));
  }

  @SuppressWarnings("unchecked")
  public static <T> T getWrapper(@Nonnull String loaderName) {
    FsRemoteWrapper fsWrapper = FS_WRAPPER_CONTAINER_MAP.get(loaderName);
    if (fsWrapper instanceof FsLoaderBatchRemote) {
      return (T) fsWrapper;
    } if (fsWrapper instanceof FsSnapshotterBatchRemote) {
      return (T) fsWrapper;
    } if (fsWrapper instanceof FsCleanerRemote) {
      return (T) fsWrapper;
    }else {
      throw new IllegalStateException("Got wrong loader name or this container doesn't have worker " +
          "(use static functions)!");
    }
  }

  public static void clearResources() {
    FS_WRAPPER_CONTAINER_MAP.forEach((k, iRemoteWorker) -> {
      log.info("Release resources for remote worker {}", k);
      iRemoteWorker.shutdown();
    });
    FS_WRAPPER_CONTAINER_MAP.clear();
  }

  private static @Nonnull FsRemoteWrapper initializeFsWrapper(FsWrapperDataForOperation data) {
    if (data instanceof FsLoaderDataForOperation) {
      return new FsLoaderBatchRemote((FsLoaderDataForOperation) data);
    } else if (data instanceof FsSnapshotterDataForOperation) {
      return new FsSnapshotterBatchRemote((FsSnapshotterDataForOperation) data);
    } else if (data instanceof FsCleanerDataForOperation) {
      return new FsCleanerRemote((FsCleanerDataForOperation) data);
    } else {
      throw new IllegalStateException("Wrong data passed for FsRemoteWorker initialization");
    }
  }
}
