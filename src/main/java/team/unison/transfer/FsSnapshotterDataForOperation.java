package team.unison.transfer;

import javax.annotation.Nonnull;
import java.util.Map;

public class FsSnapshotterDataForOperation extends FsWrapperDataForOperation {
  public final String actions;

  public FsSnapshotterDataForOperation(
      int threadCount,
      @Nonnull String wrapperName,
      @Nonnull Map<String, String> conf,
      @Nonnull String actions
  ) {
    super(threadCount, wrapperName, conf);
    this.actions = actions;
  }
}
