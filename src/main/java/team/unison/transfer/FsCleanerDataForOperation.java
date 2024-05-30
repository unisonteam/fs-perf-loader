package team.unison.transfer;

import javax.annotation.Nonnull;
import java.util.Map;

public class FsCleanerDataForOperation extends FsWrapperDataForOperation {
  public FsCleanerDataForOperation(int threads, @Nonnull String name, @Nonnull Map<String, String> conf) {
    super(threads, name, conf);
  }
}
