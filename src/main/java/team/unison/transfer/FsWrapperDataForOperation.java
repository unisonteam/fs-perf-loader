package team.unison.transfer;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Map;

public abstract class FsWrapperDataForOperation implements Serializable {
  public final String fsWrapperName;
  public final int threadCount;
  public final Map<String, String> conf;

  protected FsWrapperDataForOperation(int threadCount, @Nonnull String wrapperName, @Nonnull Map<String, String> conf) {
    this.fsWrapperName = wrapperName;
    this.threadCount = threadCount;
    this.conf = conf;
  }
}
