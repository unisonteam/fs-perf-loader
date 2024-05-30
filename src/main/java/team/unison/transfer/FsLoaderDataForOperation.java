package team.unison.transfer;

import javax.annotation.Nonnull;
import java.util.Map;

public class FsLoaderDataForOperation extends FsWrapperDataForOperation {
  public final String fill;
  public final boolean useTmpFile;
  public final long loadDelayInMillis;

  public FsLoaderDataForOperation(
      int threads,
      @Nonnull String name,
      @Nonnull Map<String, String> conf,
      long loadDelayInMillis,
      boolean useTmpFile,
      @Nonnull String fill
  ) {
    super(threads, name, conf);
    this.useTmpFile = useTmpFile;
    this.loadDelayInMillis = loadDelayInMillis;
    this.fill = fill;
  }
}
