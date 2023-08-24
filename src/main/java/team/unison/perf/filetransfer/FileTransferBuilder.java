package team.unison.perf.filetransfer;

import java.time.Duration;
import team.unison.remote.GenericWorkerBuilder;

public class FileTransferBuilder {
  private GenericWorkerBuilder genericWorkerBuilder;
  private String path;
  private String filePrefix;
  private boolean fileGzip;
  private boolean fileAppend;
  private Duration period = Duration.ofSeconds(30);

  public FileTransferBuilder genericWorkerBuilder(GenericWorkerBuilder genericWorkerBuilder) {
    this.genericWorkerBuilder = genericWorkerBuilder;
    return this;
  }

  public FileTransferBuilder period(Duration period) {
    this.period = period;
    return this;
  }

  public FileTransferBuilder path(String path) {
    this.path = path;
    return this;
  }

  public FileTransferBuilder filePrefix(String file) {
    this.filePrefix = file;
    return this;
  }

  public FileTransferBuilder fileGzip(boolean fileGzip) {
    this.fileGzip = fileGzip;
    return this;
  }

  public FileTransferBuilder fileAppend(boolean fileAppend) {
    this.fileAppend = fileAppend;
    return this;
  }

  public FileTransfer createFileTransfer() {
    return new FileTransfer(genericWorkerBuilder, period, path, filePrefix, fileGzip, fileAppend);
  }
}