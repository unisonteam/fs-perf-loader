package team.unison.perf.jstack;

import team.unison.remote.GenericWorkerBuilder;

import java.time.Duration;

public class JstackSaverBuilder {
  private GenericWorkerBuilder genericWorkerBuilder;
  private String className;
  private String filePrefix;
  private boolean fileGzip;
  private boolean fileAppend;
  private boolean fileSingle = true;
  private Duration period = Duration.ofSeconds(30);
  
  public JstackSaverBuilder genericWorkerBuilder(GenericWorkerBuilder genericWorkerBuilder) {
    this.genericWorkerBuilder = genericWorkerBuilder;
    return this;
  }

  public JstackSaverBuilder period(Duration period) {
    this.period = period;
    return this;
  }

  public JstackSaverBuilder className(String className) {
    this.className = className;
    return this;
  }

  public JstackSaverBuilder filePrefix(String file) {
    this.filePrefix = file;
    return this;
  }

  public JstackSaverBuilder fileGzip(boolean fileGzip) {
    this.fileGzip = fileGzip;
    return this;
  }

  public JstackSaverBuilder fileAppend(boolean fileAppend) {
    this.fileAppend = fileAppend;
    return this;
  }

  public JstackSaverBuilder fileSingle(boolean fileSingle) {
    this.fileSingle = fileSingle;
    return this;
  }

  public JstackSaver createJstackSaver() {
    return new JstackSaver(genericWorkerBuilder, period, className, filePrefix, fileGzip, fileAppend, fileSingle);
  }
}