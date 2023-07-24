package team.unison.perf.jstack;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.remote.GenericWorker;
import team.unison.remote.GenericWorkerBuilder;

public class JstackSaver implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(JstackSaver.class);

  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
  private final GenericWorker genericWorker;
  private final Duration period;
  private final String filePrefix;
  private final String fileSuffix;
  private final String className;
  private final boolean fileGzip;
  private final boolean fileSingle;

  public JstackSaver(GenericWorkerBuilder genericWorkerBuilder, Duration period, String className, String filePrefix,
                     boolean fileGzip, boolean fileAppend, boolean fileSingle) {
    this.genericWorker = genericWorkerBuilder.get();
    this.period = period;
    this.filePrefix = filePrefix;
    this.className = className;
    this.fileSuffix = "-" + genericWorkerBuilder.getSshConnectionBuilder().getHost() + "-" + className.replace('.', '-');
    this.fileGzip = fileGzip;
    this.fileSingle = fileSingle;

    if (!new File(filePrefix).getParentFile().exists() && !new File(filePrefix).getParentFile().mkdirs()) {
      throw new IllegalArgumentException("Can't create the parent directory of " + filePrefix);
    }
    rotateFiles(fileGzip, fileAppend, fileSingle);
  }

  private void rotateFiles(boolean fileGzip, boolean fileAppend, boolean fileSingle) {
    if (fileAppend) {
      return;
    }
    File outputFile = new File(getFileName());
    if (outputFile.exists()) {
      File oldFile = new File(getFileName(fileSuffix + "-" + sdf.format(new Date()).replace(" ", "-").replace(":", "-")));
      outputFile.renameTo(oldFile);
    }
  }

  @Override
  public void run() {
    log.info("Saving jstack for " + className);
    try {
      String jstack = genericWorker.getAgent().jstack(className);
      try (BufferedWriter outputWriter = getOutputWriter()) {
        if (fileSingle) {
          outputWriter.write("==========" + sdf.format(new Date()) + "==========");
          outputWriter.newLine();
        }
        outputWriter.write(jstack);
        if (fileSingle && !jstack.endsWith("\n")) {
          outputWriter.newLine();
        }
      }
    } catch (IOException e) {
      log.warn("Error collecting jstack for {}", className, e);
    }
  }

  public Duration getPeriod() {
    return period;
  }

  private BufferedWriter getOutputWriter() {
    String fileName = getFileName();

    BufferedWriter outputWriter;

    try {
      // if append argument is false, file has been renamed and 'APPEND' option doesn't change anything
      if (fileGzip) {
        outputWriter = new BufferedWriter(new OutputStreamWriter(
            new GZIPOutputStream(new FileOutputStream(fileName, true))));
      } else {
        outputWriter = new BufferedWriter(new FileWriter(fileName, true));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return outputWriter;
  }

  private String getFileName() {
    return getFileName(fileSuffix);
  }

  private String getFileName(String suffix) {
    String fileName = filePrefix + suffix;
    if (!fileSingle) {
      fileName += "-" + sdf.format(new Date()).replace(" ", "-").replace(":", "-");
    }
    if (fileGzip) {
      fileName += ".gz";
    }

    return fileName;
  }
}