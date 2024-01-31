/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.jstack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.LocalFile;
import team.unison.remote.GenericWorker;
import team.unison.remote.GenericWorkerBuilder;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Locale;

public class JstackSaver implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(JstackSaver.class);

  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
  private final GenericWorker genericWorker;
  private final Duration period;
  private final String className;
  private final LocalFile localFile;

  public JstackSaver(GenericWorkerBuilder genericWorkerBuilder, Duration period, String className, String filePrefix,
                     boolean fileGzip, boolean fileAppend, boolean fileSingle) {
    this.genericWorker = genericWorkerBuilder.get();
    this.period = period;
    this.className = className;
    localFile = new LocalFile(filePrefix,
            "-" + genericWorkerBuilder.getSshConnectionBuilder().getHost() + "-" + className.replace('.', '-'), fileGzip,
            fileAppend, fileSingle);
  }

  @Override
  public void run() {
    log.info("Saving jstack for " + className);
    try {
      String jstack = genericWorker.getAgent().jstack(className);

      localFile.write(jstack, "==========" + sdf.format(new Date()) + "==========" + System.lineSeparator());
    } catch (Exception e) {
      log.warn("Error collecting jstack for {}", className, e);
    }
  }

  public Duration getPeriod() {
    return period;
  }
}