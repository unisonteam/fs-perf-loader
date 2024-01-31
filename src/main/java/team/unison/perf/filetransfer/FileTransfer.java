/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.filetransfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.LocalFile;
import team.unison.remote.GenericWorker;
import team.unison.remote.GenericWorkerBuilder;

import java.time.Duration;

public class FileTransfer implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(FileTransfer.class);

  private final GenericWorker genericWorker;
  private final Duration period;
  private final String path;
  private final LocalFile localFile;

  public FileTransfer(GenericWorkerBuilder genericWorkerBuilder, Duration period, String path, String filePrefix,
                      boolean fileGzip, boolean fileAppend) {
    this.genericWorker = genericWorkerBuilder.get();
    this.period = period;
    this.path = path;

    localFile = new LocalFile(filePrefix, "-" + genericWorkerBuilder.getSshConnectionBuilder().getHost(), fileGzip, fileAppend, true);

  }

  @Override
  public void run() {
    log.info("Copy " + (path == null ? "agent log" : "'" + path + "'") + " from host " + genericWorker.getHost());
    try {
      byte[] chunk = genericWorker.getAgent().nextChunk(path);
      localFile.write(chunk);
    } catch (Exception e) {
      log.warn("Error saving jstack for {}", path, e);
    }
  }

  public Duration getPeriod() {
    return period;
  }
}