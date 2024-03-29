/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.loader;

import java.io.Serializable;

public class FsLoaderOperationConf implements Serializable {
  private final int threadCount;
  private final boolean usetmpFile;
  private final long loadDelayInMillis;
  private final char fillChar;

  public FsLoaderOperationConf(int threadCount, boolean usetmpFile, long loadDelayInMillis, char fillChar) {
    this.threadCount = threadCount;
    this.usetmpFile = usetmpFile;
    this.loadDelayInMillis = loadDelayInMillis;
    this.fillChar = fillChar;
  }

  public int getThreadCount() {
    return threadCount;
  }

  public boolean isUsetmpFile() {
    return usetmpFile;
  }

  public long getLoadDelayInMillis() {
    return loadDelayInMillis;
  }

  public char getFillChar() {
    return fillChar;
  }
}
