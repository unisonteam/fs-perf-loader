/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.snapshot;

import java.io.Serializable;

public class FsSnapshotterOperationConf implements Serializable {
  private final int threadCount;
  private final String actions;

  public FsSnapshotterOperationConf(int threadCount, String actions) {
    this.threadCount = threadCount;
    this.actions = actions;
  }

  public int getThreadCount() {
    return threadCount;
  }

  public String getActions() {
    return actions;
  }
}
