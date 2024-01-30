/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.remote;

public class SshRunResult {
  private int exitStatus;
  private String stdout;
  private String stderr;

  public int getExitStatus() {
    return exitStatus;
  }

  public void setExitStatus(int exitStatus) {
    this.exitStatus = exitStatus;
  }

  public String getStdout() {
    return stdout;
  }

  public void setStdout(String stdout) {
    this.stdout = stdout;
  }

  public String getStderr() {
    return stderr;
  }

  public void setStderr(String stderr) {
    this.stderr = stderr;
  }

  @Override
  public String toString() {
    return "SshRunResult{" + "exitStatus=" + exitStatus + ", stdout='" + stdout + '\'' + ", stderr='" + stderr + '\'' + '}';
  }
}
