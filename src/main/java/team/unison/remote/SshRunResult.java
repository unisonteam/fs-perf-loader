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
