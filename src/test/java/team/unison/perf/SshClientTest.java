package team.unison.perf;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Arrays;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import team.unison.remote.SshConnectionBuilder;
import team.unison.remote.SshRunResult;

@Disabled
public class SshClientTest {
  SshConnectionBuilder sshConnectionBuilder = TestUtils.sshConnectionBuilderFromProperties();

  @Test
  public void timeoutNotExceeded() {
    SshRunResult runResult = sshConnectionBuilder.run("sleep 10; echo hello 10 seconds", Duration.ofSeconds(20));
    assertEquals(0, runResult.getExitStatus(), "Exit status");
  }

  @Test
  public void timeoutExceeded() {
    assertThrows(RuntimeException.class, () ->
        sshConnectionBuilder.run("sleep 60; echo hello 60 seconds", Duration.ofSeconds(10)));
  }

  @Test
  public void stdout() {
    SshRunResult runResult = sshConnectionBuilder.run("echo hello stdout", Duration.ofSeconds(20));
    assertThat("Process stdout", runResult.getStdout(), containsString("stdout"));
    assertThat("Process stderr", runResult.getStderr(), not(containsString("stdout")));
  }

  @Test
  public void stderr() {
    SshRunResult runResult = sshConnectionBuilder.run("echo hello stderr >&2", Duration.ofSeconds(20));
    assertThat("Process stdout", runResult.getStdout(), not(containsString("stderr")));
    assertThat("Process stderr", runResult.getStderr(), containsString("stderr"));
  }

  @Test
  public void id() {
    for (String cmd : Arrays.asList("id", "bash -c \"echo Hello world\"")) {
      sshConnectionBuilder.run(cmd);
      sshConnectionBuilder.systemGroup("hadoop").run(cmd);
      sshConnectionBuilder.systemUser("hdfs").run(cmd);
      sshConnectionBuilder.systemUser("hdfs").systemGroup("hdfs").run(cmd);
    }
  }
}
