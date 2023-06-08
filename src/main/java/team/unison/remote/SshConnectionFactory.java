package team.unison.remote;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SshConnectionFactory {
  private static final Logger log = LoggerFactory.getLogger(SshConnectionFactory.class);

  public static SshConnectionBuilder build() {
    return new SshConnectionBuilder();
  }

  static boolean put(SshConnectionBuilder sshConnectionBuilder, InputStream inputStream, String remoteFileName) {
    log.info("Uploading data from input stream to remote file {} on host {}", remoteFileName, sshConnectionBuilder.getHost());
    Session session = getConnectedSession(sshConnectionBuilder);
    ChannelSftp channelSftp = null;
    try {
      Channel channel = session.openChannel("sftp");
      channel.connect();
      channelSftp = (ChannelSftp) channel;
      channelSftp.put(inputStream, remoteFileName);
      return true;
    } catch (JSchException | SftpException e) {
      log.warn("Creating remote file {} failed", remoteFileName, e);
      return false;
    } finally {
      if (channelSftp != null) {
        channelSftp.exit();
      }
      session.disconnect();
    }
  }

  static SshRunResult run(SshConnectionBuilder sshConnectionBuilder, String command, Map<String, String> env, Duration duration) {
    return Utils.timeout(() -> run(sshConnectionBuilder, command, env), duration);
  }

  private static SshRunResult run(SshConnectionBuilder sshConnectionBuilder, String command, Map<String, String> env) {
    try {
      List<String> commandParts = new ArrayList<>();

      if (sshConnectionBuilder.getSystemUser() != null && !sshConnectionBuilder.getSystemUser().equals("")) {
        commandParts.add("sudo");
        commandParts.add("-u");
        commandParts.add(sshConnectionBuilder.getSystemUser());
        // preserve JAVA_HOME when using sudo
        commandParts.add("JAVA_HOME=$JAVA_HOME");
      }

      for (Map.Entry<String, String> entry : env.entrySet()) {
        if (entry.getValue() != null) {
          commandParts.add(entry.getKey() + "='" + entry.getValue() + "'");
        }
      }

      // this piece of code serves two purposes:
      // - for 'sudo' calls it adds 'PATH' parameter to sudo preserving paths
      // - for non-'sudo' calls it adds standard paths to path in case path is empty
      commandParts.add("PATH=/usr/bin:/bin:$JAVA_HOME/bin:$PATH");
      // end of path workarounds

      if (sshConnectionBuilder.getSystemGroup() == null) {
        commandParts.add(command);
      } else {
        commandParts.add("sg " + sshConnectionBuilder.getSystemGroup() + " \"" + command.replace("\"", "\\\"") + "\"");
      }

      Session session = getConnectedSession(sshConnectionBuilder);
      Channel channel = session.openChannel("exec");

      String fullCommand = String.join(" ", commandParts);
      log.info("Sending command [" + fullCommand + "] to host " + sshConnectionBuilder.getHost());
      ((ChannelExec) channel).setCommand(fullCommand);

      channel.setInputStream(null);

      ByteArrayOutputStream stdout = new ByteArrayOutputStream();
      ByteArrayOutputStream stderr = new ByteArrayOutputStream();
      channel.setOutputStream(stdout);
      ((ChannelExec) channel).setErrStream(stderr);

      channel.connect(120_000);

      long waitTimeout = 50;
      long keepAlivePeriod = 30; // period in seconds to send keep alive message
      long keepAliveCount = 0;
      while (!channel.isClosed()) {
        //noinspection BusyWait
        Thread.sleep(waitTimeout);
        waitTimeout = Math.min(1000, waitTimeout + 50);
        keepAliveCount = (keepAliveCount + 1) % keepAlivePeriod;
        if (keepAliveCount == 0) {
          session.sendKeepAliveMsg();
        }
      }

      channel.disconnect();
      session.disconnect();

      SshRunResult sshRunResult = new SshRunResult();
      sshRunResult.setExitStatus(channel.getExitStatus());
      sshRunResult.setStdout(stdout.toString("UTF-8").trim());
      sshRunResult.setStderr(stderr.toString("UTF-8").trim());

      log.info("SSH run result: " + sshRunResult);

      return sshRunResult;
    } catch (Exception e) {
      log.error("SSH connection issues", e);
      throw new RuntimeException("SSH connection issues", e);
    }
  }

  private static Session getConnectedSession(SshConnectionBuilder sshConnectionBuilder) {
    // http://man.openbsd.org/ssh_config
    Properties config = new Properties();
    config.put("ClientAliveInterval", "15");
    config.put("ServerAliveInterval", "15");
    config.put("ServerAliveCountMax", "150");
    config.put("TCPKeepAlive", "no");
    config.put("StrictHostKeyChecking", "no");
    JSch jSch = new JSch();
    try {
      jSch.setKnownHosts("/dev/null");
      if (sshConnectionBuilder.getIdentity() != null) {
        config.put("PreferredAuthentications", "publickey");
        if (sshConnectionBuilder.getPassword() == null) {
          jSch.addIdentity(sshConnectionBuilder.getIdentity());
        } else {
          jSch.addIdentity(sshConnectionBuilder.getIdentity(), sshConnectionBuilder.getPassword());
        }
      }

      Session session = jSch.getSession(sshConnectionBuilder.getSshUser(), sshConnectionBuilder.getHost());
      if (sshConnectionBuilder.getIdentity() == null && sshConnectionBuilder.getPassword() != null) {
        config.put("PreferredAuthentications", "password");
        session.setPassword(sshConnectionBuilder.getPassword());
      }
      session.setConfig(config);
      session.connect();
      return session;
    } catch (JSchException e) {
      log.error("SSH connection issues", e);
      throw new RuntimeException("SSH connection issues", e);
    }
  }
}