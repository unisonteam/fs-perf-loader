package team.unison.remote;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PerfLoaderUtils;

final class RemoteExec {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteExec.class);

  private static final String DEPLOY_DIR_MASK = "/tmp/remote-agent-%s-%02d";
  private static final String AGENT_SCRIPT_NAME = "agent.sh";

  public static synchronized void deployAgent(SshConnectionBuilder sshConnectionBuilder) {
    SshRunResult checkAlreadyDeployed = sshConnectionBuilder.run("[ -f " + getScriptPath() + " ]");
    if (checkAlreadyDeployed.getExitStatus() == 0) {
      sshConnectionBuilder.run(getScriptPath() + " " + sshConnectionBuilder.getHost());
      return;
    }

    sshConnectionBuilder.run("mkdir -m 777 -p " + getScriptDir());

    String packageJarFile = getScriptDir() + "/agent.jar";

    // https://stackoverflow.com/questions/320542/how-to-get-the-path-of-a-running-jar-file
    try {
      Path jarPath = Paths.get(RemoteExec.class.getProtectionDomain().getCodeSource().getLocation().toURI());
      if (jarPath.toFile().isDirectory()) { // start from IDE
        Path jarsDir = jarPath.resolve("../../../libs").normalize();
        File jarFile = jarsDir.toFile().listFiles((dir, name) -> name.endsWith("fat.jar"))[0];
        jarPath = jarFile.toPath();
      }
      LOGGER.info("Jar path is {}", jarPath);

      try (InputStream jarStream = Files.newInputStream(jarPath)) {
        sshConnectionBuilder.put(jarStream, packageJarFile);
      }
    } catch (URISyntaxException | IOException e) {
      LOGGER.warn("Error uploading jar file to remote host " + sshConnectionBuilder.getHost(), e);
    }

    List<String> commands = new ArrayList<>();
    commands.add("umask 000");
    commands.add("mkdir -m 777 -p " + getScriptDir() + "/logs");
    commands.add("cd " + getScriptDir());
    commands.add("jar xf " + packageJarFile + " " + AGENT_SCRIPT_NAME);
    commands.add("chmod +x " + getScriptPath());
    commands.add("sed -i 's/\\r//' " + getScriptPath());
    sshConnectionBuilder.run("bash -c \"" + String.join(" && ", commands) + "\"");

    Map<String, String> env = new HashMap<>();
    env.put("_JAVA_OPTIONS", PerfLoaderUtils.getGlobalProperties().getProperty("jvmoptions", "-Xmx1G"));
    SshRunResult sshRunResult = sshConnectionBuilder.run(getScriptPath() + " " + sshConnectionBuilder.getHost(), env);

    if (sshRunResult.getExitStatus() != 0) {
      throw new RuntimeException("Couldn't deploy worker : " + sshRunResult);
    }
  }

  private static String getScriptPath() {
    return getScriptDir() + "/" + AGENT_SCRIPT_NAME;
  }

  private static String getScriptDir() {
    ZoneId zoneId = ZoneId.of("Europe/Moscow");
    ZonedDateTime now = ZonedDateTime.now(zoneId);
    int week = now.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);

    return String.format(DEPLOY_DIR_MASK, getVersionTag(), week).replaceAll("/+$", "")
        .replace(':', '-');
  }

  private static String getVersionTag() {
    try (InputStream stream = RemoteExec.class.getResourceAsStream("/perf-loader.version")) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
        return br.readLine();
      }
    } catch (IOException | NullPointerException e) {
      LOGGER.warn("Failed to read version", e);
    }
    return "WIP";
  }

}
