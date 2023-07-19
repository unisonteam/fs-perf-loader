package team.unison.perf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import team.unison.perf.jstack.JstackSaver;
import team.unison.perf.jstack.JstackSaverBuilder;
import team.unison.remote.ClientFactory;
import team.unison.remote.SshConnectionBuilder;

public class JstackSaverPropertiesBuilder {
  public static List<JstackSaver> build(Properties props, SshConnectionBuilder sshConnectionBuilder) {
    List<JstackSaver> jstackSavers = new ArrayList<>();
    Set<String> jstackSaverNames = props.stringPropertyNames().stream().filter(s -> s.startsWith("jstack.")).map(s -> s.split("\\.")[1])
        .collect(Collectors.toSet());

    for (String jstackSaverName : jstackSaverNames) {
      String prefix = "jstack." + jstackSaverName + ".";

      SshConnectionBuilder sshConnectionBuilderWithUserAndGroup = sshConnectionBuilder.systemUser(props.getProperty(prefix + "user"))
          .systemGroup(props.getProperty(prefix + "group"));

      List<String> hosts = PerfLoaderUtils.parseTemplate(props.getProperty(prefix + "hosts").replace(" ", ""));
      for (String host : hosts) {
        JstackSaver jstackSaver = new JstackSaverBuilder()
            .name(jstackSaverName)
            .className(props.getProperty(prefix + "class"))
            .filePrefix(props.getProperty(prefix + "fileprefix"))
            .append(Boolean.parseBoolean(props.getProperty(prefix + "append", "false")))
            .gzip(Boolean.parseBoolean(props.getProperty(prefix + "gzip", "false")))
            .period(Duration.ofSeconds(Long.parseLong(props.getProperty(prefix + "period", "30"))))
            .genericWorkerBuilder(ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilderWithUserAndGroup.host(host)))
            .createJstackSaver();
        jstackSavers.add(jstackSaver);
      }
    }
    return jstackSavers;
  }
}
