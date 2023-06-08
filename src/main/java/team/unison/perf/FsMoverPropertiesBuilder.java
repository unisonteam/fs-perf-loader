package team.unison.perf;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import team.unison.perf.mover.FsMover;
import team.unison.perf.mover.FsMoverBuilder;
import team.unison.remote.ClientFactory;
import team.unison.remote.SshConnectionBuilder;

final class FsMoverPropertiesBuilder {
  private FsMoverPropertiesBuilder() {
  }

  static Map<String, FsMover> build(Properties props, SshConnectionBuilder sshConnectionBuilder) {
    Map<String, FsMover> fsMovers = new HashMap<>();
    Set<String> fsLoaderNames = props.stringPropertyNames().stream().filter(s -> s.startsWith("fsmover.")).map(s -> s.split("\\.")[1])
        .collect(Collectors.toSet());

    for (String fsMoverName : fsLoaderNames) {
      String prefix = "fsmover." + fsMoverName + ".";

      String host = props.getProperty(prefix + "host");

      if (host == null || host.isEmpty()) {
        throw new IllegalArgumentException("Property" + prefix + " host" + " is not set");
      }

      FsMover fsMover = new FsMoverBuilder()
          .name(fsMoverName)
          .genericWorkerBuilder(ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilder.host(host)))
          .source(props.getProperty(prefix + "source"))
          .target(props.getProperty(prefix + "target"))
          .threads(Integer.parseInt(props.getProperty(prefix + "threads", "8")))
          .suffixes(Arrays.asList(props.getProperty(prefix + "suffixes", "").split(",")))
          .delay(Duration.ofSeconds(Long.parseLong(props.getProperty(prefix + "delay", "10"))))
          .timeout(Duration.ofSeconds(Long.parseLong(props.getProperty(prefix + "timeout", "600"))))
          .period(Duration.ofSeconds(Long.parseLong(props.getProperty(prefix + "period", "60"))))
          .removeSuffix(Boolean.parseBoolean(props.getProperty(prefix + "remove.suffix", "false")))
          .createFsMover();

      fsMovers.put(fsMoverName, fsMover);
    }
    return fsMovers;
  }
}
