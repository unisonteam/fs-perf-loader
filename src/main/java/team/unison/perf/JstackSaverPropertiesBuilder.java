/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf;

import team.unison.perf.jstack.JstackSaver;
import team.unison.perf.jstack.JstackSaverBuilder;
import team.unison.remote.ClientFactory;
import team.unison.remote.SshConnectionBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

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
      String filesDir = props.getProperty("files.dir", System.getProperty("java.io.tmpdir")) + "/";
      for (String host : hosts) {
        JstackSaver jstackSaver = new JstackSaverBuilder()
                .className(props.getProperty(prefix + "class"))
                .filePrefix(filesDir + props.getProperty(prefix + "file.prefix"))
                .fileAppend(Boolean.parseBoolean(props.getProperty(prefix + "file.append", "false")))
                .fileGzip(Boolean.parseBoolean(props.getProperty(prefix + "file.gzip", "false")))
                .fileSingle(Boolean.parseBoolean(props.getProperty(prefix + "file.single", "true")))
                .period(Duration.ofSeconds(Long.parseLong(props.getProperty(prefix + "period", "30"))))
                .genericWorkerBuilder(ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilderWithUserAndGroup.host(host)))
                .createJstackSaver();
        jstackSavers.add(jstackSaver);
      }
    }
    return jstackSavers;
  }
}
