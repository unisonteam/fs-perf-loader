/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.perf;

import team.unison.perf.filetransfer.FileTransfer;
import team.unison.perf.filetransfer.FileTransferBuilder;
import team.unison.remote.ClientFactory;
import team.unison.remote.SshConnectionBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class FileTransferPropertiesBuilder {
  public static List<FileTransfer> build(Properties props, SshConnectionBuilder sshConnectionBuilder) {
    List<FileTransfer> fileTransfers = new ArrayList<>();
    Set<String> fileTransferNames = props.stringPropertyNames().stream().filter(s -> s.startsWith("file.")).map(s -> s.split("\\.")[1])
            .collect(Collectors.toSet());

    for (String fileTransferName : fileTransferNames) {
      String prefix = "file." + fileTransferName + ".";

      SshConnectionBuilder sshConnectionBuilderWithUserAndGroup = sshConnectionBuilder.systemUser(props.getProperty(prefix + "user"))
              .systemGroup(props.getProperty(prefix + "group"));

      List<String> hosts = PerfLoaderUtils.parseTemplate(props.getProperty(prefix + "hosts").replace(" ", ""));
      String filesDir = props.getProperty("files.dir", System.getProperty("java.io.tmpdir")) + "/";
      for (String host : hosts) {
        FileTransfer fileTransfer = new FileTransferBuilder()
                .path(props.getProperty(prefix + "path"))
                .filePrefix(filesDir + props.getProperty(prefix + "file.prefix"))
                .fileAppend(Boolean.parseBoolean(props.getProperty(prefix + "file.append", "false")))
                .fileGzip(Boolean.parseBoolean(props.getProperty(prefix + "file.gzip", "false")))
                .period(Duration.ofSeconds(Long.parseLong(props.getProperty(prefix + "period", "30"))))
                .genericWorkerBuilder(ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilderWithUserAndGroup.host(host)))
                .createFileTransfer();
        fileTransfers.add(fileTransfer);
      }
    }
    return fileTransfers;
  }
}
