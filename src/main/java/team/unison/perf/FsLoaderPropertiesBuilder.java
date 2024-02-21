/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.loader.FsLoader;
import team.unison.perf.loader.FsLoaderBuilder;
import team.unison.remote.ClientFactory;
import team.unison.remote.GenericWorkerBuilder;
import team.unison.remote.SshConnectionBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static team.unison.perf.PerfLoaderUtils.getProperty;

final class FsLoaderPropertiesBuilder {
  private static final Logger log = LoggerFactory.getLogger(FsLoaderPropertiesBuilder.class);
  private static Random random = null;

  private FsLoaderPropertiesBuilder() {
  }

  static List<FsLoader> build(Properties props, SshConnectionBuilder sshConnectionBuilder) throws IOException {
    List<FsLoader> fsLoaders = new ArrayList<>();
    Set<String> fsLoaderNames = props.stringPropertyNames().stream().filter(s -> s.startsWith("fsloader.")).map(s -> s.split("\\.")[1])
            .collect(Collectors.toSet());

    for (String fsLoaderName : fsLoaderNames) {
      String prefix = "fsloader." + fsLoaderName + ".";
      List<String> hosts = PerfLoaderUtils.parseTemplate(getProperty(props, prefix, "hosts").replace(" ", ""));
      List<GenericWorkerBuilder> genericWorkerBuilders = new ArrayList<>();
      for (String host : hosts) {
        genericWorkerBuilders.add(ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilder.host(host)));
      }
      long defaultCommandDelayInSeconds = props.getProperty("prometheus.address") != null ? 25 : 0;
      long commandDelayInSeconds = getProperty(props, prefix, "command.delay") != null ? Long.parseLong(
              getProperty(props, prefix, "command.delay")) : defaultCommandDelayInSeconds;
      FsLoader fsLoader = new FsLoaderBuilder()
              .name(fsLoaderName)
              .conf(Confs.get(getProperty(props, prefix, "conf")))
              .type(FsLoader.Type.valueOf(getProperty(props, prefix, "batch.type", "WINDOW")))
              .random(getRandomFromSeed(props))
              .threads(Integer.parseInt(getProperty(props, prefix, "threads", "8")))
              .filesSizesDistribution(getProperty(props, prefix, "batch.file.sizes"))
              .filesSuffixesDistribution(getProperty(props, prefix, "batch.file.suffixes"))
              .paths(PerfLoaderUtils.parseTemplate(getProperty(props, prefix, "paths", "")))
              .workload(PerfLoaderUtils.parseWorkload(getProperty(props, prefix, "workload")))
              .genericWorkerBuilders(genericWorkerBuilders)
              .useTmpFile(Boolean.parseBoolean(getProperty(props, prefix, "usetmpfile", "false")))
              .loadDelay(Duration.ofMillis(Long.parseLong(getProperty(props, prefix, "delay", "0"))))
              .commandDelay(Duration.ofSeconds(commandDelayInSeconds))
              .count(Integer.parseInt(getProperty(props, prefix, "count", "1")))
              .period(Duration.ofSeconds(Long.parseLong(getProperty(props, prefix, "period", "0"))))
              .batches(Integer.parseInt(getProperty(props, prefix, "batch.count", "10")))
              .batchTimeout(Duration.ofSeconds(Long.parseLong(getProperty(props, prefix, "batch.timeout", "600"))))
              .filesInBatch(Integer.parseInt(getProperty(props, prefix, "batch.file.count", "10")))
              .subdirsDepth(Integer.parseInt(getProperty(props, prefix, "subdirs.depth", "2")))
              .subdirsWidth(Integer.parseInt(getProperty(props, prefix, "subdirs.width", "2")))
              .subdirsFormat(getProperty(props, prefix, "subdirs.format", "%d"))
              .fill(getProperty(props, prefix, "fill", "random"))
              .createFsLoader();

      fsLoaders.add(fsLoader);
    }
    return fsLoaders;
  }

  private static synchronized Random getRandomFromSeed(Properties props) {
    if (random == null) {
      String seed = props.getProperty("seed", Long.toString(System.currentTimeMillis()));
      log.info("Using '{}' as the random seed for this run. If you want to run the same load again, add 'seed={}' to the properties file",
              seed, seed);
      random = new Random(Long.parseLong(seed));
    }
    return random;
  }
}
