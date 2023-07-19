package team.unison.perf;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.loader.FsLoader;
import team.unison.perf.loader.FsLoaderBuilder;
import team.unison.remote.ClientFactory;
import team.unison.remote.GenericWorkerBuilder;
import team.unison.remote.SshConnectionBuilder;

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
      List<String> hosts = PerfLoaderUtils.parseTemplate(props.getProperty(prefix + "hosts").replace(" ", ""));
      List<GenericWorkerBuilder> genericWorkerBuilders = new ArrayList<>();
      for (String host : hosts) {
        genericWorkerBuilders.add(ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilder.host(host)));
      }
      long defaultCommandDelayInSeconds = props.getProperty("prometheus.address") != null ? 25 : 0;
      long commandDelayInSeconds = props.getProperty(prefix + "command.delay") != null ? Long.parseLong(
          props.getProperty(prefix + "command.delay")) : defaultCommandDelayInSeconds;
      FsLoader fsLoader = new FsLoaderBuilder()
          .name(fsLoaderName)
          .conf(Confs.get(props.getProperty(prefix + "conf")))
          .type(FsLoader.Type.valueOf(props.getProperty(prefix + "batch.type", "WINDOW")))
          .random(getRandomFromSeed(props))
          .threads(Integer.parseInt(props.getProperty(prefix + "threads", "8")))
          .filesSizesDistribution(props.getProperty(prefix + "batch.file.sizes"))
          .filesSuffixesDistribution(props.getProperty(prefix + "batch.file.suffixes"))
          .paths(PerfLoaderUtils.parseTemplate(props.getProperty(prefix + "paths", "")))
          .workload(PerfLoaderUtils.parseWorkload(props.getProperty(prefix + "workload")))
          .genericWorkerBuilders(genericWorkerBuilders)
          .useTmpFile(Boolean.parseBoolean(props.getProperty(prefix + "usetmpfile", "false")))
          .loadDelay(Duration.ofMillis(Long.parseLong(props.getProperty(prefix + "delay", "0"))))
          .commandDelay(Duration.ofSeconds(commandDelayInSeconds))
          .count(Integer.parseInt(props.getProperty(prefix + "count", "1")))
          .period(Duration.ofSeconds(Long.parseLong(props.getProperty(prefix + "period", "0"))))
          .batches(Integer.parseInt(props.getProperty(prefix + "batch.count", "10")))
          .batchTimeout(Duration.ofSeconds(Long.parseLong(props.getProperty(prefix + "batch.timeout", "600"))))
          .filesInBatch(Integer.parseInt(props.getProperty(prefix + "batch.file.count", "10")))
          .subdirsDepth(Integer.parseInt(props.getProperty(prefix + "subdirs.depth", "2")))
          .subdirsWidth(Integer.parseInt(props.getProperty(prefix + "subdirs.width", "2")))
          .subdirsFormat(props.getProperty(prefix + "subdirs.format", "%d"))
          .fill(props.getProperty(prefix + "fill", "random"))
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
