package team.unison.perf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.cleaner.FsCleaner;
import team.unison.perf.filetransfer.FileTransfer;
import team.unison.perf.jstack.JstackSaver;
import team.unison.perf.loader.FsLoader;
import team.unison.remote.SshConnectionBuilder;
import team.unison.remote.SshConnectionFactory;

public final class PerfLoaderMain {
  private static final Logger log = LoggerFactory.getLogger(PerfLoaderMain.class);
  private static final String DEFAULT_PROPERTIES_FILE_PATH = "loader.properties";
  private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);

  private PerfLoaderMain() {
  }

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception in thread {}", t.getName(), e));

    String propertiesFilePath = parseArgs(args);
    Properties properties = new Properties();
    try {
      properties.load(Files.newBufferedReader(Paths.get(propertiesFilePath)));
    } catch (IOException e) {
      usage();
    }

    PerfLoaderUtils.setGlobalProperties(properties);

    ConfsPropertiesBuilder.build(properties);
    SshConnectionBuilder sshConnectionBuilder = sshConnectionBuilder(properties);

    List<FsLoader> fsLoaders = FsLoaderPropertiesBuilder.build(properties, sshConnectionBuilder);
    List<JstackSaver> jstackSavers = JstackSaverPropertiesBuilder.build(properties, sshConnectionBuilder);
    List<FileTransfer> fileTransfers = FileTransferPropertiesBuilder.build(properties, sshConnectionBuilder);

    jstackSavers.forEach(j -> SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(j, j.getPeriod().toMillis(), j.getPeriod().toMillis(),
                                                                             TimeUnit.MILLISECONDS));
    fileTransfers.forEach(f -> SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(f, f.getPeriod().toMillis(), f.getPeriod().toMillis(),
                                                                              TimeUnit.MILLISECONDS));

    if (!fsLoaders.isEmpty()) {
      ExecutorService executorService = Executors.newFixedThreadPool(fsLoaders.size());

      List<Callable<Object>> callables = fsLoaders.stream()
          .map(Executors::callable)
          .collect(Collectors.toList());

      try {
        try {
          executorService.invokeAll(callables);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } finally {
        executorService.shutdownNow();
      }
    }

    List<FsCleaner> fsCleaners = FsCleanerPropertiesBuilder.build(properties, sshConnectionBuilder);
    fsCleaners.forEach(FsCleaner::run);

    if (fsLoaders.isEmpty() && !jstackSavers.isEmpty()) {
      System.out.println("Collecting thread dumps. Press Ctrl+C to exit");
    } else {
      SCHEDULED_EXECUTOR_SERVICE.shutdown();
    }
  }

  private static SshConnectionBuilder sshConnectionBuilder(Properties properties) {
    String sshUser = properties.getProperty("ssh.user", System.getenv("USER"));
    String sshKey = properties.getProperty("ssh.key");
    String sshPassword = properties.getProperty("ssh.password");

    if ((sshKey == null) && (sshPassword == null)) {
      sshKey = System.getenv("HOME") + "/.ssh/id_rsa";
    }

    return SshConnectionFactory.build().sshUser(sshUser).identity(sshKey).password(sshPassword);
  }

  private static String parseArgs(String... args) {
    if (args.length == 0) {
      return DEFAULT_PROPERTIES_FILE_PATH;
    }
    if (args.length > 1 || args[0].startsWith("-")) {
      usage();
    }
    return args[0];
  }

  private static void usage() {
    System.out.println("Usage: PerfLoaderMain [properties file]. Default properties file is " + DEFAULT_PROPERTIES_FILE_PATH);
    System.exit(64);
  }
}