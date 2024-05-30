/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.remote;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.cleaner.FsCleanerRemote;
import team.unison.perf.filetransfer.FileTransferRemote;
import team.unison.perf.jstack.JstackSaverRemote;
import team.unison.perf.loader.FsLoaderBatchRemote;
import team.unison.perf.snapshot.FsSnapshotterBatchRemote;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.transfer.FsWrapperDataForOperation;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.server.Unreferenced;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static team.unison.remote.Utils.sleep;

class AgentImpl implements Agent, Unreferenced {
  private static final Logger log = LoggerFactory.getLogger(AgentImpl.class);
  private static final Map<String, FsWrapperContainer> FS_WRAPPER_CONTAINER_MAP = new ConcurrentHashMap<>();

  private AgentImpl() {
  }

  public static synchronized void start(String registryName, int exportPort) {
    log.info("Agent started at {}, registry name is {}", new Date(), registryName);
    try {
      Registry registry;
      try {
        registry = LocateRegistry.getRegistry(AGENT_REGISTRY_PORT);
        registry.list(); // throws exception in case if problems
        log.info("Bound to existing registry");
      } catch (RemoteException e) {
        log.info("Creating a new registry");
        registry = LocateRegistry.createRegistry(AGENT_REGISTRY_PORT);
      }
      Agent instance = new AgentImpl();
      UnicastRemoteObject.exportObject(instance, exportPort);
      registry.bind(registryName, instance);
    } catch (IOException e) {
      log.error("IOException in start()", e);
      throw new UncheckedIOException(e);
    } catch (AlreadyBoundException e) {
      log.error("AlreadyBoundException in start()", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String info() {
    try {
      return "Host : " + InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void setupAgent(@Nonnull FsWrapperDataForOperation data) {
    FS_WRAPPER_CONTAINER_MAP.putIfAbsent(data.fsWrapperName, FsWrapperContainerFactory.get(data));
  }

  @Override
  public StatisticsDTO runCommand(
      @Nonnull String loaderName,
      @Nonnull Map<String, Long> batch,
      @Nonnull Map<String, String> command
  ) {
    FsWrapperContainer fsWrapperContainer = FS_WRAPPER_CONTAINER_MAP.get(loaderName);
    FsLoaderBatchRemote fsLoader = getWrapper(fsWrapperContainer);
    return fsLoader.runCommand(fsWrapperContainer.executor, batch, command);
  }

  @Override
  public StatisticsDTO runMixedWorkload(
      @Nonnull String loaderName,
      @Nonnull Map<String, Long> batch,
      @Nonnull List<Map<String, String>> workload
  ) {
    FsWrapperContainer fsWrapperContainer = FS_WRAPPER_CONTAINER_MAP.get(loaderName);
    FsLoaderBatchRemote fsLoader = getWrapper(fsWrapperContainer);
    return fsLoader.runMixedWorkload(fsWrapperContainer.executor, batch, workload);
  }

  @Override
  public StatisticsDTO snapshot(@Nonnull String snapshotterName, List<String> paths) throws IOException {
    FsWrapperContainer fsWrapperContainer = FS_WRAPPER_CONTAINER_MAP.get(snapshotterName);
    FsSnapshotterBatchRemote fsSnapshotter = getWrapper(fsWrapperContainer);
    return fsSnapshotter.snapshot(fsWrapperContainer.executor, paths);
  }

  @Override
  public StatisticsDTO clean(@Nonnull String cleanerName, List<String> paths, List<String> suffixes) {
    FsWrapperContainer fsWrapperContainer = FS_WRAPPER_CONTAINER_MAP.get(cleanerName);
    return FsCleanerRemote.apply(fsWrapperContainer.executor, paths, suffixes);
  }

  @Override
  public String jstack(String className) throws IOException {
    return JstackSaverRemote.jstack(className);
  }

  @Override
  public byte[] nextChunk(String path) {
    return FileTransferRemote.nextChunk(path);
  }

  @Override
  public void setupAgent(Properties properties) {
    // init kerberos
    String principal = properties.getProperty("kerberos.principal");
    String keytab = properties.getProperty("kerberos.keytab");
    if ((principal != null) && (keytab != null)) {
      log.info("kinit {} {}", principal, keytab);
      try {
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
      } catch (IOException e) {
        log.warn("Can't kinit", e);
      }
    }
    // init prometheus
    PrometheusUtils.init(properties);
  }

  @Override
  public void shutdown() {
    log.info("Agent stopped at {}", new Date());
    PrometheusUtils.clearStatistics(true);
    FS_WRAPPER_CONTAINER_MAP.forEach((k, fsWrapperContainer) -> {
      log.info("Release resources for fsWrapperContainer {}", k);
      fsWrapperContainer.executor.shutdownNow();
    });
    FS_WRAPPER_CONTAINER_MAP.clear();
    log.info("Shutdown VM {}", new Date());
    // start a separate thread to shut down VM to respond properly to the remote caller
    new Thread(() -> {
      sleep(1000);
      System.exit(0);
    }).start();
  }

  @Override
  public void clearStatistics() throws IOException {
    PrometheusUtils.clearStatistics(false);
  }

  @Override
  public void unreferenced() {
    shutdown();
  }

  @SuppressWarnings("unchecked")
  private static <T> T getWrapper(@Nonnull FsWrapperContainer wrapperContainer) {
    if (wrapperContainer.worker instanceof FsLoaderBatchRemote) {
      return (T) wrapperContainer.worker;
    } if (wrapperContainer.worker instanceof FsSnapshotterBatchRemote) {
      return (T) wrapperContainer.worker;
    } else {
      throw new IllegalStateException("Got wrong loader name or this container doesn't have worker " +
          "(use static functions)!");
    }
  }
}
