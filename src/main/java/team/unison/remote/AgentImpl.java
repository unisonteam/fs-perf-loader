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
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.FsWrapperFactory;
import team.unison.perf.jstack.JstackSaverRemote;
import team.unison.perf.loader.FsLoaderBatchRemote;
import team.unison.perf.loader.FsLoaderOperationConf;
import team.unison.perf.snapshot.FsSnapshotterBatchRemote;
import team.unison.perf.snapshot.FsSnapshotterOperationConf;
import team.unison.perf.stats.StatisticsDTO;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
import java.util.*;
import java.util.concurrent.*;

import static team.unison.remote.Utils.sleep;

class AgentImpl implements Agent, Unreferenced {
  private static final Logger log = LoggerFactory.getLogger(AgentImpl.class);
  private static final Map<Thread, FsWrapper> threadToFsWrapper = new ConcurrentHashMap<>();
  private static ExecutorService executorService;
  private FsLoaderBatchRemote fsLoaderBatchRemote;

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
  public void init(@Nonnull Map<String, String> conf, int threads, @Nullable String fillValue) {
   executorService = Executors.newFixedThreadPool(threads, runnable -> {
     Thread thread = new Thread(runnable);
     threadToFsWrapper.computeIfAbsent(thread, t -> FsWrapperFactory.get(conf));
     return thread;
   });
   if (fillValue != null) {
     fsLoaderBatchRemote = new FsLoaderBatchRemote();
     fsLoaderBatchRemote.fillData(fillValue);
   }
  }

  @Override
  public StatisticsDTO runCommand(Map<String, Long> batch, Map<String, String> command,
                                  FsLoaderOperationConf opConf) {
    return fsLoaderBatchRemote.runCommand(executorService, threadToFsWrapper, batch, command, opConf);
  }

  @Override
  public StatisticsDTO runMixedWorkload(Map<String, Long> batch, List<Map<String, String>> workload,
                                        FsLoaderOperationConf opConf) {
    return fsLoaderBatchRemote.runMixedWorkload(executorService, threadToFsWrapper, batch, workload, opConf);
  }

  @Override
  public StatisticsDTO snapshot(List<String> paths, FsSnapshotterOperationConf opConf) throws IOException {
    return FsSnapshotterBatchRemote.snapshot(executorService, threadToFsWrapper, paths, opConf);
  }

  @Override
  public StatisticsDTO clean(List<String> paths, List<String> suffixes) {
    return FsCleanerRemote.apply(executorService, threadToFsWrapper, paths, suffixes);
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
  public void init(Properties properties) {
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
    log.info("ExecutorService stopped at {}", new Date());
    executorService.shutdownNow();
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
}
