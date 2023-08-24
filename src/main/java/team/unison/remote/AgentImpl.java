package team.unison.remote;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.cleaner.FsCleanerRemote;
import team.unison.perf.filetransfer.FileTransferRemote;
import team.unison.perf.jstack.JstackSaverRemote;
import team.unison.perf.loader.FsLoaderBatchRemote;
import team.unison.perf.loader.FsLoaderOperationConf;

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

import static team.unison.remote.Utils.sleep;

class AgentImpl implements Agent, Unreferenced {
  private static final Logger log = LoggerFactory.getLogger(AgentImpl.class);

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
  public long[] runCommand(Map<String, String> conf, Map<String, Long> batch, Map<String, String> command, FsLoaderOperationConf opConf) {
    return FsLoaderBatchRemote.runCommand(conf, batch, command, opConf);
  }

  @Override
  public List<long[]> runMixedWorkload(Map<String, String> conf, Map<String, Long> batch, List<Map<String, String>> workload, FsLoaderOperationConf opConf) {
    return FsLoaderBatchRemote.runMixedWorkload(conf, batch, workload, opConf);
  }

  @Override
  public long[] clean(Map<String, String> conf, List<String> paths, List<String> suffixes, int threads) {
    return FsCleanerRemote.apply(conf, paths, suffixes, threads);
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
