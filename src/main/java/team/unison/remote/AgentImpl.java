package team.unison.remote;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.server.Unreferenced;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PrometheusUtils;
import team.unison.perf.cleaner.FsCleanerRemote;
import team.unison.perf.loader.FsLoaderBatchRemote;

class AgentImpl implements Agent, Unreferenced {
  private static final Logger log = LoggerFactory.getLogger(AgentImpl.class);

  private AgentImpl() {
  }

  public static synchronized void start() {
    log.info("Agent started at {}", new Date());
    try {
      Registry registry = LocateRegistry.createRegistry(AGENT_REGISTRY_PORT);
      Agent instance = new AgentImpl();
      UnicastRemoteObject.exportObject(instance, AGENT_REGISTRY_PORT);
      registry.bind(AGENT_REGISTRY_NAME, instance);
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
  public long[] load(Map<String, String> conf, Map<String, Long> arg, Map<String, String> command) {
    return FsLoaderBatchRemote.apply(conf, arg, command);
  }

  @Override
  public long[] clean(Map<String, String> conf, List<String> paths, List<String> suffixes, int threads) {
    return FsCleanerRemote.apply(conf, paths, suffixes, threads);
  }

  @Override
  public void init(Properties properties) throws IOException {
    // init kerberos
    String principal = properties.getProperty("kerberos.principal");
    String keytab = properties.getProperty("kerberos.keytab");
    if ((principal != null) && (keytab != null)) {
      log.info("kinit {} {}", principal, keytab);
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }
    // init prometheus
    PrometheusUtils.init(properties);
  }

  @Override
  public void shutdown() throws IOException {
    PrometheusUtils.shutdown();
  }

  @Override
  public void unreferenced() {
    log.info("Agent stopped at {}", new Date());
    System.exit(0);
  }
}
