package team.unison.remote;

import static java.rmi.registry.Registry.REGISTRY_PORT;

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
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.loader.FsLoaderBatchRemote;
import team.unison.perf.mover.FsMoverRemote;

class AgentImpl implements Agent, Unreferenced {
  private static final Logger LOGGER = LoggerFactory.getLogger(AgentImpl.class);

  private AgentImpl() {
  }

  public static synchronized void start() {
    LOGGER.info("Agent started at {}", new Date());
    try {
      Registry registry = LocateRegistry.createRegistry(REGISTRY_PORT);
      Agent instance = new AgentImpl();
      UnicastRemoteObject.exportObject(instance, REGISTRY_PORT);
      registry.bind(REGISTRY_NAME, instance);
    } catch (IOException e) {
      LOGGER.error("IOException in start()", e);
      throw new UncheckedIOException(e);
    } catch (AlreadyBoundException e) {
      LOGGER.error("AlreadyBoundException in start()", e);
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
  public long[] load(Map<String, String> conf, Map<String, Long> arg, List<Map<String, String>> workload) {
    return FsLoaderBatchRemote.apply(conf, arg, workload);
  }

  @Override
  public void move(String path, List<String> arg) {
    try {
      FsMoverRemote.apply(path, arg);
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    }
  }

  @Override
  public void kinit(String principal, String keytab) throws IOException {
    if ((principal != null) && (keytab != null)) {
      LOGGER.info("kinit {} {}", principal, keytab);
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }
  }

  @Override
  public void unreferenced() {
    LOGGER.info("Agent stopped at {}", new Date());
    System.exit(0);
  }
}
