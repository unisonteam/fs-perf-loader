package team.unison.remote;

import static team.unison.remote.Utils.sleep;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PerfLoaderUtils;

public class GenericWorkerBuilder implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(GenericWorkerBuilder.class);

  private transient SshConnectionBuilder sshConnectionBuilder;
  private static final Map<SshConnectionBuilder, GenericWorker> DEPLOYED_WORKERS = new ConcurrentHashMap<>();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> DEPLOYED_WORKERS.values().forEach(w -> {
      try {
        w.getAgent().shutdown();
      } catch (IOException e) {
        log.warn("Error in shutdown", e);
      }
    })));
  }

  GenericWorkerBuilder() {
  }

  private GenericWorkerBuilder(GenericWorkerBuilder other) {
    this.sshConnectionBuilder = other.sshConnectionBuilder;
  }

  private GenericWorkerBuilder copy() {
    return new GenericWorkerBuilder(this);
  }

  public GenericWorker get() {
    return DEPLOYED_WORKERS.computeIfAbsent(sshConnectionBuilder, GenericWorkerBuilder::newInstance);
  }

  public static GenericWorker newInstance(SshConnectionBuilder sshConnectionBuilder) {
    String agentRegistryName = Agent.AGENT_REGISTRY_NAME
        + (sshConnectionBuilder.getSystemUser() == null ? "" : "_" + sshConnectionBuilder.getSystemUser())
        + (sshConnectionBuilder.getSystemGroup() == null ? "" : "__" + sshConnectionBuilder.getSystemGroup());

    String connectionLocation = "//" + sshConnectionBuilder.getHost() + ":" + Agent.AGENT_REGISTRY_PORT + "/" + agentRegistryName;
    log.info("Connection location: " + connectionLocation);

    try {
      Agent agent = (Agent) Naming.lookup(connectionLocation);
      agent.info(); // ping
      // if agent is alive - continue without new deploy
      log.info("Connection to old instance of " + connectionLocation + " was successful");
      return new WorkerImpl(agent, sshConnectionBuilder.getHost());
    } catch (IOException | NotBoundException e) { // no old instance - will start a new one
    }

    RemoteExec.deployAgent(sshConnectionBuilder, agentRegistryName);
    try {
      Agent agent = null;
      for (int i = 0; i < 600; i++) {
        try {
          agent = (Agent) Naming.lookup(connectionLocation);
          agent.info(); // ping
          agent.init(PerfLoaderUtils.getGlobalProperties());

          log.info("Connection to " + connectionLocation + " was successful");
          break;
        } catch (IOException | NotBoundException e) {
          sleep(100);
        }
      }
      return new WorkerImpl(agent, sshConnectionBuilder.getHost());
    } catch (Exception e) {
      throw WorkerException.wrap(e);
    }
  }

  public GenericWorkerBuilder sshConnectionBuilder(SshConnectionBuilder sshConnectionBuilder) {
    GenericWorkerBuilder copy = copy();
    copy.sshConnectionBuilder = sshConnectionBuilder;
    return copy;
  }

  public SshConnectionBuilder getSshConnectionBuilder() {
    return sshConnectionBuilder;
  }

  @Override
  public String toString() {
    return "GenericWorkerBuilder{sshConnectionBuilder=" + sshConnectionBuilder + '}';
  }
}
