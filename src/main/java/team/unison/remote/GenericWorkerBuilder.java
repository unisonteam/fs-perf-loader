package team.unison.remote;

import static java.rmi.registry.Registry.REGISTRY_PORT;
import static team.unison.remote.Utils.sleep;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.Naming;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PerfLoaderUtils;

public class GenericWorkerBuilder implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericWorkerBuilder.class);

  private transient SshConnectionBuilder sshConnectionBuilder;
  private static final Map<SshConnectionBuilder, GenericWorker> DEPLOYED_WORKERS = new ConcurrentHashMap<>();

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
    RemoteExec.deployAgent(sshConnectionBuilder);

    String connectionLocation = "//" + sshConnectionBuilder.getHost() + ":" + REGISTRY_PORT + "/" + Agent.REGISTRY_NAME;
    LOGGER.info("Connection location: " + connectionLocation);
    try {
      Agent agent = null;
      for (int i = 0; i < 600; i++) {
        try {
          agent = (Agent) Naming.lookup(connectionLocation);
          agent.info(); // ping
          agent.kinit(PerfLoaderUtils.getGlobalProperties().getProperty("kerberos.principal"),
                      PerfLoaderUtils.getGlobalProperties().getProperty("kerberos.keytab"));

          LOGGER.info("Connection to " + connectionLocation + " was successful");
          break;
        } catch (IOException e) {
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

  public GenericWorkerBuilder jvmOptions(String jvmOptions) {
    GenericWorkerBuilder copy = copy();
    return copy;
  }

  @Override
  public String toString() {
    return "GenericWorkerBuilder{sshConnectionBuilder=" + sshConnectionBuilder + '}';
  }
}
