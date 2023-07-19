package team.unison.remote;

import static java.rmi.registry.Registry.REGISTRY_PORT;

import java.io.IOException;
import java.rmi.Remote;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public interface Agent extends Remote {
  String AGENT_REGISTRY_NAME = "Agent";
  int AGENT_REGISTRY_PORT = REGISTRY_PORT;

  String info() throws IOException;

  long[] runCommand(Map<String, String> conf, Map<String, Long> arg, Map<String, String> command) throws IOException;

  List<long[]> runMixedWorkload(Map<String, String> conf, Map<String, Long> batch, List<Map<String, String>> workload) throws IOException;

  long[] clean(Map<String, String> conf, List<String> paths, List<String> suffixes, int threads) throws IOException;

  void init(Properties properties) throws IOException;

  void shutdown() throws IOException;

  void clearStatistics() throws IOException;

  String jstack(String className) throws IOException;
}