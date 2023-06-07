package team.unison.remote;

import static java.rmi.registry.Registry.REGISTRY_PORT;

import java.io.IOException;
import java.rmi.Remote;
import java.util.List;
import java.util.Map;

public interface Agent extends Remote {
  String AGENT_REGISTRY_NAME = "Agent";
  int AGENT_REGISTRY_PORT = REGISTRY_PORT;

  String info() throws IOException;

  long[] load(Map<String, String> conf, Map<String, Long> arg, Map<String, String> command) throws IOException;

  void move(String path, List<String> arg) throws IOException;

  void kinit(String principal, String keytab) throws IOException;
}