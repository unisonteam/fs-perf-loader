package team.unison.remote;

import java.io.IOException;
import java.rmi.Remote;
import java.util.List;
import java.util.Map;

public interface Agent extends Remote {
  String REGISTRY_NAME = "Agent";

  String info() throws IOException;

  long[] load(Map<String, String> conf, Map<String, Long> arg, List<Map<String, String>> workload) throws IOException;

  void move(String path, List<String> arg) throws IOException;

  void kinit(String principal, String keytab) throws IOException;
}