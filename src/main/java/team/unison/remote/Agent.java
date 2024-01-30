/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.remote;

import team.unison.perf.loader.FsLoaderOperationConf;

import java.io.IOException;
import java.rmi.Remote;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.rmi.registry.Registry.REGISTRY_PORT;

public interface Agent extends Remote {
  String AGENT_REGISTRY_NAME = "Agent";
  int AGENT_REGISTRY_PORT = REGISTRY_PORT;

  String info() throws IOException;

  long[] runCommand(Map<String, String> conf, Map<String, Long> arg, Map<String, String> command, FsLoaderOperationConf opConf) throws IOException;

  List<long[]> runMixedWorkload(Map<String, String> conf, Map<String, Long> batch, List<Map<String, String>> workload, FsLoaderOperationConf opConf) throws IOException;

  long[] clean(Map<String, String> conf, List<String> paths, List<String> suffixes, int threads) throws IOException;

  void init(Properties properties) throws IOException;

  void shutdown() throws IOException;

  void clearStatistics() throws IOException;

  String jstack(String className) throws IOException;

  byte[] nextChunk(String path) throws IOException;
}