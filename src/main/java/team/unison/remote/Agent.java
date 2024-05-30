/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.remote;

import team.unison.perf.stats.StatisticsDTO;
import team.unison.transfer.FsWrapperDataForOperation;

import javax.annotation.Nonnull;
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

  void setupAgent(@Nonnull FsWrapperDataForOperation workerData) throws IOException;

  StatisticsDTO runCommand(@Nonnull String loaderName, Map<String, Long> batch, Map<String, String> command)
      throws IOException;

  StatisticsDTO runMixedWorkload(@Nonnull String loaderName, Map<String, Long> batch, List<Map<String, String>> workload)
      throws IOException;

  StatisticsDTO snapshot(@Nonnull String fsSnapshotterName, List<String> paths) throws IOException;

  StatisticsDTO clean(@Nonnull String fsCleanerName, List<String> paths, List<String> suffixes) throws IOException;

  void setupAgent(Properties properties) throws IOException;

  void shutdown() throws IOException;

  void clearStatistics() throws IOException;

  String jstack(String className) throws IOException;

  byte[] nextChunk(String path) throws IOException;
}