/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PerfLoaderMain;

public class RemoteMain {
  private static final Logger log = LoggerFactory.getLogger(PerfLoaderMain.class);

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception in thread {}", t.getName(), e));
    String host = args[0];
    String registryName = args[1];
    int exportPort = Integer.parseInt(args[2]);
    System.setProperty("java.rmi.server.hostname", host);
    AgentImpl.start(registryName, exportPort);
  }
}
