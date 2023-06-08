package team.unison.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.PerfLoaderMain;

public class RemoteMain {
  private static final Logger log = LoggerFactory.getLogger(PerfLoaderMain.class);

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception in thread {}", t.getName(), e));
    System.setProperty("java.rmi.server.hostname", args[0]);
    AgentImpl.start();
  }
}
