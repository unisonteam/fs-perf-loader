package team.unison.remote;

public class RemoteMain {
  public static void main(String[] args) throws Exception {
    System.setProperty("java.rmi.server.hostname", args[0]);
    AgentImpl.start();
  }
}
