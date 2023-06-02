package team.unison.remote;

class WorkerImpl implements GenericWorker {
  private final Agent agent;
  private final String host;

  WorkerImpl(Agent agent, String host) {
    this.agent = agent;
    this.host = host;
  }

  @Override
  public Agent getAgent() {
    return agent;
  }

  @Override
  public String getHost() {
    return host;
  }
}
