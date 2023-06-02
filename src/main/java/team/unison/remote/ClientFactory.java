package team.unison.remote;

public final class ClientFactory {
  public static GenericWorkerBuilder buildGeneric() {
    return new GenericWorkerBuilder();
  }
}
