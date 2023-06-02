package team.unison.remote;

public class WorkerException extends RuntimeException {
  private WorkerException(Exception e) {
    super(e);
  }

  public static WorkerException wrap(Exception e) {
    if (e instanceof WorkerException) {
      return (WorkerException) e;
    }
    return new WorkerException(e);
  }
}
