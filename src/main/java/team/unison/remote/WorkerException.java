/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

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
