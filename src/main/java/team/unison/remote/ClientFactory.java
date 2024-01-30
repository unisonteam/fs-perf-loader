/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.remote;

public final class ClientFactory {
  public static GenericWorkerBuilder buildGeneric() {
    return new GenericWorkerBuilder();
  }
}
