/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

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
