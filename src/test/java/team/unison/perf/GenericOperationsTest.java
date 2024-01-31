/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import team.unison.remote.ClientFactory;
import team.unison.remote.GenericWorker;
import team.unison.remote.SshConnectionBuilder;

import java.io.IOException;

@Disabled
public class GenericOperationsTest {
  SshConnectionBuilder sshConnectionBuilder = TestUtils.sshConnectionBuilderFromProperties();
  GenericWorker genericWorker = ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilder).get();

  @Test
  public void test() throws IOException {
    System.out.println("Host ip is " + genericWorker.getAgent().info());
  }

  // SSH:
  // 3-arg DemoIpCommand 100 loops 3 min 25 sec = 200 sec
  // one call - 2 сек
  // RPC:
  // 3-arg DemoIpCommand 10_000 loops 8 sec
  // one call - 0.0008 sec
  @Test
  public void testLoop() throws IOException {
    for (int i = 0; i < 10_000; i++) {
      genericWorker.getAgent().info();
    }
  }

  @Test
  public void testLoop1() throws IOException {
    testLoop();
  }

  @Test
  public void testLoop2() throws IOException {
    testLoop();
  }

}

