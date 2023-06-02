package team.unison.remote;

import java.io.IOException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class GenericOperationsTest {
  SshConnectionBuilder sshConnectionBuilder = TestUtils.sshConnectionBuilderFromProperties();
  GenericWorker genericWorker = ClientFactory.buildGeneric().sshConnectionBuilder(sshConnectionBuilder).get();

  @Test
  public void test() throws IOException {
    System.out.println("Host ip is " + genericWorker.getAgent().info());
  }

  // SSH:
  // 3-arg DemoIpCommand 100 циклов 3 мин 25 сек = 200 сек
  // один вызов - 2 сек
  // RPC:
  // 3-arg DemoIpCommand 10_000 циклов 8 сек
  // один вызов - 0.0008 сек
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

