/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.remote.SshConnectionBuilder;
import team.unison.remote.SshConnectionFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

public class TestUtils {
  private static final Logger log = LoggerFactory.getLogger(TestUtils.class);
  private static final Properties TESTS_PROPERTIES = new Properties();

  private TestUtils() {
  }

  static {
    try {
      TESTS_PROPERTIES.load(TestUtils.class.getResourceAsStream("/tests.properties"));
    } catch (IOException e) {
      log.error("Can't initialize tests.properties", e);
      throw new UncheckedIOException("Can't initialize tests.properties", e);
    }
  }

  public static SshConnectionBuilder sshConnectionBuilderFromProperties() {
    return sshConnectionBuilderFromProperties("");
  }

  public static SshConnectionBuilder sshConnectionBuilderFromProperties(String suffix) {
    return SshConnectionFactory.build()
            .sshUser(TESTS_PROPERTIES.getProperty("sshUser" + suffix, TESTS_PROPERTIES.getProperty("sshUser")))
            .identity(TESTS_PROPERTIES.getProperty("identity" + suffix, TESTS_PROPERTIES.getProperty("identity")))
            .systemUser(TESTS_PROPERTIES.getProperty("systemUser" + suffix, TESTS_PROPERTIES.getProperty("systemUser")))
            .host(TESTS_PROPERTIES.getProperty("host" + suffix));
  }
}
