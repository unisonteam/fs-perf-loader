/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class SshConnectionBuilder implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(SshConnectionBuilder.class);
  private String host;
  private String sshUser;
  private String systemUser;
  private String systemGroup;
  private String identity;
  private String password;

  SshConnectionBuilder() {
  }

  private SshConnectionBuilder(SshConnectionBuilder other) {
    this.host = other.host;
    this.sshUser = other.sshUser;
    this.systemUser = other.systemUser;
    this.systemGroup = other.systemGroup;
    this.identity = other.identity;
    this.password = other.password;
  }

  private SshConnectionBuilder copy() {
    return new SshConnectionBuilder(this);
  }

  public SshConnectionBuilder host(String host) {
    SshConnectionBuilder copy = copy();
    copy.host = host;
    return copy;
  }

  public String getHost() {
    return host;
  }

  public SshConnectionBuilder sshUser(String sshUser) {
    SshConnectionBuilder copy = copy();
    copy.sshUser = sshUser;
    return copy;
  }

  public String getSshUser() {
    return sshUser;
  }

  public SshConnectionBuilder systemUser(String systemUser) {
    SshConnectionBuilder copy = copy();
    copy.systemUser = systemUser;
    return copy;
  }

  public String getSystemUser() {
    return systemUser;
  }

  public SshConnectionBuilder systemGroup(String systemGroup) {
    SshConnectionBuilder copy = copy();
    copy.systemGroup = systemGroup;
    return copy;
  }

  public String getSystemGroup() {
    return systemGroup;
  }

  public SshConnectionBuilder identity(String identity) {
    SshConnectionBuilder copy = copy();
    copy.identity = identity;
    return copy;
  }

  String getIdentity() {
    return identity;
  }

  public SshConnectionBuilder password(String password) {
    SshConnectionBuilder copy = copy();
    copy.password = password;
    return copy;
  }

  String getPassword() {
    return password;
  }

  public void validate() {
    if (host == null && sshUser == null && identity == null && password == null) {
      return;
    }
    if (host == null || sshUser == null || (identity == null && password == null)) {
      log.error("All field should be set for SSH connection: host, port and identity or password " + toString());
      throw new IllegalArgumentException("All field should be set for SSH connection: host, port and identity or password " + toString());
    }
  }

  public boolean put(InputStream inputStream, String remoteFileName) {
    validate();
    return SshConnectionFactory.put(this, inputStream, remoteFileName);
  }

  public SshRunResult run(String command) {
    return run(command, Duration.ofMinutes(5));
  }

  public SshRunResult run(String command, Map<String, String> env) {
    return run(command, env, Duration.ofMinutes(5));
  }

  public SshRunResult run(String command, Duration duration) {
    return run(command, Collections.emptyMap(), duration);
  }

  public SshRunResult run(String command, Map<String, String> env, Duration duration) {
    validate();
    return SshConnectionFactory.run(this, command, env, duration);
  }

  @Override
  public String toString() {
    return "SshConnectionBuilder{host='" + host + '\'' + ", sshUser='" + sshUser + '\'' + ", identity='" + identity + '\'' + ", "
            + "password='" + password + '\'' + ", systemUser='" + systemUser + '\'' + ", systemGroup='" + systemGroup + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SshConnectionBuilder that = (SshConnectionBuilder) o;
    return Objects.equals(host, that.host) && Objects.equals(sshUser, that.sshUser) && Objects.equals(systemUser, that.systemUser)
            && Objects.equals(systemGroup, that.systemGroup);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, sshUser, systemUser, systemGroup);
  }

}
