/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.perf.jstack;

import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.remote.RemoteMain;
import team.unison.remote.WorkerException;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static team.unison.remote.Utils.sleep;

public class JstackSaverRemote {
  private static final Logger log = LoggerFactory.getLogger(JstackSaverRemote.class);

  private static final int MAX_FRAMES = 128;
  private static final Map<String, ThreadMXBean> THREAD_MX_BEANS = new ConcurrentHashMap<>();
  private static final Map<String, MBeanServerConnection> MBEAN_SERVER_CONNECTIONS = new ConcurrentHashMap<>();

  public static String jstack(String className) {
    boolean clientJstack = RemoteMain.class.getSimpleName().equals(className) || "".equals(className) || (className == null);

    if (!clientJstack) {
      for (int i = 0; i < 10; i++) {
        try {
          getMBeanServerConnection(className).getDefaultDomain();
          break;
        } catch (IOException | WorkerException e) {
          log.warn("Error calling getDefaultDomain for MBeanServerConnection for class {}, attempt {}", className, i);
          sleep(1000);
          THREAD_MX_BEANS.remove(className);
        }
      }
    }

    ThreadMXBean threadMXBean =
            clientJstack ? ManagementFactory.getThreadMXBean() :
                    THREAD_MX_BEANS.computeIfAbsent(className, cn -> getMxBean(className, ManagementFactory.THREAD_MXBEAN_NAME, ThreadMXBean.class));

    long[] allThreadIds = threadMXBean.getAllThreadIds();
    ThreadInfo[] threads = threadMXBean.getThreadInfo(allThreadIds, MAX_FRAMES);
    return Arrays.stream(threads).map(JstackSaverRemote::threadInfoToString).collect(Collectors.joining("\n"));
  }

  // returns MBeanServerConnection to JVM with main class = className
  private static MBeanServerConnection getMBeanServerConnection(String className) {
    return MBEAN_SERVER_CONNECTIONS.computeIfAbsent(className, JstackSaverRemote::newMBeanServerConnection);
  }

  private static <T> T getMxBean(String className, String mxBeanName, Class<T> clazz) {
    try {
      return ManagementFactory.newPlatformMXBeanProxy(getMBeanServerConnection(className), mxBeanName, clazz);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static MBeanServerConnection newMBeanServerConnection(String className) {
    try {
      List<VirtualMachineDescriptor> vmList = VirtualMachine.list();

      VirtualMachine vm = null;

      for (VirtualMachineDescriptor vmd : vmList) {
        log.info("VM: [{}], displayName: [{}], id: [{}]", vmd, vmd.displayName(), vmd.id());
        String vmClassName = vmd.displayName().split(" ")[0];
        // displayName may contain not class name but path to jar - use two options for 'endsWith'
        if (vmClassName.endsWith("." + className) || vmClassName.endsWith("/" + className)) {
          vm = VirtualMachine.attach(vmd);
        }
      }

      if (vm == null) {
        throw new IllegalArgumentException("VM running class " + className + " not found");
      }

      String serviceURL = vm.startLocalManagementAgent();
      log.info("serviceUrl: {}", serviceURL);

      JMXServiceURL jmxServiceURL = new JMXServiceURL(serviceURL);
      JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, null); // NOPMD
      return jmxConnector.getMBeanServerConnection();
    } catch (Exception e) {
      throw WorkerException.wrap(e);
    }
  }

  // copy ot ThreadInfo.toString() with increased stack trace depth
  private static String threadInfoToString(ThreadInfo ti) {
    // it seems that some threads may finish between 'getAllThreadIds' and 'getThreadInfo'

    if (ti == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder("\"" + ti.getThreadName() + "\""
            + " Id=" + ti.getThreadId() + " "
            + ti.getThreadState());
    if (ti.getLockName() != null) {
      sb.append(" on " + ti.getLockName());
    }
    if (ti.getLockOwnerName() != null) {
      sb.append(" owned by \"" + ti.getLockOwnerName() + "\" Id=" + ti.getLockOwnerId());
    }
    if (ti.isSuspended()) {
      sb.append(" (suspended)");
    }
    if (ti.isInNative()) {
      sb.append(" (in native)");
    }
    sb.append('\n');
    int i = 0;
    StackTraceElement[] stackTrace = ti.getStackTrace();
    for (; i < stackTrace.length && i < MAX_FRAMES; i++) {
      StackTraceElement ste = stackTrace[i];
      sb.append("\tat " + ste.toString());
      sb.append('\n');
      if (i == 0 && ti.getLockInfo() != null) {
        Thread.State ts = ti.getThreadState();
        switch (ts) {
          case BLOCKED:
            sb.append("\t-  blocked on " + ti.getLockInfo());
            sb.append('\n');
            break;
          case WAITING:
            sb.append("\t-  waiting on " + ti.getLockInfo());
            sb.append('\n');
            break;
          case TIMED_WAITING:
            sb.append("\t-  waiting on " + ti.getLockInfo());
            sb.append('\n');
            break;
          default:
        }
      }

      for (MonitorInfo mi : ti.getLockedMonitors()) {
        if (mi.getLockedStackDepth() == i) {
          sb.append("\t-  locked " + mi);
          sb.append('\n');
        }
      }
    }
    if (i < stackTrace.length) {
      sb.append("\t...");
      sb.append('\n');
    }

    LockInfo[] locks = ti.getLockedSynchronizers();
    if (locks.length > 0) {
      sb.append("\n\tNumber of locked synchronizers = " + locks.length);
      sb.append('\n');
      for (LockInfo li : locks) {
        sb.append("\t- " + li);
        sb.append('\n');
      }
    }
    sb.append('\n');
    return sb.toString();
  }
}