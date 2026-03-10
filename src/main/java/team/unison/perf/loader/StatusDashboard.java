/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.loader;

import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.graphics.TextGraphics;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.Terminal;
import com.googlecode.lanterna.terminal.ansi.UnixTerminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.stats.StatisticsDTO;
import team.unison.remote.GenericWorker;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class StatusDashboard {
  private static final Logger log = LoggerFactory.getLogger(StatusDashboard.class);
  private static final int MAX_DISPLAYED_HOSTS = 8;
  private static final int RENDER_INTERVAL_MS = 250;
  private static final int MAX_LOG_LINES = 5000;

  private static volatile StatusDashboard activeInstance;

  private final String loaderName;
  private volatile List<GenericWorker> workers;
  private final Duration pollInterval;
  private final int totalBatches;
  private final AtomicInteger completedBatches;
  private final Instant startTime;

  private final LinkedList<String> logLines = new LinkedList<>();
  private volatile List<String> statusLines;

  private Terminal terminal;
  private Screen screen;
  private PrintStream originalOut;
  private ScheduledExecutorService renderScheduler;
  private ScheduledExecutorService pollScheduler;
  private volatile boolean running;
  private Thread shutdownHook;

  private StatusDashboard(String loaderName, Duration pollInterval,
                          int totalBatches, AtomicInteger completedBatches) {
    this.loaderName = loaderName;
    this.workers = Collections.emptyList();
    this.pollInterval = pollInterval;
    this.totalBatches = totalBatches;
    this.completedBatches = completedBatches;
    this.startTime = Instant.now();
    this.statusLines = Arrays.asList("Agent Status", loaderName, "", "Connecting...");
  }

  static synchronized StatusDashboard tryActivate(String loaderName, Duration pollInterval,
                                                   int totalBatches, AtomicInteger completedBatches) {
    if (activeInstance != null) {
      return null;
    }
    StatusDashboard dashboard = new StatusDashboard(loaderName, pollInterval,
            totalBatches, completedBatches);
    try {
      dashboard.start();
      activeInstance = dashboard;
      return dashboard;
    } catch (IOException e) {
      log.warn("Failed to start status dashboard", e);
      return null;
    }
  }

  void setWorkers(List<GenericWorker> workers) {
    this.workers = new ArrayList<>(workers);
  }

  private void start() throws IOException {
    running = true;

    // Save original System.out before redirecting
    originalOut = System.out;

    // Redirect System.out so all subsequent log output is captured.
    // With log4j.appender.stdout.follow=true, log4j dynamically reads System.out
    // on each write, so this redirect takes effect for log4j automatically.
    System.setOut(new PrintStream(new LogCapturingStream(logLines, MAX_LOG_LINES), true));

    // Create Lanterna terminal using saved originalOut — this ensures
    // Lanterna writes escape sequences to the real terminal, not our capturing stream
    terminal = new UnixTerminal(System.in, originalOut, Charset.defaultCharset());
    screen = new TerminalScreen(terminal);
    screen.startScreen();
    screen.setCursorPosition(null); // hide cursor

    // Shutdown hook to restore terminal on Ctrl+C
    shutdownHook = new Thread(() -> {
      if (running) {
        stop();
      }
    }, "dashboard-cleanup");
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    // Render screen at regular intervals
    renderScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "dashboard-render");
      t.setDaemon(true);
      return t;
    });
    renderScheduler.scheduleAtFixedRate(this::render, RENDER_INTERVAL_MS, RENDER_INTERVAL_MS,
            TimeUnit.MILLISECONDS);

    // Poll agents at the configured interval
    pollScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "dashboard-poll");
      t.setDaemon(true);
      return t;
    });
    pollScheduler.scheduleAtFixedRate(this::pollAgents,
            pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  void stop() {
    running = false;

    renderScheduler.shutdown();
    pollScheduler.shutdown();
    try {
      renderScheduler.awaitTermination(1, TimeUnit.SECONDS);
      pollScheduler.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    restoreTerminal();

    // Flush captured log lines to real stdout so they're preserved
    synchronized (logLines) {
      for (String line : logLines) {
        originalOut.println(line);
      }
    }
    originalOut.flush();

    try {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (IllegalStateException e) {
      // JVM is already shutting down
    }

    synchronized (StatusDashboard.class) {
      activeInstance = null;
    }
  }

  private void restoreTerminal() {
    // Restore System.out — with follow=true, log4j picks this up automatically
    System.setOut(originalOut);
    try {
      if (screen != null) {
        screen.stopScreen();
      }
      if (terminal != null) {
        terminal.close();
      }
    } catch (IOException e) {
      // best effort
    }
  }

  private void pollAgents() {
    if (!running) {
      return;
    }
    List<GenericWorker> currentWorkers = workers;
    if (currentWorkers.isEmpty()) {
      return;
    }
    try {
      Map<String, Map<String, long[]>> perHostStats = new LinkedHashMap<>();
      for (GenericWorker worker : currentWorkers) {
        try {
          StatisticsDTO agentStats = worker.getAgent().getRunningStatistics();
          perHostStats.put(worker.getHost(), agentStats.getSummary());
        } catch (Exception e) {
          Map<String, long[]> errorMap = new LinkedHashMap<>();
          errorMap.put("ERROR", new long[]{0, 0});
          perHostStats.put(worker.getHost(), errorMap);
        }
      }
      statusLines = buildStatusLines(perHostStats);
    } catch (Exception e) {
      // swallow to keep polling alive
    }
  }

  private List<String> buildStatusLines(Map<String, Map<String, long[]>> perHostStats) {
    Duration elapsed = Duration.between(startTime, Instant.now());
    int done = completedBatches.get();

    Set<String> allOps = new LinkedHashSet<>();
    for (Map<String, long[]> hostStats : perHostStats.values()) {
      allOps.addAll(hostStats.keySet());
    }
    allOps.remove("ERROR");

    List<String> lines = new ArrayList<>();
    lines.add("Agent Status");
    lines.add(loaderName + " | " + formatDuration(elapsed));
    lines.add(String.format("Batches: %d/%d", done, totalBatches));
    lines.add("");

    long grandTotal = 0;
    long grandFailed = 0;
    int hostsDisplayed = 0;
    int hostsSkipped = 0;

    for (Map.Entry<String, Map<String, long[]>> entry : perHostStats.entrySet()) {
      if (hostsDisplayed >= MAX_DISPLAYED_HOSTS) {
        hostsSkipped++;
        for (Map.Entry<String, long[]> opEntry : entry.getValue().entrySet()) {
          if (!"ERROR".equals(opEntry.getKey())) {
            grandTotal += opEntry.getValue()[0];
            grandFailed += opEntry.getValue()[1];
          }
        }
        continue;
      }

      String host = shortenHost(entry.getKey());
      Map<String, long[]> stats = entry.getValue();

      if (stats.containsKey("ERROR")) {
        lines.add(host + ":");
        lines.add("  [unavailable]");
        hostsDisplayed++;
        continue;
      }

      lines.add(host + ":");
      for (String op : allOps) {
        long[] counts = stats.get(op);
        if (counts != null) {
          long ok = counts[0] - counts[1];
          long fail = counts[1];
          grandTotal += counts[0];
          grandFailed += counts[1];
          if (fail > 0) {
            lines.add(String.format("  %s %d ok /%d F", op, ok, fail));
          } else {
            lines.add(String.format("  %s %d ok", op, ok));
          }
        }
      }
      hostsDisplayed++;
    }

    if (hostsSkipped > 0) {
      lines.add(String.format("...+%d host(s)", hostsSkipped));
    }

    lines.add("");
    double failPct = grandTotal > 0 ? (100.0 * grandFailed / grandTotal) : 0;
    lines.add(String.format("Total: %d ok /%d F", grandTotal - grandFailed, grandFailed));
    lines.add(String.format("Errors: %.1f%%", failPct));

    return lines;
  }

  private void render() {
    if (!running || screen == null) {
      return;
    }

    try {
      // Poll for keyboard input — Lanterna handles Ctrl+C internally
      // (default CtrlCBehaviour.CTRL_C_KILLS_APPLICATION calls System.exit)
      screen.pollInput();

      TerminalSize size = screen.doResizeIfNecessary();
      if (size == null) {
        size = screen.getTerminalSize();
      }

      int termHeight = size.getRows();
      int termWidth = size.getColumns();
      int rightPaneWidth = Math.max(25, Math.min(40, termWidth * 35 / 100));
      int dividerCol = termWidth - rightPaneWidth - 1;
      int leftWidth = dividerCol;

      // Get log lines snapshot
      List<String> visibleLogs;
      synchronized (logLines) {
        int start = Math.max(0, logLines.size() - termHeight);
        visibleLogs = new ArrayList<>(logLines.subList(start, logLines.size()));
      }

      // Get status lines snapshot
      List<String> currentStatus = statusLines;

      screen.clear();
      TextGraphics tg = screen.newTextGraphics();

      // Draw left pane (logs)
      tg.setForegroundColor(TextColor.ANSI.DEFAULT);
      for (int row = 0; row < termHeight; row++) {
        if (row < visibleLogs.size()) {
          String line = visibleLogs.get(row);
          if (line.length() > leftWidth) {
            line = line.substring(0, leftWidth);
          }
          tg.putString(0, row, line);
        }
      }

      // Draw vertical divider
      tg.setForegroundColor(TextColor.ANSI.WHITE);
      for (int row = 0; row < termHeight; row++) {
        tg.setCharacter(dividerCol, row, '\u2502'); // │
      }

      // Draw right pane (agent status)
      int rightStart = dividerCol + 1;
      for (int row = 0; row < termHeight && row < currentStatus.size(); row++) {
        String line = currentStatus.get(row);
        if (line.length() > rightPaneWidth) {
          line = line.substring(0, rightPaneWidth);
        }

        // Highlight header
        if (row == 0) {
          tg.setForegroundColor(TextColor.ANSI.CYAN);
        } else if (line.contains(" F") || line.contains("FAIL")) {
          tg.setForegroundColor(TextColor.ANSI.RED);
        } else {
          tg.setForegroundColor(TextColor.ANSI.GREEN);
        }

        tg.putString(rightStart + 1, row, line);
      }

      screen.refresh(Screen.RefreshType.DELTA);
    } catch (IOException e) {
      // swallow to keep rendering alive
    } catch (Exception e) {
      // swallow unexpected errors
    }
  }

  private static String formatDuration(Duration d) {
    long hours = d.toHours();
    long minutes = d.toMinutes() % 60;
    long seconds = d.getSeconds() % 60;
    return String.format("%02d:%02d:%02d", hours, minutes, seconds);
  }

  private static String shortenHost(String host) {
    int dot = host.indexOf('.');
    return dot > 0 ? host.substring(0, dot) : host;
  }

  /**
   * OutputStream that captures written bytes, splits on newlines,
   * and stores complete lines in a thread-safe ring buffer.
   */
  static class LogCapturingStream extends OutputStream {
    private final LinkedList<String> lines;
    private final int maxLines;
    private byte[] buffer = new byte[4096];
    private int pos = 0;

    LogCapturingStream(LinkedList<String> lines, int maxLines) {
      this.lines = lines;
      this.maxLines = maxLines;
    }

    @Override
    public synchronized void write(int b) {
      if (b == '\n') {
        addLine(new String(buffer, 0, pos));
        pos = 0;
      } else if (b != '\r') {
        if (pos >= buffer.length) {
          buffer = Arrays.copyOf(buffer, buffer.length * 2);
        }
        buffer[pos++] = (byte) b;
      }
    }

    @Override
    public synchronized void write(byte[] buf, int off, int len) {
      for (int i = off; i < off + len; i++) {
        write(buf[i]);
      }
    }

    @Override
    public void flush() {
      // lines are added on \n
    }

    private void addLine(String line) {
      synchronized (lines) {
        lines.add(line);
        while (lines.size() > maxLines) {
          lines.removeFirst();
        }
      }
    }
  }
}
