package team.unison.perf;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.loader.FsLoaderBatchRemote;

public final class PrometheusUtils {
  private static final Logger log = LoggerFactory.getLogger(FsLoaderBatchRemote.class);

  private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();
  private static final Map<String, Counter> COUNTERS = new ConcurrentHashMap<>();
  private static final Map<String, Histogram> HISTOGRAM_MAP = new ConcurrentHashMap<>();
  private static final String HOST_NAME = System.getProperty("java.rmi.server.hostname"); // set in RemoteMain
  private static final String PROCESS_NAME = "perfloader";
  private static final String JOB_NAME = "perfloaderjob";
  private static final List<Long> HISTOGRAM_BUCKETS = new ArrayList<>(Arrays.asList(10L, 20L, 30L, 40L, 50L,
                                                                                    100L, 200L, 300L, 400L, 500L,
                                                                                    1_000L, 2_000L, 5_000L, 10_000L));
  private static PushGateway PUSH_GATEWAY;
  private static final Map<String, String> INSTANCE_GROUPING_KEY = new HashMap<>();

  private static final int PUSH_PERIOD_SECONDS = 15;
  private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

  private PrometheusUtils() {
  }

  private static final CollectorRegistry COLLECTOR_REGISTRY = new CollectorRegistry();

  public static void record(String command, long objectSize, boolean success, long elapsedMs) {
    if (!INITIALIZED.get()) {
      return;
    }

    try {
      getOperationsCounter(command, success).labels(HOST_NAME, PROCESS_NAME, Long.toString(objectSize)).inc();
      getOperationsHistogram(command, success).labels(HOST_NAME, PROCESS_NAME).observe(elapsedMs);
    } catch (Exception e) {
      log.warn("Exception in recording", e);
    }
  }

  private static Histogram getOperationsHistogram(String command, boolean success) {
    String operationName = String.format("fsloader_%s_%s_operations_latency_ms", success ? "successful" : "failed", command);

    return HISTOGRAM_MAP.computeIfAbsent(operationName, n -> Histogram.build()
        .name(operationName)
        .help("Latency of " + command + " requests.")
        .labelNames("hostname", "processname")
        .buckets(HISTOGRAM_BUCKETS.stream().mapToDouble(l -> (double) l).toArray())
        .register(COLLECTOR_REGISTRY));
  }

  private static Counter getOperationsCounter(String command, boolean success) {
    String operationName = String.format("fsloader_%s_%s_operations", success ? "successful" : "failed", command);

    return COUNTERS.computeIfAbsent(operationName, n -> Counter.build()
        .name(n)
        .labelNames("hostname", "processname", "objectsize")
        .help("Total " + command + " requests.").register(COLLECTOR_REGISTRY));
  }

  public static synchronized void init(Properties properties) {
    String prometheusAddress = properties.getProperty("prometheus.address");

    if (prometheusAddress != null && !INITIALIZED.get()) {
      log.info("Initialize Prometheus - address is " + prometheusAddress);
      INITIALIZED.set(true);

      String histogramBuckets = properties.getProperty("prometheus.buckets");

      if (histogramBuckets != null) {
        List<Long> bucketsList = Arrays.stream(histogramBuckets.split(",")).map(Long::parseLong).collect(Collectors.toList());
        HISTOGRAM_BUCKETS.clear();
        HISTOGRAM_BUCKETS.addAll(bucketsList);
      }

      INSTANCE_GROUPING_KEY.put("instance", HOST_NAME);
      PUSH_GATEWAY = new PushGateway(prometheusAddress);

      EXECUTOR_SERVICE.scheduleAtFixedRate(() -> {
        try {
          if (INITIALIZED.get()) {
            log.info("Push collected data");
            PUSH_GATEWAY.push(COLLECTOR_REGISTRY, JOB_NAME, INSTANCE_GROUPING_KEY);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }, PUSH_PERIOD_SECONDS, PUSH_PERIOD_SECONDS, TimeUnit.SECONDS);
    }
  }

  public static synchronized void clearStatistics(boolean finalCleanup) {
    if (!INITIALIZED.get() || COUNTERS.isEmpty()) {
      return;
    }
    if (finalCleanup) {
      INITIALIZED.set(false);
    }
    try {
      PUSH_GATEWAY.push(COLLECTOR_REGISTRY, JOB_NAME, INSTANCE_GROUPING_KEY);
      COUNTERS.values().forEach(COLLECTOR_REGISTRY::unregister);
      HISTOGRAM_MAP.values().forEach(COLLECTOR_REGISTRY::unregister);
      COUNTERS.clear();
      HISTOGRAM_MAP.clear();
      PUSH_GATEWAY.delete(JOB_NAME, INSTANCE_GROUPING_KEY);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}