package team.unison.perf.fswrapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import team.unison.perf.PerfLoaderUtils;

public class FsWrapperFactory {
  private static final Map<Map<String, String>, FsWrapper> CACHE = new ConcurrentHashMap<>();
  private static final Map<String, AtomicInteger> S3_ENDPOINT_COUNTER = new ConcurrentHashMap<>();
  private static final Random RANDOM = new Random();

  private FsWrapperFactory() {
  }

  public static FsWrapper get(String path, Map<String, String> conf) {
    Map<String, String> nonNullConf = new HashMap<>();
    if (conf != null) {
      nonNullConf.putAll(conf);
    }
    String root = path.startsWith("/") ? "/" : path.replaceAll("(//.*?)/.*$", "$1");
    nonNullConf.put("root", root);

    String s3uri = nonNullConf.get("s3.uri");
    if (s3uri != null) {
      List<String> s3Uris = PerfLoaderUtils.parseTemplate(s3uri);
      if (s3Uris.size() > 1) {
        String balance = nonNullConf.get("s3.balance");
        if ("cycle".equalsIgnoreCase(balance)) {
          AtomicInteger counter = S3_ENDPOINT_COUNTER.computeIfAbsent(s3uri, (s) -> new AtomicInteger());
          nonNullConf.put("s3.uri", s3Uris.get(counter.getAndIncrement() % s3Uris.size()));
        } else {
          nonNullConf.put("s3.uri", s3Uris.get(RANDOM.nextInt(s3Uris.size())));
        }
      }
    }

    return CACHE.computeIfAbsent(nonNullConf, map -> newInstance(root, map));
  }

  private static FsWrapper newInstance(String path, Map<String, String> conf) {
    if (conf.containsKey("s3.uri")) {
      return new S3FsWrapper(conf);
    } else {
      return new HdfsFsWrapper(path, conf);
    }
  }
}
