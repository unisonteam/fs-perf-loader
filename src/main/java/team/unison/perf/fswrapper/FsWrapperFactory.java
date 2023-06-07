package team.unison.perf.fswrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FsWrapperFactory {
  private static final Map<Map<String, String>, FsWrapper> CACHE = new ConcurrentHashMap<>();

  private FsWrapperFactory() {
  }

  public static FsWrapper get(String path, Map<String, String> conf) {
    Map<String, String> nonNullConf = new HashMap<>();
    if (conf != null) {
      nonNullConf.putAll(conf);
    }
    String root = path.startsWith("/") ? "/" : path.replaceAll("(//.*?)/.*$", "$1");
    nonNullConf.put("root", root);

    return CACHE.computeIfAbsent(nonNullConf, map -> newInstance(root, map));
  }

  private static FsWrapper newInstance(String path, Map<String, String> conf) {
    if (conf.containsKey("s3.bucket")) {
      return new S3FsWrapper(conf);
    } else {
      return new HdfsFsWrapper(path, conf);
    }
  }
}
