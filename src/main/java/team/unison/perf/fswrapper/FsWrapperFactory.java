package team.unison.perf.fswrapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import team.unison.perf.PerfLoaderUtils;

public class FsWrapperFactory {
  private static final Map<Map<String, String>, FsWrapper> CACHE = new ConcurrentHashMap<>();
  private static final Random RANDOM = new Random();

  private FsWrapperFactory() {
  }

  public static List<FsWrapper> get(String path, Map<String, String> conf) {
    Map<String, String> nonNullConf = new HashMap<>();
    if (conf != null) {
      nonNullConf.putAll(conf);
    }
    String root = path.startsWith("/") ? "/" : path.replaceAll("(//.*?)/.*$", "$1");
    nonNullConf.put("root", root);

    if (nonNullConf.containsKey("s3.uri")) {
      List<String> s3Uris = PerfLoaderUtils.parseTemplate(nonNullConf.get("s3.uri"));
      List<FsWrapper> ret = new ArrayList<>();
      for (String s3uri : s3Uris) {
        nonNullConf.put("s3.uri", s3uri);
        ret.add(CACHE.computeIfAbsent(nonNullConf, map -> newInstance(root, map)));
      }
      return ret;
    } else {
      return Collections.singletonList(CACHE.computeIfAbsent(nonNullConf, map -> newInstance(root, map)));
    }
  }

  private static FsWrapper newInstance(String path, Map<String, String> conf) {
    if (conf.containsKey("s3.uri")) {
      return new S3FsWrapper(conf);
    } else {
      return new HdfsFsWrapper(path, conf);
    }
  }
}
