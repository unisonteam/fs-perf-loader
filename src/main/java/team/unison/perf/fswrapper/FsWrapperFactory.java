/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.fswrapper;

import team.unison.perf.PerfLoaderUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FsWrapperFactory {
  private static final Map<Map<String, String>, FsWrapper> CACHE = new ConcurrentHashMap<>();

  private FsWrapperFactory() {
  }

  public static List<FsWrapper> get(String path, Map<String, String> conf) {
    Map<String, String> nonNullConf = new HashMap<>();
    if (conf != null) {
      nonNullConf.putAll(conf);
    }
    String root = path.startsWith("/") ? "/" : path.replaceAll("(//.*?/).*$", "$1");
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
