/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.fswrapper;

import team.unison.perf.PerfLoaderUtils;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FsWrapperFactory {
  private static final Map<Map<String, String>, FsWrapper> CACHE = new ConcurrentHashMap<>();

  private FsWrapperFactory() {
  }

  public static List<FsWrapper> get(@Nonnull Map<String, String> conf) {
    String root = conf.get("root");
    if (root == null) {
      throw new IllegalArgumentException("Missing root configuration");
    }

    if (conf.containsKey("s3.uri")) {
      List<String> s3Uris = PerfLoaderUtils.parseTemplate(conf.get("s3.uri"));
      List<FsWrapper> ret = new ArrayList<>();
      for (String s3uri : s3Uris) {
        conf.put("s3.uri", s3uri);
        ret.add(CACHE.computeIfAbsent(conf, map -> newInstance(root, map)));
      }
      return ret;
    } else {
      return Collections.singletonList(newInstance(root, conf));
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
