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
import java.util.concurrent.atomic.AtomicInteger;

public class FsWrapperFactory {
  private static final Map<Map<String, String>, FsWrapper> S3_FS_CACHE = new ConcurrentHashMap<>();
  private static final Map<Thread, FsWrapper> HDFS_FS_WRAPPER_MAP = new ConcurrentHashMap<>();

  private static final AtomicInteger FS_WRAPPER_COUNTER = new AtomicInteger();

  private FsWrapperFactory() {
  }

  public static FsWrapper get(String path, Map<String, String> conf) {
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
        ret.add(S3_FS_CACHE.computeIfAbsent(nonNullConf, S3FsWrapper::new));
      }
      return randomFsWrapper(ret);
    } else {
      return HDFS_FS_WRAPPER_MAP.computeIfAbsent(Thread.currentThread(), k -> new HdfsFsWrapper(root, nonNullConf));
    }
  }

  private static FsWrapper randomFsWrapper(List<FsWrapper> fsWrappers) {
    return fsWrappers.get(FS_WRAPPER_COUNTER.getAndIncrement() % fsWrappers.size());
  }
}
