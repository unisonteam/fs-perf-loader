package team.unison.perf.cleaner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import team.unison.remote.GenericWorkerBuilder;

public class FsCleanerBuilder {
  private String name;
  private Map<String, String> conf;
  private GenericWorkerBuilder genericWorkerBuilder;
  private List<String> paths = new ArrayList<>();
  private int threads = 8;
  private Collection<String> suffixes = Collections.singleton("");

  public FsCleanerBuilder name(String name) {
    this.name = name;
    return this;
  }

  public FsCleanerBuilder conf(Map<String, String> conf) {
    this.conf = (conf == null) ? null : new HashMap<>(conf);

    return this;
  }

  public FsCleanerBuilder genericWorkerBuilder(GenericWorkerBuilder genericWorkerBuilder) {
    this.genericWorkerBuilder = genericWorkerBuilder;
    return this;
  }

  public FsCleanerBuilder paths(List<String> paths) {
    this.paths = paths;
    return this;
  }

  public FsCleanerBuilder threads(int threads) {
    this.threads = threads;
    return this;
  }

  public FsCleanerBuilder suffixes(Collection<String> suffixes) {
    this.suffixes = suffixes;
    return this;
  }

  public FsCleaner createFsCleaner() {
    return new FsCleaner(name, conf, genericWorkerBuilder, paths, threads, suffixes);
  }
}