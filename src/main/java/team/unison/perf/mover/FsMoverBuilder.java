package team.unison.perf.mover;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import team.unison.remote.GenericWorkerBuilder;

public class FsMoverBuilder {
  private String name;
  private GenericWorkerBuilder genericWorkerBuilder;
  private String source;
  private String target;
  private int threads = 8;
  private Collection<String> suffixes = Collections.singleton("");
  private Duration delay = Duration.ZERO;
  private Duration period;
  private Duration timeout = Duration.ofHours(1);
  private boolean removeSuffix;

  public FsMoverBuilder name(String name) {
    this.name = name;
    return this;
  }

  public FsMoverBuilder genericWorkerBuilder(GenericWorkerBuilder genericWorkerBuilder) {
    this.genericWorkerBuilder = genericWorkerBuilder;
    return this;
  }

  public FsMoverBuilder source(String source) {
    this.source = source;
    return this;
  }

  public FsMoverBuilder target(String target) {
    this.target = target;
    return this;
  }

  public FsMoverBuilder threads(int threads) {
    this.threads = threads;
    return this;
  }

  public FsMoverBuilder suffixes(Collection<String> suffixes) {
    this.suffixes = suffixes;
    return this;
  }

  public FsMoverBuilder delay(Duration delay) {
    this.delay = delay;
    return this;
  }

  public FsMoverBuilder period(Duration period) {
    this.period = period;
    return this;
  }

  public FsMoverBuilder timeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public FsMoverBuilder removeSuffix(boolean removeSuffix) {
    this.removeSuffix = removeSuffix;
    return this;
  }

  public FsMover createFsMover() {
    return new FsMover(name, genericWorkerBuilder, source, target, threads, suffixes, delay, period, timeout, removeSuffix);
  }
}