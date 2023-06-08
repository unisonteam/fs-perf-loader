package team.unison.perf.fswrapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.remote.WorkerException;

class HdfsFsWrapper implements FsWrapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFsWrapper.class);
  private static final byte[] DEVNULL = new byte[128 * 1024 * 1024];

  final FileSystem fs;

  HdfsFsWrapper(String path, Map<String, String> properties) {
    Configuration conf = new Configuration();
    if (properties != null) {
      properties.forEach(conf::set);
    }
    try {
      fs = FileSystem.get(new URI(path), conf);
    } catch (IOException | URISyntaxException e) {
      throw WorkerException.wrap(e);
    }
  }

  @Override
  public boolean create(String bucket, String path, long length, byte[] data, boolean useTmpFile) {
    final Path tmpPath = useTmpFile ? new Path(path + "._COPYING_") : new Path(path);
    try {
      try (FSDataOutputStream fdos = fs.create(tmpPath)) {
        while (fdos.getPos() < length) {
          int toWrite = (fdos.getPos() + data.length < length) ? data.length : (int) (length - fdos.getPos());
          fdos.write(data, 0, toWrite);
        }
      }
      if (useTmpFile) {
        boolean result = fs.rename(tmpPath, new Path(path));
        if (!result) {
          fs.delete(tmpPath, true);
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Can't create file {}", path, e);
      return false;
    }
    return true;
  }

  @Override
  public boolean copy(String s, String bucket, String path) {
    throw new IllegalArgumentException("Not applicable for HDFS");
  }

  @Override
  public boolean get(String bucket, String path) {
    try (FSDataInputStream fis = fs.open(new Path(path))) {
      while (fis.read(DEVNULL) >= 0) {
        // nop
      }
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    }
    return true;
  }

  @Override
  public boolean head(String bucket, String path) {
    throw new IllegalArgumentException("Not applicable for HDFS");
  }

  @Override
  public boolean delete(String bucket, String path) {
    try {
      fs.delete(new Path(path), true);
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    }
    return true;
  }
}
