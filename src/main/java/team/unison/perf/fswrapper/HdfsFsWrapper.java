/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.fswrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.ozone.om.protocolPB.OmTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.remote.WorkerException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_TRANSPORT_CLASS_DEFAULT;

class HdfsFsWrapper implements FsWrapper {
  private static final Logger log = LoggerFactory.getLogger(HdfsFsWrapper.class);
  private static final byte[] DEVNULL = new byte[128 * 1024 * 1024];

  final FileSystem fs;

  HdfsFsWrapper(String path, Map<String, String> properties) {
    Configuration conf = new Configuration();
    if (properties != null) {
      properties.forEach(conf::set);
    }
    try {
      URI uri = new URI(path);
      disableFileSystemCache(uri, conf);
      fs = FileSystem.get(uri, conf);
      log.info("Created HDFS wrapper for the path {} with following configuration: ", path);
      fs.getConf().forEach(e -> log.info("{} : {}", e.getKey(), e.getValue()));

      if (path.startsWith("ofs://")) {
        log.info("Ozone configuration for path {}: ", path);
        OzoneConfiguration ozConf = OzoneConfiguration.of(fs.getConf());
        ozConf.forEach(e -> log.info("{} : {}", e.getKey(), e.getValue()));

        log.info("Value of configuration property for Ozone transport: {}",
                ozConf.get(OZONE_OM_TRANSPORT_CLASS, OZONE_OM_TRANSPORT_CLASS_DEFAULT));

        if (!OZONE_OM_TRANSPORT_CLASS_DEFAULT.equals(ozConf
                .get(OZONE_OM_TRANSPORT_CLASS, OZONE_OM_TRANSPORT_CLASS_DEFAULT))) {
          log.info("Ozone transport non-default branch.");
          ServiceLoader<OmTransportFactory> transportFactoryServiceLoader =
                  ServiceLoader.load(OmTransportFactory.class);
          Iterator<OmTransportFactory> iterator =
                  transportFactoryServiceLoader.iterator();
          if (iterator.hasNext()) {
            log.info("Load transport from service loader, class: {}", iterator.next().getClass().getName());
          } else {
            log.info("Tried to load transport from service loader but nothing was found");
            log.info("Fall back to default ozone transport {}", OZONE_OM_TRANSPORT_CLASS_DEFAULT);
          }
        }
      }
    } catch (IOException | URISyntaxException e) {
      throw WorkerException.wrap(e);
    }
  }

  private static void disableFileSystemCache(@Nonnull URI uri, @Nonnull Configuration conf) throws URISyntaxException {
    String scheme = uri.getScheme();
    if (scheme != null) {
      String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
      conf.setBoolean(disableCacheName, true);
      boolean disableCache = conf.getBoolean(disableCacheName, false);
      log.info("Uri scheme: {}, authority: {}, disableCache: {}", scheme, uri.getAuthority(), disableCache);
    } else {
      log.warn("Can't disable cache for NULL uri scheme!");
    }
  }

  @Override
  public boolean create(String bucket, String path, long length, byte[] writableData, boolean useTmpFile) {
    final Path tmpPath = useTmpFile ? new Path(path + "._COPYING_") : new Path(path);
    try {
      try (FSDataOutputStream fdos = fs.create(tmpPath)) {
        while (fdos.getPos() < length) {
          int toWrite = (fdos.getPos() + writableData.length < length) ? writableData.length : (int) (length - fdos.getPos());
          fdos.write(writableData, 0, toWrite);
        }
      }
      if (useTmpFile) {
        boolean result = fs.rename(tmpPath, new Path(path));
        if (!result) {
          fs.delete(tmpPath, true);
        }
      }
    } catch (IOException e) {
      log.warn("Can't create file {}", path, e);
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
      log.warn("Error getting path: {}", path, e);
      return false;
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
      log.warn("Error deleting path: {} recursively", path, e);
      return false;
    }
    return true;
  }

  @Override
  public List<String> list(String bucket, String path) {
    try {
      List<String> ret = new ArrayList<>();
      RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(path), true);
      while (iter.hasNext()) {
        ret.add(iter.next().getPath().toString());
      }
      return ret;
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    }
  }

  @Override
  public boolean allowSnapshot(String path) {
    try {
      ((DistributedFileSystem) fs).allowSnapshot(new Path(path));
      return true;
    } catch (IOException e) {
      log.warn("Error allowing snapshot for path {}", path, e);
      return false;
    }
  }

  @Override
  public boolean createSnapshot(String path, String snapshotName) {
    try {
      fs.createSnapshot(new Path(path), snapshotName);
      return true;
    } catch (IOException e) {
      log.warn("Error creating snapshot for path {}", path, e);
      return false;
    }
  }

  @Override
  public boolean renameSnapshot(String path, String snapshotOldName, String snapshotNewName) {
    try {
      fs.renameSnapshot(new Path(path), snapshotOldName, snapshotNewName);
      return true;
    } catch (IOException e) {
      log.warn("Error renaming in path {} snapshot {} to {}", path, snapshotOldName, snapshotNewName, e);
      return false;
    }
  }

  @Override
  public boolean deleteSnapshot(String path, String snapshotName) {
    try {
      fs.deleteSnapshot(new Path(path), snapshotName);
      return true;
    } catch (IOException e) {
      log.warn("Error deleting snapshot for path {}", path, e);
      return false;
    }
  }
}