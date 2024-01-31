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
      fs = FileSystem.get(new URI(path), conf);
      log.info("Created HDFS wrapper for the path {} with following configuration: ", path);
      fs.getConf().forEach(e -> log.info("{} : {}", e.getKey(), e.getValue()));

      if (path.startsWith("ofs://")) {
        log.info("Ozone configuration for path {}: ", path);
        OzoneConfiguration ozConf = OzoneConfiguration.of(fs.getConf());
        ozConf.forEach(e -> log.info("{} : {}", e.getKey(), e.getValue()));

        log.info("Value of configuration property for Ozone transport: {}",
                ozConf.get(OZONE_OM_TRANSPORT_CLASS, OZONE_OM_TRANSPORT_CLASS_DEFAULT));

        if (ozConf
                .get(OZONE_OM_TRANSPORT_CLASS,
                        OZONE_OM_TRANSPORT_CLASS_DEFAULT) !=
                OZONE_OM_TRANSPORT_CLASS_DEFAULT) {
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
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    }
    return true;
  }

  @Override
  public boolean createSnapshot(String path, String snapshotName) throws IOException {
    fs.createSnapshot(new Path(path), snapshotName);
    return true;
  }

  @Override
  public boolean deleteSnapshot(String path, String snapshotName) {
    try {
      fs.deleteSnapshot(new Path(path), snapshotName);
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    }
    return true;
  }
}