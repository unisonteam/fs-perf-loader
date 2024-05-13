package team.unison.perf.fswrapper.consistency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.perf.fswrapper.FsWrapper;
import team.unison.perf.fswrapper.LocalFs;
import team.unison.perf.fswrapper.S3FsWrapper;

import javax.annotation.Nonnull;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Proxy class for FSWrapper used for ozoneFS consistency check:
 * 1. Run JAR with `loader.properties.docker` where `fsloader.LOAD.workload=/opt/hadoop/wl.[PUT|GET].json`
 *    - on PUT workload this class writes file content to ozoneFS and md5 file hash to datanode's localFS
 *    - on GET workload this class:
 *        a. Reads md5 hashes from localFS
 *        b. Reads file from ozoneFS and compute its md5 hash
 *        c. Compare md5 hashes from localFS and ozoneFS *
 * 2. On datanodes there is a localFS root `CONSISTENCY_CHECK_PATH`(the same FS-tree as in ozoneFS)
 *
 * To use FsConsistencyCheckerProxy yo uhave to add the following lines in `loader.properties.docker`:
 *   fsloader.LOAD.conf=fsloaderconf
 *   conf.fsloaderconf.consistency.check=true
 *   conf.fsloaderconf.consistency.check.path=/opt/hadoop/consistency_check_fs_root *
* */

public class FsConsistencyCheckerProxy implements FsWrapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(FsConsistencyCheckerProxy.class);
  private static final String CONSISTENCY_CHECK_KEY = "consistency.check";
  private static final String CONSISTENCY_CHECK_PATH_KEY = "consistency.check.path";

  private final FsWrapper fsWrapper;
  private final MessageDigest md5;
  private final LocalFs localFS;

  private FsConsistencyCheckerProxy(@Nonnull LocalFs localFS, @Nonnull FsWrapper wrapper, @Nonnull MessageDigest md5) {
    fsWrapper = wrapper;
    this.md5 = md5;
    this.localFS = localFS;
  }

  public static @Nonnull FsWrapper createOrUseDefault(@Nonnull Map<String, String> conf, @Nonnull FsWrapper fsWrapper) {
    if (!Boolean.parseBoolean(conf.get(CONSISTENCY_CHECK_KEY))) {
      return fsWrapper;
    }
    try {
      LocalFs localFS = new LocalFs(conf.get(CONSISTENCY_CHECK_PATH_KEY));
      return new FsConsistencyCheckerProxy(localFS, fsWrapper, MessageDigest.getInstance("MD5"));
    } catch (NoSuchAlgorithmException e) {
      LOGGER.error("Failed to create FsConsistencyChecker, use standalone fsWrapper:", e);
    }
    return fsWrapper;
  }

  @Override
  public boolean create(@Nonnull String bucket, @Nonnull String path, long length, byte[] data, boolean useTmpFile) {
    int writableSize = (int) length;
    md5.update(data, 0, writableSize);
    byte[] hash = md5.digest();
    localFS.writeFile(bucket, path, hash);
    return fsWrapper.create(bucket, path, length, data, useTmpFile);
  }

  @Override
  public boolean copy(@Nonnull String sourceBucket, @Nonnull String destinationBucket, @Nonnull String path) {
    if (fsWrapper instanceof S3FsWrapper) {
      localFS.copyFile(sourceBucket, destinationBucket, path);
    } else {
      LOGGER.error("Not applicable for HDFSFsWrapper!");
    }
    return fsWrapper.copy(sourceBucket, destinationBucket, path);
  }

  @Override
  public boolean get(@Nonnull String bucket, @Nonnull String path) {
    if (!fsWrapper.get(bucket, path)) {
      return false;
    }
    byte[] bytes = fsWrapper.readBytes(bucket, path);
    byte[] actualHash = md5.digest(bytes);
    byte[] expectedHash = localFS.readFile(bucket, path);
    return Arrays.equals(expectedHash, actualHash);
  }

  @Override
  public boolean head(@Nonnull String bucket, @Nonnull String path) {
    return fsWrapper.head(bucket, path);
  }

  @Override
  public boolean delete(@Nonnull String bucket, @Nonnull String path) {
    localFS.deleteFile(bucket, path);
    return fsWrapper.delete(bucket, path);
  }

  @Override
  public @Nonnull List<String> list(@Nonnull String bucket, @Nonnull String path) {
    return fsWrapper.list(bucket, path);
  }

  @Override
  public boolean allowSnapshot(@Nonnull String path) {
    return fsWrapper.allowSnapshot(path);
  }

  @Override
  public boolean createSnapshot(@Nonnull String path, @Nonnull String snapshotName) {
    return fsWrapper.createSnapshot(path, snapshotName);
  }

  @Override
  public boolean renameSnapshot(@Nonnull String path, @Nonnull String snapshotOldName, @Nonnull String snapshotNewName) {
    return fsWrapper.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }

  @Override
  public boolean deleteSnapshot(@Nonnull String path, @Nonnull String snapshotName) {
    return fsWrapper.deleteSnapshot(path, snapshotName);
  }

  @Override
  public @Nonnull byte[] readBytes(String bucket, String path) {
    return fsWrapper.readBytes(bucket, path);
  }
}
