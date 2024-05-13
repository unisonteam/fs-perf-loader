package team.unison.perf.fswrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class LocalFs {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFs.class);
  private static final byte[] EMPTY_BYTES = new byte[0];
  private final Path root;

  public LocalFs(@Nonnull String rootPath) {
    root = Paths.get(rootPath);
    LOGGER.info("Create local fs root for consistency_check: {}", root.toAbsolutePath());
    root.toFile().mkdirs();
  }

  public LocalFs() {
    this(System.getProperty("user.dir") + File.separator + "consistency_check_fs_root");
  }

  public void writeFile(@Nonnull String bucket, @Nonnull String path, @Nonnull byte[] data) {
    File localFile = mapToLocalFile(bucket, path);
    createLocalFile(localFile);
    try (FileOutputStream os = new FileOutputStream(localFile)) {
      os.write(data);
    } catch (IOException e) {
      LOGGER.error("Can't write md5 hash to file: {}", localFile);
    }
  }

  public @Nonnull byte[] readFile(@Nonnull String bucket, @Nonnull String path) {
    Path localFilePath = mapToLocalFile(bucket, path).toPath();
    try {
      return Files.readAllBytes(localFilePath);
    } catch (IOException e) {
      LOGGER.error("Can't read file: {}", e.getMessage());
    }
    return EMPTY_BYTES;
  }

  public void copyFile(@Nonnull String sourceBucket, @Nonnull String destinationBucket, @Nonnull String path) {
    try {
      Path src = mapToLocalFile(sourceBucket, path).toPath();
      Path dst = mapToLocalFile(destinationBucket, path).toPath();
      Files.copy(src, dst);
    } catch (IOException e) {
      LOGGER.error("Can't copy file: {}", e.getMessage());
    }
  }

  public void deleteFile(@Nonnull String bucket, @Nonnull String path) {
    File localFile = mapToLocalFile(bucket, path);
    if (!localFile.delete()) {
      LOGGER.error("Can't delete file: {}", localFile);
    }
  }

  public void deleteAllFilesRecursively() {
    File rootFile = root.toFile();
    try (Stream<Path> walk = Files.walk(root)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(file -> {
       if (!rootFile.equals(file)) {
         file.delete();
       }
      });
    } catch (IOException e) {
      LOGGER.error("Can't delete files: {}", e.getMessage());
    }
    rootFile.delete();
  }

  private static void createLocalFile(@Nonnull File localFile){
    try {
      File parentFile = localFile.getParentFile();
      parentFile.mkdirs();
      if (!localFile.createNewFile()) {
        LOGGER.error("File already exists: {}", localFile);
      }
    } catch (IOException e) {
      LOGGER.error("Can't create local file: {}", e.getMessage());
    }
  }

  private @Nonnull File mapToLocalFile(@Nonnull String bucket, @Nonnull String path) {
    int idx = path.indexOf(bucket);
    String fileName = (idx != -1) ? path.substring(idx) : path;
    return root.resolve(fileName).toFile();
  }
}
