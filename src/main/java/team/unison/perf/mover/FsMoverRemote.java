package team.unison.perf.mover;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import team.unison.remote.WorkerException;

public class FsMoverRemote {
  private static final Logger log = LoggerFactory.getLogger(FsMoverRemote.class);

  public static String apply(String sourceString, List<String> arg) throws IOException {
    Path source = new Path(sourceString);
    FileSystem fs = FileSystem.get(new Configuration());

    log.info("source: {}, arg: {}", source, arg);
    if (!fs.exists(source)) {
      return null;
    }

    String target = arg.get(0);
    int threads = Integer.parseInt(arg.get(1));
    boolean removeSuffix = Boolean.parseBoolean(arg.get(2));
    List<String> suffixes = arg.subList(3, arg.size());

    ExecutorService executorService = Executors.newFixedThreadPool(threads);

    RemoteIterator<LocatedFileStatus> iter = fs.listFiles(source, true);

    try {
      while (iter.hasNext()) {
        LocatedFileStatus lfs = iter.next();
        Path sourceFilePath = lfs.getPath();
        Optional<String> fileSuffix = suffixes.stream().filter(s -> sourceFilePath.toString().endsWith(s)).findAny();

        if (!fileSuffix.isPresent()) {
          continue;
        }

        executorService.submit(() -> renameFile(fs, sourceFilePath, source.toString(), target, fileSuffix.get(), removeSuffix));
      }
    } catch (Exception e) {
      log.warn("Error in mover", e);
    } finally {
      executorService.shutdown();
      try {
        executorService.awaitTermination(1, TimeUnit.HOURS);
      } catch (InterruptedException e) {
        throw WorkerException.wrap(e);
      }
    }
    return null;
  }

  private static void renameFile(FileSystem fs, Path sourceFilePath, String source, String target, String fileSuffix,
                                 boolean removeSuffix) {
    if ("/dev/null".equals(target)) {
      try {
        fs.delete(sourceFilePath, true);
      } catch (IOException e) {
        log.warn("Error removing file {}", sourceFilePath);
      }
      return;
    }
    String sourceFile = sourceFilePath.toString();
    // the code with Pattern.compile is taken from java.lang.String.replace() with changed 'replaceAll' to 'replaceFirst'
    String targetFile = Pattern.compile(source, Pattern.LITERAL).matcher(sourceFile).replaceFirst(Matcher.quoteReplacement(target));

    if (removeSuffix) {
      targetFile = targetFile.substring(0, targetFile.length() - fileSuffix.length());
    }

    Path targetFilePath = new Path(targetFile);
    try {
      fs.mkdirs(targetFilePath.getParent());
      fs.rename(sourceFilePath, targetFilePath);
    } catch (IOException e) {
      log.warn("Error renaming file {} to {}", sourceFilePath, targetFilePath, e);
    }
  }
}
