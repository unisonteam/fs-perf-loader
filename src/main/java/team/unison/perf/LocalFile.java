package team.unison.perf;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;

public class LocalFile {
  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);

  private final String prefix;
  private final String suffix;
  private final boolean gzip;
  private final boolean append;
  private final boolean single;

  public LocalFile(String prefix, String suffix, boolean gzip, boolean append, boolean single) {
    this.prefix = prefix;
    this.suffix = suffix;
    this.gzip = gzip;
    this.append = append;
    this.single = single;

    init();
  }

  void init() {
    if (!new File(prefix).getParentFile().exists() && !new File(prefix).getParentFile().mkdirs()) {
      throw new IllegalArgumentException("Can't create the parent directory of " + prefix);
    }
    rotateFiles();
  }

  public void write(byte[] contents) {
    try (OutputStream os = getOutputStream()) {
      os.write(contents);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void write(String contents, String separator) {
    try (OutputStream os = getOutputStream()) {
      if (single) {
        os.write(separator.getBytes(StandardCharsets.UTF_8));
      }
      os.write(contents.getBytes(StandardCharsets.UTF_8));
      if (single && !contents.endsWith("\n")) {
        os.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private OutputStream getOutputStream() throws IOException {
    String fileName = getFileName();

    OutputStream os;

    if (gzip) {
      os = new GZIPOutputStream(new FileOutputStream(fileName, true));
    } else {
      os = new FileOutputStream(fileName, true);
    }

    return new BufferedOutputStream(os);
  }

  private void rotateFiles() {
    if (append) {
      return;
    }
    File outputFile = new File(getFileName());
    if (outputFile.exists()) {
      File oldFile = new File(getFileName(suffix + "-" + sdf.format(new Date()).replace(" ", "-").replace(":", "-")));
      outputFile.renameTo(oldFile);
    }
  }

  private String getFileName() {
    return getFileName(suffix);
  }

  private String getFileName(String suffix) {
    String fileName = prefix + suffix;
    if (!single) {
      fileName += "-" + sdf.format(new Date()).replace(" ", "-").replace(":", "-");
    }
    if (gzip) {
      fileName += ".gz";
    }

    return fileName;
  }
}
