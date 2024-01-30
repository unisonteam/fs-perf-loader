/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.perf.filetransfer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileTransferRemote {
  private static final Map<String, Long> OFFSET = new ConcurrentHashMap<>();

  public static byte[] nextChunk(String path) {
    if (path == null) {
      path = System.getenv("LOG_FILE");
    }

    long previousOffset = OFFSET.getOrDefault(path, 0L);

    long newLength = new File(path).length();

    if (newLength < previousOffset) {
      previousOffset = 0;
    }

    try (RandomAccessFile r = new RandomAccessFile(path, "r")) {
      r.seek(previousOffset);
      byte[] ret = new byte[(int) (newLength - previousOffset)];
      r.read(ret);
      OFFSET.put(path, newLength);
      return ret;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}