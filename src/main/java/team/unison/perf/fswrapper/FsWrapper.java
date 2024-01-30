/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.perf.fswrapper;

import java.util.List;

public interface FsWrapper {
  boolean create(String bucket, String path, long length, byte[] data, boolean useTmpFile);

  boolean copy(String s, String bucket, String path);

  boolean get(String bucket, String path);

  boolean head(String bucket, String path);

  boolean delete(String bucket, String path);

  List<String> list(String bucket, String path);
}