/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.perf.fswrapper;

import java.io.InputStream;

class EndlessInputStream extends InputStream {
  private final byte[] barr;

  EndlessInputStream(byte[] barr) {
    this.barr = barr;
  }

  @Override
  public int read(byte[] b, int off, int len) {
    for (int pos = 0; pos < len; pos += barr.length) {
      System.arraycopy(barr, 0, b, off + pos, pos + barr.length < len ? barr.length : len - pos);
    }
    return len;
  }

  @Override
  public int read() {
    return barr[0];
  }
}