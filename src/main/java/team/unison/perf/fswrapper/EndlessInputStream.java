package team.unison.perf.fswrapper;

import java.io.InputStream;

class EndlessInputStream extends InputStream {
  final byte[] barr;

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