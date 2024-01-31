/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Custom Implementation of FileUtils.byteCountToDisplaySize to fix rounding bug
 */
public class TextFormatter {
  // code is taken from https://issues.apache.org/jira/browse/IO-373 (and modified to display speed as well)
  // constants taken from org.apache.commons.io.TextFormatter
  public static final BigInteger ONE_KB_BI = BigInteger.valueOf(1024);
  public static final BigInteger ONE_MB_BI = ONE_KB_BI.multiply(ONE_KB_BI);
  public static final BigInteger ONE_GB_BI = ONE_KB_BI.multiply(ONE_MB_BI);
  public static final BigInteger ONE_TB_BI = ONE_KB_BI.multiply(ONE_GB_BI);
  public static final BigInteger ONE_PB_BI = ONE_KB_BI.multiply(ONE_TB_BI);

  private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;

  enum DataSize {
    PETABYTE("PB", ONE_PB_BI),
    TERABYTE("TB", ONE_TB_BI),
    GIGABYTE("GB", ONE_GB_BI),
    MEGABYTE("MB", ONE_MB_BI),
    KILOBYTE("KB", ONE_KB_BI),
    BYTE("bytes", BigInteger.ONE),
    BIT("bits", BigInteger.ONE);

    private final String unit;
    private final BigInteger byteCount;

    DataSize(String unit, BigInteger byteCount) {
      this.unit = unit;
      this.byteCount = byteCount;
    }
  }

  /**
   * Formats a file's size into a human readable format
   *
   * @param fileSize the file's size as BigInteger
   * @return the size as human readable string
   */
  public static String byteCountToDisplaySize(final BigInteger fileSize) {
    String unit = DataSize.BYTE.unit;
    BigDecimal fileSizeInUnit = BigDecimal.ZERO;
    String val;

    for (DataSize fs : DataSize.values()) {
      BigDecimal size_bd = new BigDecimal(fileSize);
      fileSizeInUnit = size_bd.divide(new BigDecimal(fs.byteCount), 5, ROUNDING_MODE);
      if (fileSizeInUnit.compareTo(BigDecimal.ONE) >= 0) {
        unit = fs.unit;
        break;
      }
    }

    // always round so that at least 3 numerics are displayed (###, ##.#, #.##)
    if (fileSizeInUnit.divide(BigDecimal.valueOf(100.0), BigDecimal.ROUND_DOWN).compareTo(BigDecimal.ONE) >= 0) {
      val = fileSizeInUnit.setScale(0, ROUNDING_MODE).toString();
    } else if (fileSizeInUnit.divide(BigDecimal.valueOf(10.0), BigDecimal.ROUND_DOWN).compareTo(BigDecimal.ONE) >= 0) {
      val = fileSizeInUnit.setScale(1, ROUNDING_MODE).toString();
    } else {
      val = fileSizeInUnit.setScale(2, ROUNDING_MODE).toString();
    }

    // trim zeros at the end
    if (val.endsWith(".00")) {
      val = val.substring(0, val.length() - 3);
    } else if (val.endsWith(".0")) {
      val = val.substring(0, val.length() - 2);
    }

    return String.format("%s %s", val, unit);
  }

  /**
   * Formats a file's size into a human readable format
   *
   * @param fileSize the file's size as long
   * @return the size as human readable string
   */
  public static String byteCountToDisplaySize(long fileSize) {
    return byteCountToDisplaySize(BigInteger.valueOf(fileSize));
  }

  /**
   * Formats a speed in bytes per second to human readable format
   */
  public static String bytesPerSecondToSpeed(long bytesPerSecond) {
    return byteCountToDisplaySize(BigInteger.valueOf(bytesPerSecond * 8))
            .replace("B", "ibit/s")
            .replace("bytes", "bit/s");
  }
}