/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TextFormatterTest {
  @Test
  void testByteCountToDisplaySize() {
    assertEquals("1.72 TB", TextFormatter.byteCountToDisplaySize(1887436800000L));
    assertEquals("5 bytes", TextFormatter.byteCountToDisplaySize(5));
  }

  @Test
  void testSpeedDisplay() {
    assertEquals("20.6 Gibit/s", TextFormatter.bytesPerSecondToSpeed(2771566519L));
    assertEquals("8 bit/s", TextFormatter.bytesPerSecondToSpeed(1));
  }
}