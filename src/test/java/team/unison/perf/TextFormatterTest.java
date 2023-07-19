package team.unison.perf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

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