package team.unison.perf;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PerfLoaderUtils {
  private static final Properties GLOBAL_PROPERTIES = new Properties();

  private PerfLoaderUtils() {
  }

  /**
   * Parses list of strings from CSV string. Substrings may be either regular strings or in formats
   * - count:format - in this case 'format' string is applied to 0..count-1. if format string has several format positions, each
   * position will have 'count' variants (total count^positions strings generated)
   * - prefix{start..end}suffix - this is expanded to prefix+start+suffix, prefix+(start+1)+suffix, ..., prefix+end+suffix
   */
  public static List<String> parseTemplate(String templateString) {
    String[] partsRaw = templateString.split(",");
    List<String> partsParsed = new ArrayList<>();

    for (String s : partsRaw) {
      partsParsed.addAll(parseTemplateSingle(s));
    }

    return partsParsed;
  }

  private static List<String> parseTemplateSingle(String template) {
    String numberFormatRegex = "^(\\d+):(.*%.*)$";
    Pattern patternFormat = Pattern.compile(numberFormatRegex);
    String intervalRegex = "^(.*)\\{(\\d+)\\.\\.(\\d+)}(.*)$";
    Pattern patternInterval = Pattern.compile(intervalRegex);

    Matcher matcherFormat = patternFormat.matcher(template);
    List<String> ret = new ArrayList<>();
    if (matcherFormat.find()) {
      int countInPosition = Integer.parseInt(matcherFormat.group(1));
      String format = matcherFormat.group(2);
      int positionsCount = format.split("%").length - 1;
      int totalPaths = (int) Math.pow(countInPosition, positionsCount);

      for (int i = 0; i < totalPaths; i++) {
        ret.add(String.format(format, pathNumbers(countInPosition, positionsCount, i).toArray()));
      }
    } else {
      Matcher matcherInterval = patternInterval.matcher(template);

      if (matcherInterval.find()) {
        String prefix = matcherInterval.group(1);
        int start = Integer.parseInt(matcherInterval.group(2));
        int end = Integer.parseInt(matcherInterval.group(3));
        String suffix = matcherInterval.group(4);

        for (int i = start; i <= end; i++) {
          ret.add(prefix + i + suffix);
        }
      } else {
        ret.add(template);
      }
    }
    return ret;
  }

  private static List<Object> pathNumbers(int countInPosition, int positionsCount, int itemNoParam) {
    if (positionsCount <= 0) {
      return Collections.EMPTY_LIST;
    }
    List ret = new ArrayList(positionsCount);
    int itemNo = itemNoParam;
    for (int pos = 0; pos < countInPosition; pos++) {
      ret.add(itemNo % countInPosition);
      itemNo /= countInPosition;
    }
    return ret;
  }

  public static Properties getGlobalProperties() {
    return GLOBAL_PROPERTIES;
  }

  public static void setGlobalProperties(Properties globalProperties) {
    globalProperties.forEach((k, v) -> GLOBAL_PROPERTIES.setProperty(k.toString(), v.toString()));
  }

  public static List<Map<String, String>> parseWorkload(String workloadPath) throws IOException {
    if (workloadPath == null || workloadPath.isEmpty()) {
      return new ArrayList<>();
    }

    List<Map<String, String>> ret = new ArrayList<>();
    String workloadJson = new String(Files.readAllBytes(Paths.get(workloadPath)));
    JsonElement root = new JsonParser().parse(workloadJson);

    String workloadPropertyName = root.getAsJsonObject().has("workload") ? "workload" : "mixedWorkload";

    for (JsonElement jsonElement : root.getAsJsonObject().get(workloadPropertyName).getAsJsonArray()) {
      Map<String, Object> map = new HashMap();
      map = new Gson().fromJson(jsonElement, map.getClass());
      Map<String, String> stringMap = new HashMap();
      map.entrySet().forEach(e -> stringMap.put(e.getKey(), e.getValue().toString()));
      ret.add(stringMap);
    }

    return ret;
  }

  public static void printStatistics(String header, String operation, String endpoint, int concurrency, long averageObjectSize,
                                     long[] results, Duration duration) {
    long durationInSeconds = duration.toMillis() / 1000;
    Arrays.sort(results);
    System.out.printf("%s%n", header);
    System.out.printf("            --- Total Results ---%n");
    System.out.printf("Operation: %s%n", operation);
    if (endpoint != null) {
      System.out.printf("Endpoint: %s%n", endpoint);
    }
    System.out.printf("Concurrency: %d%n", concurrency);
    System.out.printf("Total number of requests: %d%n", results.length);
    System.out.printf("Failed requests: %d%n", Arrays.stream(results).filter(l -> l <= 0).count());
    System.out.printf("Total elapsed time: %fs%n", ((double) Arrays.stream(results).sum()) / 1_000_000_000);
    System.out.printf("Duration: %ds%n", durationInSeconds);
    if (durationInSeconds != 0) {
      System.out.printf("Requests/sec: %d%n", results.length / durationInSeconds);
    }
    System.out.printf("Average request time: %fms%n", (Arrays.stream(results).average().orElse(0)) / 1_000_000);
    System.out.printf("Minimum request time: %.2fms%n", ((double) Arrays.stream(results).filter(l -> l > 0).min().orElse(0)) / 1_000_000);
    System.out.printf("Maximum request time: %.2fms%n", ((double) Arrays.stream(results).max().orElse(0)) / 1_000_000);

    if (averageObjectSize != 0) {
      long totalObjectSize = averageObjectSize * results.length;
      System.out.printf("Average Object Size: %d%n", averageObjectSize);
      System.out.printf("Total Object Size: %d%n", totalObjectSize);
      System.out.printf(" - human readable: %s%n", FileUtils.byteCountToDisplaySize(totalObjectSize));
      if (durationInSeconds != 0) {
        System.out.printf("Speed (bytes/sec): %d%n", totalObjectSize / durationInSeconds);

        String humanReadableTransferSpeed = FileUtils.byteCountToDisplaySize(totalObjectSize / durationInSeconds);
        System.out.printf(" - human readable: %s%s%n", humanReadableTransferSpeed,
                humanReadableTransferSpeed.endsWith("B") ? "ps" : " per second");
      }
    }

    System.out.printf("Response Time Percentiles%n");

    long[] parr = new long[]{500,
            750,
            900,
            950,
            990,
            999};
    for (long l : parr) {
      System.out.printf("  %.1f : %.2f ms %n", (float) l / 10, getPercentile(results, ((double) l) / 1000) / 1_000_000);
    }
  }

  private static double getPercentile(long[] arr, double percentile) {
    int index = (int) Math.ceil(percentile * (double) arr.length);
    return arr[index - 1];
  }
}