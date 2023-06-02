package team.unison.perf;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PerfLoaderUtils {
  private static Properties GLOBAL_PROPERTIES = new Properties();

  private PerfLoaderUtils() {
  }

  /**
   * Parses list of strings from CSV string. Substrings may be either regular strings or in formats
   * - count:format - in this case 'format' string is applied to 0..count-1. if format string has several format positions, each
   * position will have 'count' variants (total count^positions strings generated)
   * - prefix{start..end}suffix - this is expanded to prefix+start+suffix, prefix+(start+1)+suffix, ..., prefix+end+suffix
   */
  static List<String> parseTemplate(String templateString) {
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
    for (JsonElement jsonElement : root.getAsJsonObject().get("workload").getAsJsonArray()) {
      Map<String, String> map = new HashMap();
      map = (Map<String, String>) new Gson().fromJson(jsonElement, map.getClass());

      ret.add(map);
    }

    return ret;
  }
}


