import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
  public static String getFilePath(String type, String filename) {
    String ext = MicroDB.tableFormat;
    String folder = MicroDB.userDataFolder;
    if (type.equals("master")) folder = MicroDB.systemDataFolder;
    else if (type.equals("index")) {
      folder = MicroDB.indicesFolder;
    }

    Path path = FileSystems.getDefault().getPath(MicroDB.tableLocation, folder, filename + ext);
    return path.toString();
  }

  public static String getOSPath(String[] tokens) {
    StringBuilder str = new StringBuilder();
    for (String token : tokens) {
      str.append(token);
      str.append(File.separator);
    }
    return str.toString();
  }

  public static String repeat(String s, int num) {
    String a = "";
    for (int i = 0; i < num; i++) {
      a += s;
    }
    return a;
  }

  public static String format(String string, int length) {
    return String.format("%1$" + length + "s", string);
  }

  public static String getIndexName(String tableName, String[] columnList) {
    String indexName = "unique_" + tableName + "_";
    indexName += String.join("_", columnList) + "_idx";
    return indexName;
  }

  public static LinkedHashMap<String, ArrayList<String>> buildInsertRecord(List<String> values) {
    LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
    List<String> colNames = new ArrayList<String>(Arrays.asList("rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable", "default", "is_unique"));
    List<String> dataTypes = new ArrayList<String>(Arrays.asList("int", "text", "text", "text", "tinyint", "text", "text", "text"));
    if (values.size() != colNames.size()) return null;
    for (int i = 0; i < values.size(); i++) {
      token.put(colNames.get(i), new ArrayList<String>(Arrays.asList(dataTypes.get(i), values.get(i))));
    }
    return token;
  }

  /**
   * Validations - Type checkings
   */
  public static boolean checkValue(String type, String value) {
    switch (type.toLowerCase()) {
      case "text":
        if ((value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'')
            || (value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"'))
          return true;
        break;
      case "tinyint":
        if (Integer.parseInt(value) >= Byte.MIN_VALUE && Integer.parseInt(value) <= Byte.MAX_VALUE)
          return true;
        break;
      case "smallint":
        if (Integer.parseInt(value) >= Short.MIN_VALUE && Integer.parseInt(value) <= Short.MAX_VALUE)
          return true;
        break;
      case "int":
        if (Integer.parseInt(value) >= Integer.MIN_VALUE && Integer.parseInt(value) <= Integer.MAX_VALUE)
          return true;
        break;
      case "bigint":
        if (Long.parseLong(value) >= Long.MIN_VALUE && Long.parseLong(value) <= Long.MAX_VALUE)
          return true;
        break;
      case "real":
        if (Float.parseFloat(value) >= Float.MIN_VALUE && Float.parseFloat(value) <= Float.MAX_VALUE)
          return true;
        break;
      case "double":
        if (Double.parseDouble(value) >= Double.MIN_VALUE && Double.parseDouble(value) <= Double.MAX_VALUE)
          return true;
        break;
      case "datetime":
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        try {
          Date date = df.parse(value);
        } catch (ParseException e) {
          return false;
        }
        return true;
      case "date":
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd");
        try {
          Date date = d.parse(value);
        } catch (ParseException e) {
          return false;
        }
        return true;
      default:
        return false;
    }
    return false;
  }
}
