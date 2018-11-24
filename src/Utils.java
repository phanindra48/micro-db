import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class Utils {
  public static String getFilePath(String type, String filename) {
    String ext = MicroDB.tableFormat;
    String folder = MicroDB.userData;
    if (type.equals("master")) folder = ""; // MicroDB.masterData;

    Path path = FileSystems.getDefault().getPath(MicroDB.tableLocation, folder, filename + ext);
    return path.toString();
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

  public static LinkedHashMap<String, ArrayList<String>> buildInsertRecord(List<String> values) {
    LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
    List<String> colNames = new ArrayList<String>(Arrays.asList("rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"));
    if (values.size() < 6) return null;
    for (int i = 0; i < 6; i++) {
      token.put(colNames.get(i), new ArrayList<String>(Arrays.asList("int", values.get(i))));
    }
    return token;
  }
}
