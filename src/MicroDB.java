import java.io.RandomAccessFile;
import java.io.File;
import java.util.Scanner;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Arrays;

public class MicroDB {
  protected static final String masterTableName = "master_tables";
  public static final String masterColumnTableName = "master_columns";
  protected static final String tableLocation = "data";
  protected static final String tableFormat = ".tbl";
  private static final String prompt = "mdbsql> ";
  protected static boolean isExit = false;
  protected static RandomAccessFile dbColumnFile;
  protected static RandomAccessFile dbMasterTableFile;
  static long pageSize = 512;
  static Scanner scanner = new Scanner(System.in).useDelimiter(";");

  public static void main(String[] args) {
    // File system setup
    File folder = new File("data");
    if (!folder.exists()) {
      folder.mkdir();
      folder = new File("data/user_data");
      folder.mkdir();
    }

    try {
      dbMasterTableFile = new RandomAccessFile(masterTableName + tableFormat, "rw");
      dbColumnFile = new RandomAccessFile(masterColumnTableName + tableFormat, "rw");

      BTree mDBtabletree = new BTree(dbMasterTableFile, MicroDB.masterTableName, false, true);
      BTree mDBColumnFiletree = new BTree(dbColumnFile, MicroDB.masterColumnTableName, true, false);

      buildDBBaseTable(dbMasterTableFile, mDBtabletree);
      buildDBColumnTable(dbColumnFile, mDBColumnFiletree);

    } catch (Exception e) {
      System.out.println("Unexpected Error: " + e.getMessage());
    }

    // Greet user!
    Prompt.splashScreen();

    String userCommand = "";
    while (!isExit) {
      System.out.print(prompt);
      userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
      Parser.parse(userCommand);
    }
    System.out.println("Exiting...");
  }

  private static void buildDBColumnTable(RandomAccessFile mDBColumnFile, BTree mDBColumnFiletree) throws Exception {
    if (mDBColumnFile.length() > 0) return;

    LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();

    token = Utils.buildInsertRecord(Arrays.asList("1", MicroDB.masterTableName, "rowid", "int", "1", "no"));
    mDBColumnFiletree.createNewTableLeaf(token);

    token = Utils.buildInsertRecord(Arrays.asList("2", MicroDB.masterTableName, "table_name", "text", "2", "no"));
    mDBColumnFiletree.insertNewRecord(token);

    token = Utils.buildInsertRecord(Arrays.asList("3", MicroDB.masterColumnTableName, "rowid", "int", "1", "no"));
    mDBColumnFiletree.insertNewRecord(token);

    token = Utils.buildInsertRecord(Arrays.asList("4", MicroDB.masterColumnTableName, "table_name", "text", "2", "no"));
    mDBColumnFiletree.insertNewRecord(token);

    token = Utils.buildInsertRecord(Arrays.asList("5", MicroDB.masterColumnTableName, "column_name", "text", "3", "no"));
    mDBColumnFiletree.insertNewRecord(token);

    token = Utils.buildInsertRecord(Arrays.asList("6", MicroDB.masterColumnTableName, "data_type", "text", "4", "no"));
    mDBColumnFiletree.insertNewRecord(token);

    token = Utils.buildInsertRecord(Arrays.asList("7", MicroDB.masterColumnTableName, "ordinal_position", "tinyint", "5", "no"));
    mDBColumnFiletree.insertNewRecord(token);

    token = Utils.buildInsertRecord(Arrays.asList("8", MicroDB.masterColumnTableName, "is_nullable", "text", "6", "no"));
    mDBColumnFiletree.insertNewRecord(token);
  }

  private static void buildDBBaseTable(RandomAccessFile mDBtableFile, BTree mDBtabletree) throws Exception {
    if (mDBtableFile.length() > 0) return;

    LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
    token.put("rowid", new ArrayList<String>(Arrays.asList("int", "1")));
    token.put("table_name", new ArrayList<String>(Arrays.asList("text", MicroDB.masterTableName)));
    mDBtabletree.createNewTableLeaf(token);

    token.clear();
    token.put("rowid", new ArrayList<String>(Arrays.asList("int", "2")));
    token.put("table_name", new ArrayList<String>(Arrays.asList("text", MicroDB.masterColumnTableName)));
    mDBtabletree.insertNewRecord(token);
  }
}