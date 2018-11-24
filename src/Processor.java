import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class Processor {
  public String commandType;
  public boolean isExit;

  public Processor(String type) {
    this.commandType = type;
  }

  public static void help() {
    System.out.println(Utils.repeat("*", 80));
    System.out.println("SUPPORTED COMMANDS");
    System.out.println("All commands below are case insensitive");
    System.out.println();
    System.out.println(
        "\tCREATE TABLE table_name (column_name1 INT PRIMARY KEY, column_name2 data_type2 [NOT NULL],... );                         Create a new table schema if not already exist.");
    System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
    System.out
        .println("\tSELECT * FROM table_name WHERE <column_name> = <value>;  Display records whose column is <value>.");
    System.out.println(
        "\tUPDATE <table_name> SET column_name = value <primary_key=value>;  Modifies one or more records in a table.");
    System.out.println(
        "\tINSERT INTO <table_name> (column_list) VALUES (value1, value2, value3,..);  Insert a new record into the indicated table.");
    System.out.println(
        "\tDELETE FROM <table_name> WHERE primary_key = key_value; Delete a single row/record from a table given the row_id primary key.");
    System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
    System.out
        .println("\tSHOW TABLES;                                     Displays a list of all tables in the Database.");
    System.out.println("\tVERSION;                                         Show the program version.");
    System.out.println("\tHELP;                                            Show this help information");
    System.out.println("\tEXIT;                                            Exit the program");
    System.out.println();
    System.out.println();
    System.out.println(Utils.repeat("*", 80));
  }

  /**
   * Utility to print table
   * @param table
   */
  public static void printTable(List<LinkedHashMap<String, ArrayList<String>>> table) {
    if (table.isEmpty()) {
      System.out.println("0 Rows Returned");
      return;
    }

    int size[] = new int[table.get(0).size()];
    Arrays.fill(size, -1);
    int k = 0;
    for (LinkedHashMap<String, ArrayList<String>> x : table) {
      k = 0;
      for (String y : x.keySet()) {
        if (size[k] < y.length()) {
          size[k] = y.length();
        }
        if (size[k] < x.get(y).get(0).length()) {
          size[k] = x.get(y).get(0).length();
        }
        k++;
      }
    }
    for (int i : size)
      System.out.print("+" + Utils.repeat("-", i));
    System.out.print("+\n");
    k = 0;
    for (String x : table.get(0).keySet())
      System.out.print("|" + Utils.format(x.toUpperCase(), size[k++]));
    System.out.print("|\n");
    for (int i : size)
      System.out.print("+" + Utils.repeat("-", i));
    System.out.print("+\n");
    for (LinkedHashMap<String, ArrayList<String>> x : table) {
      k = 0;
      for (String y : x.keySet())
        System.out.print("|" + Utils.format(x.get(y).get(0), size[k++]));
      System.out.print("|\n");
      for (int i : size)
        System.out.print("+" + Utils.repeat("-", i));
      System.out.print("+\n");
    }
  }

  public static void showTables() {
    RandomAccessFile mDBtableFile;
    try {
      mDBtableFile = new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterTableName), "rw");
      BTree tableBTree = new BTree(mDBtableFile, MicroDB.masterTableName, false, true);
      printTable(tableBTree.printAll());
    } catch (FileNotFoundException e1) {
      System.out.println("Table Not found");
    }
  }

  public static boolean dropTable(String tableName) {
    boolean isDropped = false;
    int flag = 1;
    RandomAccessFile dropTableFile = null;
    File folder = new File("data/user_data");
    File[] listOfFiles = folder.listFiles();
    RandomAccessFile mDBColumnFile, mDBtableFile;
    for (int i = 0; i < listOfFiles.length; i++)
      if (listOfFiles[i].getName().equals(tableName + ".tbl")) {
        flag = 0;
      }
    if (flag == 1) {
      System.out.println("Table does not exist");
      return false;
    }

    try {
      mDBColumnFile = new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterColumnTableName), "rw");
      mDBtableFile = new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterTableName), "rw");
      dropTableFile = new RandomAccessFile(Utils.getFilePath("user", tableName), "rw");
    } catch (FileNotFoundException e1) {
      System.out.println(" Table file not found");
      return false;
    }
    try {
      dropTableFile.setLength(0);
      if (dropTableFile != null)
        dropTableFile.close();
      File f = new File(Utils.getFilePath("user", tableName));
      f.delete();
      System.out.println("Table Dropped");
    } catch (IOException e) {
      return false;
    }
    BTree tableBTree = new BTree(mDBtableFile, MicroDB.masterTableName, false, true);
    BTree columnBTree = new BTree(mDBColumnFile, MicroDB.masterColumnTableName, true, false);
    ArrayList<String> arryL = new ArrayList<String>();
    arryL.add(new Integer(1).toString()); // search cond col ordinal
    // position
    arryL.add("text"); // search cond col data type
    arryL.add(tableName); // search cond col value
    List<LinkedHashMap<String, ArrayList<String>>> op = tableBTree.searchNonPrimaryCol(arryL);
    for (LinkedHashMap<String, ArrayList<String>> map : op) {
      Integer rowId = Integer.parseInt(map.get("rowid").get(0));
      LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
      ArrayList<String> array = new ArrayList<String>();
      array.add("int");
      array.add(rowId.toString());
      token.put("rowid", new ArrayList<String>(array));
      isDropped = tableBTree.deleteRecord(token);

    }
    arryL = new ArrayList<String>();
    arryL.add(new Integer(1).toString()); // search cond col ordinal
    // position
    arryL.add("text"); // search cond col data type
    arryL.add(tableName); // search cond col value
    List<LinkedHashMap<String, ArrayList<String>>> opp = columnBTree.searchNonPrimaryCol(arryL);
    for (LinkedHashMap<String, ArrayList<String>> map : opp) {
      Integer rowId = Integer.parseInt(map.get("rowid").get(0));
      LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
      ArrayList<String> array = new ArrayList<String>();
      array.add("int");
      array.add(rowId.toString());
      token.put("rowid", new ArrayList<String>(array));
      isDropped = columnBTree.deleteRecord(token);
    }
    return isDropped;
  }
}
