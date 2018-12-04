import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Arrays;

public class Parser {
  // Supported data types
  public static List<String> dataTypes = new ArrayList<String>(
    Arrays.asList("tinyint", "smallint", "int", "bigint", "real", "double", "datetime", "date", "text")
  );

  public static List<String> contraints = new ArrayList<String>(
    Arrays.asList("not null", "unique", "primary key", "autoincrement", "default")
  );

  public static void parse(String userCommand) {
    /*
     * commandTokens is an array of Strings that contains one token per array
     * element The first token can be used to determine the type of command The
     * other tokens can be used to pass relevant parameters to each command-specific
     * method inside each case statement
     */
    // String[] commandTokens = userCommand.split(" ");
    ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

    /*
     * This switch handles a very small list of hardcoded commands of known syntax.
     * You will want to rewrite this method to interpret more complex commands.
     */
    switch (commandTokens.get(0)) {
      case "select":
        parseQueryString(userCommand);
        break;
      case "create":
        parseCreateString(userCommand);
        break;
      case "insert":
        parseInsertString(userCommand);
        break;
      case "update":
        parseUpdateString(userCommand);
        break;
      case "drop":
        if (commandTokens.get(1).equals("table"))
          Processor.dropTable(commandTokens.get(2));
        else
          System.out.println("Syntax Error");
        break;
      case "delete":
        parseDeleteString(userCommand);
        break;
      case "show":
        parseShowString(userCommand);
        break;
      case "help":
        Processor.help();
        break;
      case "version":
        Prompt.displayVersion();
        break;
      case "exit":
        MicroDB.isExit = true;
        break;
      case "quit":
      MicroDB.isExit = true;
      default:
        System.out.println("I didn't understand the command: \"" + userCommand + "\"");
        break;
    }
  }

  public static void parseQueryString(String queryString) {
    String tableName = "";
    ArrayList<String> cols = new ArrayList<String>();
    Map<String, ArrayList<String>> tableInfo = new LinkedHashMap<String, ArrayList<String>>();
    RandomAccessFile newTable = null;
    RandomAccessFile mDBColumnFile;
    RandomAccessFile mDBtableFile;
    try {
      queryString = queryString.replace("*", " * ");
      queryString = queryString.replace("=", " = ");
      queryString = queryString.replace(",", " , ");
      ArrayList<String> queryTokens = new ArrayList<String>(Arrays.asList(queryString.split("\\s+")));

      mDBColumnFile = new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterColumnTableName), "rw");
      mDBtableFile = new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterTableName), "rw");

      BTree columnBTree = new BTree(mDBColumnFile, MicroDB.masterColumnTableName, true, false);
      int flag = 1;
      if (queryTokens.get(1).equals("*") && queryTokens.get(2).equals("from") && queryTokens.size() == 4) {
        tableName = queryTokens.get(3);
        tableInfo = columnBTree.getSchema(queryTokens.get(3));
        if (tableInfo != null) {
          // System.out.println("*"); //select * from table;
          BTree tableBTree;
          try {
            if (tableName.trim().equals(MicroDB.masterTableName)) {
              tableBTree = new BTree(mDBtableFile, tableName, false, true);

            } else if (tableName.trim().equals(MicroDB.masterColumnTableName)) {
              tableBTree = columnBTree;

            } else {
              newTable = new RandomAccessFile(Utils.getFilePath("user", tableName), "rw");
              tableBTree = new BTree(newTable, tableName);
            }
            Processor.printTable(tableBTree.printAll());
            try {
              if (newTable != null)
                newTable.close();
            } catch (IOException e) {
              // TODO Auto-generated catch block
              // System.out.println("Unexpected Error");
              e.printStackTrace();
            }

          }

          catch (FileNotFoundException e) {
            System.out.println("Table file not found");
          }
        }
      } else if (queryTokens.get(1).equals("*") && queryTokens.get(2).equals("from")
          && queryTokens.get(4).equals("where") && queryTokens.get(6).equals("=")) {
        tableName = queryTokens.get(3);
        tableInfo = columnBTree.getSchema(queryTokens.get(3));
        if (tableInfo != null && tableInfo.keySet().contains(queryTokens.get(5))) {
          BTree tableBTree;
          try {

            if (tableName.trim().equals(MicroDB.masterTableName)) {
              tableBTree = new BTree(mDBtableFile, tableName, false, true);

            } else if (tableName.trim().equals(MicroDB.masterColumnTableName)) {
              tableBTree = new BTree(new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterColumnTableName), "rw"), tableName, true, false);

            } else {
              newTable = new RandomAccessFile(Utils.getFilePath("user", tableName), "rw");
              tableBTree = new BTree(newTable, tableName);
            }

          } catch (FileNotFoundException e) {
            System.out.println("Table file not found");
            mDBtableFile.close();
            return;
          }
          // search cond
          ArrayList<String> arryL = new ArrayList<String>();
          Integer ordinalPos = 0;
          String dataType = "";
          for (String key : tableInfo.keySet()) {
            if (key.equals(queryTokens.get(5))) {
              dataType = tableInfo.get(key).get(0);
              break;
            }
            ordinalPos++;
          }
          arryL.add(ordinalPos.toString()); // search cond col ordinal
          // position
          arryL.add(dataType); // search cond col data type
          if ((queryTokens.get(7).charAt(0) == '\''
              && queryTokens.get(7).charAt(queryTokens.get(7).length() - 1) == '\'')
              || (queryTokens.get(7).charAt(0) == '"'
                  && queryTokens.get(7).charAt(queryTokens.get(7).length() - 1) == '"'))
            arryL.add(queryTokens.get(7).substring(1, queryTokens.get(7).length() - 1));
          else
            arryL.add(queryTokens.get(7));
          // arryL.add(queryTokens.get(7)); // search cond col value

          if (ordinalPos == 0) {
            LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
            ArrayList<String> array = new ArrayList<String>();
            array.add(dataType);
            if ((queryTokens.get(7).charAt(0) == '\''
                && queryTokens.get(7).charAt(queryTokens.get(7).length() - 1) == '\'')
                || (queryTokens.get(7).charAt(0) == '"'
                    && queryTokens.get(7).charAt(queryTokens.get(7).length() - 1) == '"'))
              array.add(queryTokens.get(7).substring(1, queryTokens.get(7).length() - 1));
            else
              array.add(queryTokens.get(7));

            token.put(queryTokens.get(5), new ArrayList<String>(array));
            LinkedHashMap<String, ArrayList<String>> op = tableBTree.searchWithPrimaryKey(token);
            List<LinkedHashMap<String, ArrayList<String>>> temp = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
            temp.add(op);
            Processor.printTable(temp);

          } else {
            List<LinkedHashMap<String, ArrayList<String>>> op = tableBTree.searchNonPrimaryCol(arryL);
            Processor.printTable(op);

          }
          try {
            if (newTable != null)
              newTable.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            // System.out.println("Unexpected Error");
          }

        }
      } else {
        tableName = queryTokens.get(queryTokens.indexOf("from") + 1);
        tableInfo = columnBTree.getSchema(queryTokens.get(queryTokens.indexOf("from") + 1));
        if (tableInfo != null) {
          for (int i = 1; i < queryTokens.indexOf("from") && flag == 1; i++) {
            if (tableInfo.keySet().contains(queryTokens.get(i)) && queryTokens.get(i + 1).equals(",")) {
              cols.add(queryTokens.get(i));
              i++;
            } else if (tableInfo.keySet().contains(queryTokens.get(i)) && queryTokens.get(i + 1).equals("from"))
              cols.add(queryTokens.get(i));
            else
              flag = 0;
          }

          if (flag == 1) // select "coln" from table;
          {
            ArrayList<String> removeColumn = new ArrayList<String>(tableInfo.keySet());
            removeColumn.removeAll(cols);
            BTree tableBTree;
            try {

              if (tableName.trim().equals(MicroDB.masterTableName)) {
                tableBTree = new BTree(mDBtableFile, tableName, false, true);

              } else if (tableName.trim().equals(MicroDB.masterColumnTableName)) {
                tableBTree = new BTree(new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterColumnTableName), "rw"), tableName, true, false);

              } else {
                newTable = new RandomAccessFile(Utils.getFilePath("user", tableName), "rw");
                tableBTree = new BTree(newTable, tableName);
              }

              List<LinkedHashMap<String, ArrayList<String>>> op = tableBTree.printAll();
              for (LinkedHashMap<String, ArrayList<String>> map : op) {

                for (String x : removeColumn) {
                  map.remove(x);
                }

              }
              if (!queryTokens.contains("where")) // condition not
              // working
              {
                Processor.printTable(op);
              }
              if (newTable != null)
                newTable.close();
            }

            catch (Exception e) {
              System.out.println("Table file not found");
            }

          }
          if (queryTokens.size() > queryTokens.indexOf("from") + 2
              && queryTokens.get(queryTokens.indexOf("from") + 2).equals("where")
              && queryTokens.get(queryTokens.indexOf("from") + 4).equals("=")
              && tableInfo.keySet().contains(queryTokens.get(queryTokens.indexOf("from") + 3))) {

            ArrayList<String> removeColumn = new ArrayList<String>(tableInfo.keySet());
            removeColumn.removeAll(cols);

            BTree tableBTree;
            try {

              if (tableName.trim().equals(MicroDB.masterTableName)) {
                tableBTree = new BTree(mDBtableFile, tableName, false, true);

              } else if (tableName.trim().equals(MicroDB.masterColumnTableName)) {
                tableBTree = new BTree(new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterColumnTableName), "rw"), tableName, true, false);

              } else {
                newTable = new RandomAccessFile(Utils.getFilePath("user", tableName), "rw");
                tableBTree = new BTree(newTable, tableName);
              }

            } catch (FileNotFoundException e) {
              System.out.println("Table file not found");
              return;
            }
            // search cond
            ArrayList<String> arryL = new ArrayList<String>();
            Integer ordinalPos = 0;
            String dataType = "";
            for (String key : tableInfo.keySet()) {
              if (key.equals(queryTokens.get(queryTokens.size() - 3))) {
                dataType = tableInfo.get(key).get(0);
                break;
              }
              ordinalPos++;
            }
            arryL.add(ordinalPos.toString()); // search cond col
            // ordinal position
            arryL.add(dataType); // search cond col data type
            if ((queryTokens.get(queryTokens.size() - 1).charAt(0) == '\'' && queryTokens.get(queryTokens.size() - 1)
                .charAt(queryTokens.get(queryTokens.size() - 1).length() - 1) == '\'')
                || (queryTokens.get(queryTokens.size() - 1).charAt(0) == '"' && queryTokens.get(queryTokens.size() - 1)
                    .charAt(queryTokens.get(queryTokens.size() - 1).length() - 1) == '"'))
              queryTokens.get(queryTokens.size() - 1).substring(1,
                  queryTokens.get(queryTokens.size() - 1).length() - 1);
            arryL.add(queryTokens.get(queryTokens.size() - 1)); // search
            // cond
            // col
            // value

            if (ordinalPos == 0) {
              LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
              ArrayList<String> array = new ArrayList<String>();
              array.add(dataType);
              array.add(queryTokens.get(7));

              token.put(queryTokens.get(5), new ArrayList<String>(array));
              LinkedHashMap<String, ArrayList<String>> op = tableBTree.searchWithPrimaryKey(token);
              List<LinkedHashMap<String, ArrayList<String>>> temp = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
              temp.add(op);

              for (LinkedHashMap<String, ArrayList<String>> map : temp) {

                for (String x : removeColumn) {
                  map.remove(x);
                }

              }
              Processor.printTable(temp);

            } else {
              List<LinkedHashMap<String, ArrayList<String>>> op = tableBTree.searchNonPrimaryCol(arryL);
              for (LinkedHashMap<String, ArrayList<String>> map : op) {

                for (String x : removeColumn) {
                  map.remove(x);
                }

              }
              Processor.printTable(op);

            }
            try {

              if (newTable != null)
                newTable.close();
            } catch (IOException e) {
              // TODO Auto-generated catch block
              System.out.println("IO Exception: " + e.getMessage());
            }

          }

        }
      }
      if (flag == 0)
        System.out.println("Syntax Error");
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    } catch (Exception e) {
      System.out.println("Syntax Error");
    }
  }

  public static void parseCreateString(String createTableString) {
    Pattern topLevel = Pattern.compile("(create\\s+table\\s+([a-z0-9_]+)\\s*)(\\(([a-z0-9_\\,\\s]+)\\))");
    Matcher matcher = topLevel.matcher(createTableString);
    if (!matcher.find() || matcher.groupCount() < 4) {
      System.out.println("Syntax Error. Please check and try again!");
      return;
    }
    String tableName = matcher.group(2);

    try {
      RandomAccessFile mDBtableFile = new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterTableName), "rw");
      RandomAccessFile mDBColumnFile = new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterColumnTableName), "rw");

      BTree mDBtabletree = new BTree(mDBtableFile, MicroDB.masterTableName, false, true);
      BTree mDBColumntree = new BTree(mDBColumnFile, MicroDB.masterColumnTableName, true, false);

      List<LinkedHashMap<String, ArrayList<String>>> schematableColList = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
      LinkedHashMap<String, ArrayList<String>> newTable = new LinkedHashMap<String, ArrayList<String>>();

      if (tableName.equals(MicroDB.masterColumnTableName) || tableName.equals(MicroDB.masterTableName)) {
        System.out.println("You have chose a reserved keyword for table name. Please change and try again!");
        return;
      }

      File folder = new File(Utils.getOSPath(new String[] { MicroDB.tableLocation, MicroDB.userDataFolder }));
      File[] listOfFiles = folder.listFiles();
      for (int i = 0; i < listOfFiles.length; i++) {
        if (listOfFiles[i].getName().equals(tableName + ".tbl")) {
          System.out.println(String.format("Table with name %s already exist!", tableName));
          return;
        }
      }

      String[] tableColumns = matcher.group(4).replaceAll(" +", " ").split(",");

      // First column should be of type row_id and primary key
      Pattern primaryKeyRegex = Pattern.compile("(row_id\\s+int\\s+primary\\s+key)");
      Matcher primaryKeyMatcher = primaryKeyRegex.matcher(tableColumns[0]);
      if (!primaryKeyMatcher.find()) {
        System.out.println("First column must be row_id and it should be a primary key");
        return;
      }

      /* Add records to base column table */
      int dbRowId = mDBColumntree.getNextMaxRowID() + 1;

      // Check all other columns
      String supportedTypes = String.join("|", dataTypes);
      Pattern columnPattern = Pattern.compile("([a-z0-9_]+)\\s+("+ supportedTypes + ")\\s*([a-z0-9\\s]*)");
      for (int i = 0; i < tableColumns.length; i++) {
        Matcher columnMatcher = columnPattern.matcher(tableColumns[i]);
        if (!columnMatcher.find()) {
          System.out.println("Syntax Error. Please check and try again!! " + columnMatcher.groupCount() + " " + columnMatcher.group(1));
          return;
        }
        String columnName = columnMatcher.group(1);
        String columnType = columnMatcher.group(2);
        String constraintsText = columnMatcher.group(3);

        /**
         * Handle unique constraint
         */
        String isUnique = constraintsText.contains("unique") ? "yes" : "no";

        String isNullable = "yes";
        String defaultValue = "null";
        if (constraintsText.contains("not null") || constraintsText.contains("primary key") || constraintsText.contains("default")) isNullable = "no";
        // default constraint
        if (constraintsText.contains("default") && constraintsText.split("\\s+").length==2){
          defaultValue = constraintsText.split(" ")[1];
        }
        schematableColList.add(Utils.buildInsertRecord(Arrays.asList(String.valueOf(dbRowId++), tableName, columnName, columnType, String.valueOf(i + 1), isNullable, defaultValue, isUnique)));
      }

      for (LinkedHashMap<String, ArrayList<String>> row : schematableColList) {
        mDBColumntree.insertNewRecord(row);
      }

      /* Add rows to base table */
      newTable.put("rowid", new ArrayList<String>(Arrays.asList("int", String.valueOf(mDBtabletree.getNextMaxRowID() + 1))));
      newTable.put("table_name", new ArrayList<String>(Arrays.asList("text", tableName)));
      mDBtabletree.insertNewRecord(newTable);

      /**
       * Create required table and initialize with zero bytes
       */
      RandomAccessFile tableFile = new RandomAccessFile(Utils.getFilePath("user", tableName), "rw");
      tableFile.setLength(0);

      new BTree(tableFile, tableName).createEmptyTable();

      if (tableFile != null) {
        tableFile.close();
      }
      System.out.println("Table Created");
    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
    }
  }

  public static void parseInsertString(String insertTableString) {
    try {
      BTree columnBTree = new BTree(MicroDB.dbColumnFile, MicroDB.masterColumnTableName, true, false);
      insertTableString = insertTableString.replace("(", " ( ");
      insertTableString = insertTableString.replace(")", " ) ");
      insertTableString = insertTableString.replace(",", " , ");
      Map<String, ArrayList<String>> tableInfo = new LinkedHashMap<String, ArrayList<String>>();
      Map<String, ArrayList<String>> tableVal = new LinkedHashMap<String, ArrayList<String>>();
      ArrayList<String> colName = new ArrayList<String>();
      ArrayList<String> insertTableTokens = new ArrayList<String>(Arrays.asList(insertTableString.split("\\s+")));
      int flag = 1;
      File folder = new File(Utils.getOSPath(new String[] { MicroDB.tableLocation, MicroDB.userDataFolder }));
      File[] listOfFiles = folder.listFiles();
      for (int i = 0; i < listOfFiles.length; i++) {
        if (listOfFiles[i].getName().equals(insertTableTokens.get(2) + ".tbl"))
          flag = 0;
      }
      if (flag == 1 || !insertTableTokens.get(1).equals("into"))
        System.out.println("Table does not exist/Syntax Error");
      else
        tableInfo = columnBTree.getSchema(insertTableTokens.get(2));
      if (insertTableTokens.get(3).equals("(")) {
        for (String x : tableInfo.keySet())
          if (tableInfo.get(x).contains("no"))
            colName.add(x);
        int k = insertTableTokens.indexOf(")");
        if (insertTableTokens.get(k + 1).equals("values") && insertTableTokens.get(k + 2).equals("(")
            && insertTableTokens.get(insertTableTokens.size() - 1).equals(")")) {
          for (int i = 4; !insertTableTokens.get(i).equals(")") && flag == 0; i++) {
            if (colName.contains(insertTableTokens.get(i)))
              colName.remove(insertTableTokens.get(i));
            if (tableInfo.keySet().contains(insertTableTokens.get(i))
                && (Utils.checkValue(tableInfo.get(insertTableTokens.get(i)).get(0), insertTableTokens.get(k + i - 1)))
                && insertTableTokens.get(i + 1).equals(",")) {
              tableVal.put(insertTableTokens.get(i), new ArrayList<String>(
                  Arrays.asList(tableInfo.get(insertTableTokens.get(i)).get(0), insertTableTokens.get(k + i - 1))));
              i++;
            } else if (tableInfo.keySet().contains(insertTableTokens.get(i))
                && (Utils.checkValue(tableInfo.get(insertTableTokens.get(i)).get(0), insertTableTokens.get(k + i - 1)))
                && insertTableTokens.get(i + 1).equals(")"))
              tableVal.put(insertTableTokens.get(i), new ArrayList<String>(
                  Arrays.asList(tableInfo.get(insertTableTokens.get(i)).get(0), insertTableTokens.get(k + i - 1))));
            else
              flag = 1;
          }
        } else
          flag = 1;
        if (colName.size() != 0)
          flag = 1;
      } else if (insertTableTokens.get(3).equals("values") && insertTableTokens.get(4).equals("(")
          && insertTableTokens.get(insertTableTokens.size() - 1).equals(")")) {
        int k = 5;
        for (String x : tableInfo.keySet()) {
          if (tableInfo.get(x).get(1).equals("no")) {
            if (Utils.checkValue(tableInfo.get(x).get(0), insertTableTokens.get(k))
                && insertTableTokens.get(k + 1).equals(",")) {
              tableVal.put(x, new ArrayList<String>(Arrays.asList(tableInfo.get(x).get(0), insertTableTokens.get(k))));
              k += 2;
            } else if (Utils.checkValue(tableInfo.get(x).get(0), insertTableTokens.get(k))
                && insertTableTokens.get(k + 1).equals(")")) {
              tableVal.put(x, new ArrayList<String>(Arrays.asList(tableInfo.get(x).get(0), insertTableTokens.get(k))));
              k++;
            } else
              flag = 1;
          } else {
            if (!insertTableTokens.get(k).equals(",") && !insertTableTokens.get(k).equals(")")) {
              if (Utils.checkValue(tableInfo.get(x).get(0), insertTableTokens.get(k))
                  && insertTableTokens.get(k + 1).equals(",")) {
                tableVal.put(x,
                    new ArrayList<String>(Arrays.asList(tableInfo.get(x).get(0), insertTableTokens.get(k))));
                k += 2;
              } else if (Utils.checkValue(tableInfo.get(x).get(0), insertTableTokens.get(k))
                  && insertTableTokens.get(k + 1).equals(")")) {
                tableVal.put(x,
                    new ArrayList<String>(Arrays.asList(tableInfo.get(x).get(0), insertTableTokens.get(k))));
                k++;
              }
            } else {
              tableVal.put(x, new ArrayList<String>(Arrays.asList(tableInfo.get(x).get(0), "NULL")));
            }

          }
        }
      } else
        flag = 1;
      if (flag == 0) {
        int primaryKeyVal = -1;
        for (String key : tableVal.keySet()) {
          String primaryKey = tableVal.get(key).get(1);
          primaryKeyVal = Integer.parseInt(primaryKey);
          break;
        }
        for (String key : tableVal.keySet()) {
          tableInfo.put(key, tableVal.get(key));
        }
        // String fileName = tableLocation + insertTableTokens.get(2) + tableFormat;
        try {
          RandomAccessFile newTable = new RandomAccessFile(Utils.getFilePath("user", insertTableTokens.get(2)), "rw");
          BTree tableTree = new BTree(newTable, insertTableTokens.get(2));

          if (tableTree.isEmptyTable()) {
            tableTree.createNewTableLeaf(tableInfo);

          } else {
            if (tableTree.isPrimaryKeyExists(primaryKeyVal)) {
              System.out.println(" Primary key with value " + primaryKeyVal + " already exists");
              return;
            } else {
              for (String x : tableInfo.keySet())
                if ((tableInfo.get(x).get(1).charAt(0) == '\''
                    && tableInfo.get(x).get(1).charAt(tableInfo.get(x).get(1).length() - 1) == '\'')
                    || (tableInfo.get(x).get(1).charAt(0) == '"'
                        && tableInfo.get(x).get(1).charAt(tableInfo.get(x).get(1).length() - 1) == '"'))
                  tableInfo.put(x, new ArrayList<String>(Arrays.asList("text",
                      tableInfo.get(x).get(1).substring(1, tableInfo.get(x).get(1).length() - 1))));
              tableTree.insertNewRecord(tableInfo);
              if (newTable != null)
                newTable.close();

            }
          }

        } catch (Exception e) {
          // TODO Auto-generated catch block
          System.out.println("Unexpected Error");
        }

        System.out.println("1 row inserted");

      } else
        System.out.println("Error in syntax");
    } catch (Exception e) {
      System.out.println("Syntax Error");
    }
  }

  public static void parseShowString(String showTableString) {
    try {
      ArrayList<String> showTableTokens = new ArrayList<String>(Arrays.asList(showTableString.split("\\s+")));
      if (showTableTokens.get(1).equals("tables"))
        Processor.showTables();
      else
        System.out.println("Syntax Error");
    } catch (Exception e) {
      System.out.println("Syntax Error");
    }
  }

  public static void parseUpdateString(String updateTableString) {
    try {
      updateTableString = updateTableString.replace("=", " = ");
      BTree columnBTree = new BTree(MicroDB.dbColumnFile, MicroDB.masterColumnTableName, true, false);
      ArrayList<String> updateTableTokens = new ArrayList<String>(Arrays.asList(updateTableString.split("\\s+")));
      Map<String, ArrayList<String>> tableInfo = new LinkedHashMap<String, ArrayList<String>>();
      int flag = 1;
      if (updateTableTokens.get(2).equals("set") && updateTableTokens.get(4).equals("=")) {
        if (updateTableTokens.size() > 6 && updateTableTokens.get(6).equals("where")
            && updateTableTokens.get(8).equals("=")) {
          tableInfo = columnBTree.getSchema(updateTableTokens.get(1));
          if (tableInfo != null) {
            if (tableInfo.keySet().contains(updateTableTokens.get(3))
                && tableInfo.keySet().contains(updateTableTokens.get(7))) {
              if (Utils.checkValue(tableInfo.get(updateTableTokens.get(3)).get(0), updateTableTokens.get(5))
                  && Utils.checkValue(tableInfo.get(updateTableTokens.get(7)).get(0), updateTableTokens.get(9))) {

                ArrayList array = new ArrayList<String>();
                LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();

                BTree tableTree = null;
                RandomAccessFile newTable = null;
                try {
                  newTable = new RandomAccessFile(Utils.getFilePath("user", updateTableTokens.get(1)), "rw");
                  tableTree = new BTree(newTable, updateTableTokens.get(1));
                } catch (FileNotFoundException e1) {
                  System.out.println(" Table not found during update");
                  return;
                }
                String dataType = tableInfo.get(updateTableTokens.get(7)).get(0);
                array.add(dataType);
                if ((updateTableTokens.get(9).charAt(0) == '\''
                    && updateTableTokens.get(9).charAt(updateTableTokens.get(9).length() - 1) == '\'')
                    || (updateTableTokens.get(9).charAt(0) == '"'
                        && updateTableTokens.get(9).charAt(updateTableTokens.get(9).length() - 1) == '"'))
                  array.add(updateTableTokens.get(9).substring(1, updateTableTokens.get(9).length() - 1));
                else
                  array.add(new String(updateTableTokens.get(9)));

                token.put(updateTableTokens.get(7), new ArrayList<String>(array));

                LinkedHashMap<String, ArrayList<String>> result = tableTree.searchWithPrimaryKey(token);
                if (result == null) {
                  return;
                }
                LinkedHashMap<String, ArrayList<String>> table = columnBTree.getSchema(updateTableTokens.get(1));
                for (String column : result.keySet()) {
                  if (column.equals(updateTableTokens.get(3)))// colname
                  {
                    ArrayList<String> value = table.get(column);
                    value.remove(value.size() - 1);
                    if ((updateTableTokens.get(5).charAt(0) == '\''
                        && updateTableTokens.get(5).charAt(updateTableTokens.get(5).length() - 1) == '\'')
                        || (updateTableTokens.get(5).charAt(0) == '"'
                            && updateTableTokens.get(5).charAt(updateTableTokens.get(5).length() - 1) == '"'))
                      value.add(updateTableTokens.get(5).substring(1, updateTableTokens.get(5).length() - 1)); // newValue
                    else
                      value.add(updateTableTokens.get(5));
                    table.put(column, value);
                  } else {
                    ArrayList<String> value = table.get(column);
                    ArrayList<String> res = result.get(column);
                    value.remove(value.size() - 1);
                    String val = res.get(0);
                    value.add(val);
                    table.put(column, value);
                  }

                }
                token.clear();
                array.clear();
                dataType = tableInfo.get(updateTableTokens.get(7)).get(0);
                array.add(dataType);
                array.add(new String(updateTableTokens.get(9)));
                token.put(updateTableTokens.get(7), new ArrayList<String>(array));
                tableTree.deleteRecord(token);
                try {
                  tableTree.insertNewRecord(table);
                  System.out.println(" 1 row updated");
                  if (newTable != null)
                    newTable.close();
                } catch (Exception e) {

                  System.out.println(" Update Failed");
                }

              } else
                flag = 0;
            } else
              flag = 0;
          } else
            flag = 0;
        } else {
          tableInfo = columnBTree.getSchema(updateTableTokens.get(1));
          if (tableInfo != null) {
            if (tableInfo.keySet().contains(updateTableTokens.get(3))) {
              if (Utils.checkValue(tableInfo.get(updateTableTokens.get(3)).get(0), updateTableTokens.get(5))) {

                int noOfRows = 0;
                BTree tableTree = null;
                RandomAccessFile newTable = null;
                try {
                  newTable = new RandomAccessFile(Utils.getFilePath("user", updateTableTokens.get(1)), "rw");
                  tableTree = new BTree(newTable, updateTableTokens.get(1));
                } catch (FileNotFoundException e1) {
                  System.out.println(" Table not found during update");
                  return;
                }

                List<LinkedHashMap<String, ArrayList<String>>> list_result = tableTree.printAll();
                for (LinkedHashMap<String, ArrayList<String>> result : list_result) {
                  LinkedHashMap<String, ArrayList<String>> table = columnBTree.getSchema(updateTableTokens.get(1));
                  LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
                  ArrayList<String> array = new ArrayList<String>();

                  for (String column : result.keySet()) {
                    if (column.equals(updateTableTokens.get(3)))// colname
                    {
                      ArrayList<String> value = table.get(column);
                      value.remove(value.size() - 1);
                      if ((updateTableTokens.get(5).charAt(0) == '\''
                          && updateTableTokens.get(5).charAt(updateTableTokens.get(5).length() - 1) == '\'')
                          || (updateTableTokens.get(5).charAt(0) == '"'
                              && updateTableTokens.get(5).charAt(updateTableTokens.get(5).length() - 1) == '"'))
                        value.add(updateTableTokens.get(5).substring(1, updateTableTokens.get(5).length() - 1)); // newValue
                      else
                        value.add(updateTableTokens.get(5));
                      table.put(column, value);
                    } else {
                      ArrayList<String> value = table.get(column);
                      ArrayList<String> res = result.get(column);
                      value.remove(value.size() - 1);
                      String val = res.get(0);
                      value.add(val);
                      table.put(column, value);
                    }

                  }
                  token.clear();
                  array.clear();

                  array.add("int");
                  array.add(new String(table.get(tableTree.getPrimaryKey()).get(1)));
                  token.put(tableTree.getPrimaryKey(), new ArrayList<String>(array));
                  tableTree.deleteRecord(token);
                  try {
                    tableTree.insertNewRecord(table);
                    noOfRows++;
                  } catch (Exception e) {

                    System.out.println(" Update Failed");
                  }

                }
                if (newTable != null)
                  try {
                    newTable.close();
                  } catch (IOException e) {
                    // TODO Auto-generated catch block
                    System.out.println("Unexpected Error");
                  }

                System.out.println(noOfRows + " row(s) updated.");

              } else
                flag = 0;
            } else
              flag = 0;
          } else
            flag = 0;
        }
      } else
        flag = 0;
      if (flag == 0)
        System.out.println("Syntax Error");
    } catch (Exception e) {
      System.out.println("Syntax Error");
    }
  }

  public static void parseDeleteString(String deleteTableString) {
    try {
      BTree columnBTree = new BTree(MicroDB.dbColumnFile, MicroDB.masterColumnTableName, true, false);
      deleteTableString = deleteTableString.replace("=", " = ");
      ArrayList<String> deleteTableTokens = new ArrayList<String>(Arrays.asList(deleteTableString.split("\\s+")));
      Map<String, ArrayList<String>> tableInfo = new LinkedHashMap<String, ArrayList<String>>();
      int flag = 1;
      if (deleteTableTokens.get(1).equals("from")) {
        if (deleteTableTokens.size() > 3 && deleteTableTokens.get(3).equals("where")
            && deleteTableTokens.get(5).equals("=")) {
          tableInfo = columnBTree.getSchema(deleteTableTokens.get(2));
          if (tableInfo != null) {
            for (String x : tableInfo.keySet()) {
              if (!deleteTableTokens.get(4).equals(x))
                flag = 0;
              break;
            }
            String dataTypeOfDeleteKey = "int";
            if (tableInfo != null) {
              for (String x : tableInfo.keySet()) {
                if (deleteTableTokens.get(4).equals(x)) {
                  dataTypeOfDeleteKey = tableInfo.get(x).get(0);
                }

                break;
              }
            }

            if (flag == 1) {
              LinkedHashMap<String, ArrayList<String>> token = new LinkedHashMap<String, ArrayList<String>>();
              ArrayList<String> array = new ArrayList<String>();
              array.add(dataTypeOfDeleteKey);
              if ((deleteTableTokens.get(6).charAt(0) == '\''
                  && deleteTableTokens.get(6).charAt(deleteTableTokens.get(6).length() - 1) == '\'')
                  || (deleteTableTokens.get(6).charAt(0) == '"'
                      && deleteTableTokens.get(6).charAt(deleteTableTokens.get(6).length() - 1) == '"'))
                array.add(deleteTableTokens.get(6).substring(1, deleteTableTokens.get(6).length() - 1));
              else
                array.add(deleteTableTokens.get(6));
              token.put(deleteTableTokens.get(4), new ArrayList<String>(array));
              RandomAccessFile filename;

              try {
                filename = new RandomAccessFile(Utils.getFilePath("user", deleteTableTokens.get(2)), "rw");

                BTree mDBColumnFiletree = new BTree(filename, deleteTableTokens.get(2));
                mDBColumnFiletree.deleteRecord(token);
                System.out.println("1 row deleted");

                if (filename != null)
                  filename.close();
              } catch (Exception e) {
                // TODO Auto-generated catch block
                System.out.println("Table does not exists");
              }
            }
          } else
            System.out.println("Table does not exist!!!");
        } else {
          tableInfo = columnBTree.getSchema(deleteTableTokens.get(2));
          if (tableInfo != null)
            System.out.println("Schema doesn't exists");
          else
            System.out.println("Table does not exist!!!");
        }
      }
    } catch (Exception e) {
      System.out.println("Syntax Error");
    }
  }
}
