import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class BTree {

  public static final int NODE_INTERNAL = 5;
  public static final int NODE_LEAF = 13;

  private RandomAccessFile binaryFile;

  private static final int pageSize = 512;

  private int currentPage = 1;

  private long pageHeader_Offset_noOfCells = 0;
  private long pageHeader_Offset_startOfCell = 0;
  private long pageHeader_Offset_rightPagePointer = 0;
  private long pageHeader_array_offset = 0;
  private long pageHeader_offset = 0;

  private ZoneId zoneId = ZoneId.of("America/Chicago");

  private boolean isLeafPage = false;

  private int lastPage = 1;

  private ArrayList<Integer> routeOfLeafPage = new ArrayList<>();

  private boolean isColumnSchema;

  private boolean isTableSchema;

  private String tableName;

  private String tableKey = "rowid";

  private BTree mDBColumnFiletree;

  public BTree(RandomAccessFile file, String tableName) {
    binaryFile = file;
    this.tableName = tableName;
    try {
      if (file.length() > 0) {
        lastPage = (int) (file.length() / 512);
        currentPage = lastPage;
      }

      if (!tableName.equals(MicroDB.masterColumnTableName) && !tableName.equals(MicroDB.masterTableName)) {
        mDBColumnFiletree = new BTree(new RandomAccessFile(Utils.getFilePath("master", MicroDB.masterColumnTableName), "rw"), MicroDB.masterColumnTableName,
            true, false);

        for (String key : mDBColumnFiletree.getSchema(tableName).keySet()) {
          tableKey = key;
          break;
        }
      }
    } catch (Exception e) {
      System.out.println("Unexpected Error");
    }
  }

  public BTree(RandomAccessFile file, String tableName, boolean isColSchema, boolean isTableSchema) {
    this(file, tableName);
    this.isColumnSchema = isColSchema;
    this.isTableSchema = isTableSchema;
  }

  public void createNewInterior(int pageNumber, int rowID, int pageRight) {
    try {
      binaryFile.seek(0);
      binaryFile.write(5);
      binaryFile.write(1);
      binaryFile.writeShort(pageSize - 8);
      binaryFile.writeInt(pageRight);
      binaryFile.writeShort(pageSize - 8);
      binaryFile.seek(pageSize - 8);
      binaryFile.writeInt(pageNumber);
      binaryFile.writeInt(rowID);

    } catch (IOException e) {
      System.out.println("Unexpected Error");
    }
  }

  private void writeCellInterior(int pageLocation, int pageNumber, int rowID, int pageRight) {
    try {

      binaryFile.seek(pageLocation * pageSize - pageSize + 1);
      short No_OfCells = binaryFile.readByte();
      if (No_OfCells < 49) {
        binaryFile.seek(pageLocation * pageSize - pageSize + 4);
        if (binaryFile.readInt() == pageNumber && pageRight != -1) {
          binaryFile.seek(pageLocation * pageSize - pageSize + 4);
          binaryFile.writeInt(pageRight);
          long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
          binaryFile.seek(pageLocation * pageSize - pageSize + 2);
          binaryFile.writeShort((int) cellStartOffset);
          binaryFile.seek(pageLocation * pageSize - pageSize + 1);
          binaryFile.write(No_OfCells + 1);
          binaryFile.seek(cellStartOffset);
          binaryFile.writeInt(pageNumber);
          binaryFile.writeInt(rowID);
          binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * No_OfCells));
          binaryFile.writeShort((short) cellStartOffset);
        } else {
          int flag = 0;
          for (int i = 0; i < No_OfCells; i++) {
            binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
            binaryFile.seek(binaryFile.readUnsignedShort());
            if (binaryFile.readInt() == pageNumber) {
              flag = 1;
              int tempRowID = binaryFile.readInt();
              binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
              binaryFile.seek(binaryFile.readUnsignedShort() + 4);
              binaryFile.writeInt(rowID);
              long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
              binaryFile.seek(pageLocation * pageSize - pageSize + 2);
              binaryFile.writeShort((int) cellStartOffset);
              binaryFile.seek(pageLocation * pageSize - pageSize + 1);
              binaryFile.write(No_OfCells + 1);
              binaryFile.seek(cellStartOffset);
              binaryFile.writeInt(pageRight);
              binaryFile.writeInt(tempRowID);
              binaryFile.seek(pageLocation * pageSize - pageSize + 8 + 2 * No_OfCells);
              binaryFile.writeShort((short) cellStartOffset);
            }
          }
          if (flag == 0) {
            long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
            binaryFile.seek(pageLocation * pageSize - pageSize + 2);
            binaryFile.writeShort((int) cellStartOffset);
            binaryFile.seek(pageLocation * pageSize - pageSize + 1);
            binaryFile.write(No_OfCells + 1);
            binaryFile.seek(cellStartOffset);
            binaryFile.writeInt(pageNumber);
            binaryFile.writeInt(rowID);
            binaryFile.seek(pageLocation * pageSize - pageSize + 8 + 2 * No_OfCells);
            binaryFile.writeShort((short) cellStartOffset);
          }
        }
        int tempAddi, tempAddj, tempi, tempj;
        for (int i = 0; i <= No_OfCells; i++)
          for (int j = i + 1; j <= No_OfCells; j++) {
            binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
            tempAddi = binaryFile.readUnsignedShort();
            binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * j));
            tempAddj = binaryFile.readUnsignedShort();
            binaryFile.seek(tempAddi + 4);
            tempi = binaryFile.readInt();
            binaryFile.seek(tempAddj + 4);
            tempj = binaryFile.readInt();
            if (tempi > tempj) {
              binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
              binaryFile.writeShort(tempAddj);
              binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * j));
              binaryFile.writeShort(tempAddi);

            }
          }
      } else {
        binaryFile.seek(pageLocation * pageSize - pageSize + 4);
        if (binaryFile.readInt() == pageNumber && pageRight != -1) {
          binaryFile.seek(pageLocation * pageSize - pageSize + 4);
          binaryFile.writeInt(pageRight);
          long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
          binaryFile.seek(pageLocation * pageSize - pageSize + 2);
          binaryFile.writeShort((int) cellStartOffset);
          binaryFile.seek(pageLocation * pageSize - pageSize + 1);
          binaryFile.write(No_OfCells + 1);
          binaryFile.seek(cellStartOffset);
          binaryFile.writeInt(pageNumber);
          binaryFile.writeInt(rowID);
          binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * No_OfCells));
          binaryFile.writeShort((short) cellStartOffset);
        } else {
          int flag = 0;
          for (int i = 0; i < No_OfCells; i++) {
            binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
            binaryFile.seek(binaryFile.readUnsignedShort());
            if (binaryFile.readInt() == pageNumber) {
              flag = 1;
              int tempRowID = binaryFile.readInt();
              binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
              binaryFile.seek(binaryFile.readUnsignedShort() + 4);
              binaryFile.writeInt(rowID);
              long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
              binaryFile.seek(pageLocation * pageSize - pageSize + 2);
              binaryFile.writeShort((int) cellStartOffset);
              binaryFile.seek(pageLocation * pageSize - pageSize + 1);
              binaryFile.write(No_OfCells + 1);
              binaryFile.seek(cellStartOffset);
              binaryFile.writeInt(pageRight);
              binaryFile.writeInt(tempRowID);
              binaryFile.seek(pageLocation * pageSize - pageSize + 8 + 2 * No_OfCells);
              binaryFile.writeShort((short) cellStartOffset);
            }
          }
          if (flag == 0) {
            long cellStartOffset = (pageLocation * (pageSize)) - (8 * (No_OfCells + 1));
            binaryFile.seek(pageLocation * pageSize - pageSize + 2);
            binaryFile.writeShort((int) cellStartOffset);
            binaryFile.seek(pageLocation * pageSize - pageSize + 1);
            binaryFile.write(No_OfCells + 1);
            binaryFile.seek(cellStartOffset);
            binaryFile.writeInt(pageNumber);
            binaryFile.writeInt(rowID);
            binaryFile.seek(pageLocation * pageSize - pageSize + 8 + 2 * No_OfCells);
            binaryFile.writeShort((short) cellStartOffset);
          }
        }
        int tempAddi, tempAddj, tempi, tempj;
        for (int i = 0; i <= No_OfCells; i++)
          for (int j = i + 1; j <= No_OfCells; j++) {
            binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
            tempAddi = binaryFile.readUnsignedShort();
            binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * j));
            tempAddj = binaryFile.readUnsignedShort();
            binaryFile.seek(tempAddi + 4);
            tempi = binaryFile.readInt();
            binaryFile.seek(tempAddj + 4);
            tempj = binaryFile.readInt();
            if (tempi > tempj) {
              binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
              binaryFile.writeShort(tempAddj);
              binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * j));
              binaryFile.writeShort(tempAddi);

            }
          }
        if (pageLocation == 1) {
          int x, y;
          binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * 25));
          binaryFile.seek(binaryFile.readUnsignedShort());
          x = binaryFile.readInt();
          y = binaryFile.readInt();
          writePageHeader(lastPage + 1, false, 0, x);
          for (int i = 0; i < 25; i++) {
            binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
            binaryFile.seek(binaryFile.readUnsignedShort());
            writeCellInterior(lastPage + 1, binaryFile.readInt(), binaryFile.readInt(), -1);
          }
          binaryFile.seek(pageLocation * pageSize - pageSize + 4);
          writePageHeader(lastPage + 2, false, 0, binaryFile.readInt());
          for (int i = 26; i < 50; i++) {
            binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
            binaryFile.seek(binaryFile.readUnsignedShort());
            writeCellInterior(lastPage + 2, binaryFile.readInt(), binaryFile.readInt(), -1);
          }
          writePageHeader(1, false, 0, lastPage + 2);

          writeCellInterior(1, lastPage + 1, y, lastPage + 2);
          lastPage += 2;

        } else {

          int x, y;
          binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * 25));
          binaryFile.seek(binaryFile.readUnsignedShort());
          x = binaryFile.readInt();
          y = binaryFile.readInt();
          binaryFile.seek(pageLocation * pageSize - pageSize + 4);
          writePageHeader(lastPage + 1, false, 0, binaryFile.readInt());
          binaryFile.seek(pageLocation * pageSize - pageSize + 4);
          binaryFile.writeInt(x);
          for (int i = 26; i < 50; i++) {
            binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * i));
            binaryFile.seek(binaryFile.readUnsignedShort());
            writeCellInterior(lastPage + 1, binaryFile.readInt(), binaryFile.readInt(), -1);

          }

          binaryFile.seek(pageLocation * pageSize - pageSize + 1);
          binaryFile.write(25);

          int lastInteriorPage = routeOfLeafPage.remove(routeOfLeafPage.size() - 1);

          writeCellInterior(lastInteriorPage, pageLocation, y, lastPage + 1);
          lastPage++;

        }
      }

    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

  public void deleteCellInterior(int pageLocation, int pageNumber) {
    try {
      binaryFile.seek(pageLocation * pageSize - pageSize + 1);
      short No_OfCells = binaryFile.readByte();
      int pos = No_OfCells;
      for (int i = 0; i < No_OfCells; i++) {
        binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * (i)));
        binaryFile.seek(binaryFile.readUnsignedShort());
        if (pageNumber == binaryFile.readInt()) {
          pos = i;
          binaryFile.seek(pageLocation * pageSize - pageSize + 1);
          binaryFile.write(No_OfCells - 1);
          break;
        }
      }
      int temp;
      while (pos < No_OfCells) {
        binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * (pos + 1)));
        temp = binaryFile.readUnsignedShort();
        binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * (pos)));
        binaryFile.writeShort(temp);
        pos++;
      }
      temp = 0;
      for (int i = 0; i < No_OfCells - 1; i++) {
        binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * (i)));
        if (temp < binaryFile.readUnsignedShort()) {
          binaryFile.seek((pageLocation * pageSize - pageSize + 8) + (2 * (i)));
          temp = binaryFile.readUnsignedShort();
        }
      }
      binaryFile.seek(pageLocation * pageSize - pageSize + 2);
      binaryFile.writeShort(temp);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

  public void createEmptyTable() {

    try {
      currentPage = 1;
      binaryFile.setLength(0);

      binaryFile.setLength(pageSize);

      writePageHeader(currentPage, true, 0, -1);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");
    }

  }

  public void createNewTableLeaf(Map<String, ArrayList<String>> token) {
    try {
      currentPage = 1;
      binaryFile.setLength(0);

      binaryFile.setLength(pageSize);

      writePageHeader(currentPage, true, 0, -1);

      long no_of_Bytes = payloadSizeInBytes(token);

      long cellStartOffset = (currentPage * (pageSize)) - (no_of_Bytes + 6);

      writeCell(currentPage, token, cellStartOffset, no_of_Bytes);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");
    }
  }

  public int getNextMaxRowID() {
    currentPage = 1;
    searchRightMostLeafNode();
    readPageHeader(currentPage);
    try {
      binaryFile.seek(pageHeader_Offset_noOfCells);
      int noOfCells = binaryFile.readUnsignedByte();
      binaryFile.seek(pageHeader_array_offset + (2 * (noOfCells - 1)));
      long address = binaryFile.readUnsignedShort();
      binaryFile.seek(address);
      binaryFile.readShort();
      return binaryFile.readInt();

    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");
    }
    return -1;

  }

  public LinkedHashMap<String, ArrayList<String>> getSchema(String tableName) {
    ArrayList<String> vall = new ArrayList<String>();
    vall.add("1");
    vall.add("TEXT");
    vall.add(tableName);
    List<LinkedHashMap<String, ArrayList<String>>> output = searchNonPrimaryCol(vall);

    LinkedHashMap<String, ArrayList<String>> finalResult = new LinkedHashMap<String, ArrayList<String>>();

    for (LinkedHashMap<String, ArrayList<String>> map : output) {
      ArrayList<String> val = map.get("column_name");
      String key = val.get(0);

      ArrayList<String> valuee = new ArrayList<String>();

      ArrayList<String> dataTypeList = map.get("data_type");
      String dataType = dataTypeList.get(0);
      valuee.add(dataType);

      ArrayList<String> nullStringList = map.get("is_nullable");
      String isNull = nullStringList.get(0);
      if (isNull.equalsIgnoreCase("yes"))
        valuee.add("NULL");
      else
        valuee.addAll(nullStringList);

      finalResult.put(key, valuee);
    }

    return finalResult;

  }

  public boolean isPrimaryKeyExists(int newKey) {

    currentPage = 1;

    int rowId = newKey;
    searchLeafPage(rowId, false);
    readPageHeader(currentPage);
    long[] result = getCellOffset(rowId);
    long cellOffset = result[1];
    if (cellOffset > 0) {
      try {
        binaryFile.seek(cellOffset);
        binaryFile.readUnsignedShort();
        int actualRowID = binaryFile.readInt();
        // System.out.println("Row id is " + actualRowID);
        if (actualRowID == rowId) {

          return true;
        }

      } catch (IOException e) {
        // TODO Auto-generated catch block
        System.out.println("Unexpected Error");
      }

    } else {
      return false;
    }

    return false;
  }

  public LinkedHashMap<String, ArrayList<String>> searchWithPrimaryKey(LinkedHashMap<String, ArrayList<String>> token) {

    currentPage = 1;

    LinkedHashMap<String, ArrayList<String>> value = null;
    int rowId = Integer.parseInt(token.get(tableKey).get(1));
    searchLeafPage(rowId, false);
    readPageHeader(currentPage);
    long[] result = getCellOffset(rowId);
    long cellOffset = result[1];
    if (cellOffset > 0) {
      try {
        binaryFile.seek(cellOffset);
        binaryFile.readUnsignedShort();
        int actualRowID = binaryFile.readInt();
        if (actualRowID == rowId) {

          if (isColumnSchema) {
            token = new LinkedHashMap<String, ArrayList<String>>();
            token.put("rowid", null);
            token.put("table_name", null);
            token.put("column_name", null);
            token.put("data_type", null);
            token.put("ordinal_position", null);
            token.put("is_nullable", null);
          } else if (isTableSchema) {
            token = new LinkedHashMap<String, ArrayList<String>>();
            token.put("rowid", null);
            token.put("table_name", null);

          } else {
            token = mDBColumnFiletree.getSchema(tableName);
          }

          value = populateData(cellOffset, token);

        }

      } catch (IOException e) {
        // TODO Auto-generated catch block
        System.out.println("Unexpected Error");
      }

      return value;

    } else {
      System.out.println(" No rows matches");
      return null;
    }

  }

  public boolean deleteRecord(LinkedHashMap<String, ArrayList<String>> token) {
    currentPage = 1;
    boolean isDone = false;

    int rowId = Integer.parseInt(token.get(tableKey).get(1));
    searchLeafPage(rowId, false);
    readPageHeader(currentPage);
    long[] retVal = getCellOffset(rowId);
    long cellOffset = retVal[1];
    if (cellOffset > 0) {
      try {
        binaryFile.seek(cellOffset);
        binaryFile.readUnsignedShort();
        int actualRowID = binaryFile.readInt();
        // System.out.println("Row id is " + actualRowID);
        if (actualRowID == rowId) {

          binaryFile.seek(pageHeader_Offset_startOfCell);
          long startOfCell = binaryFile.readUnsignedShort();
          if (cellOffset == startOfCell) {

            binaryFile.seek(cellOffset);
            int payLoadSize = binaryFile.readUnsignedShort();
            binaryFile.seek(pageHeader_Offset_startOfCell);
            binaryFile.writeShort((int) (startOfCell - payLoadSize - 6));

          }

          binaryFile.seek(pageHeader_Offset_noOfCells);

          int No_OfCells = binaryFile.readUnsignedByte();

          int temp;
          long pos = retVal[0];
          while (pos < No_OfCells) {
            binaryFile.seek((currentPage * pageSize - pageSize + 8) + (2 * (pos + 1)));
            temp = binaryFile.readUnsignedShort();
            binaryFile.seek((currentPage * pageSize - pageSize + 8) + (2 * (pos)));
            binaryFile.writeShort(temp);
            pos++;
          }

          binaryFile.seek(pageHeader_Offset_noOfCells);
          int col = binaryFile.readUnsignedByte();
          binaryFile.seek(pageHeader_Offset_noOfCells);
          binaryFile.writeByte(--col);
          if (col == 0) {

            binaryFile.seek(pageHeader_Offset_startOfCell);
            binaryFile.writeShort((int) (currentPage * pageSize));

          }
          isDone = true;
        } else {

          System.out.println("No row matches");
        }

      } catch (IOException e) {
        // TODO Auto-generated catch block
        System.out.println("Unexpected Error");
      }

    } else {
      System.out.println(" No rows matches");
    }
    return isDone;
  }

  private void searchLeftMostLeafNode() {

    // TODO Auto-generated method stub

    routeOfLeafPage.add(currentPage);
    readPageHeader(currentPage);
    if (isLeafPage) {

      routeOfLeafPage.remove(routeOfLeafPage.size() - 1);
      return;
    } else {
      try {
        binaryFile.seek(pageHeader_Offset_noOfCells);

        int noOfColumns = binaryFile.readUnsignedByte();

        binaryFile.seek(pageHeader_array_offset);
        int address;
        if (noOfColumns > 0) {

          address = binaryFile.readUnsignedShort();

          binaryFile.seek(address);
          int pageNumber = binaryFile.readInt();

          currentPage = pageNumber;
          searchLeftMostLeafNode();

        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        System.out.println("Unexpected Error");
      }
    }

  }

  private void searchRightMostLeafNode() {

    // TODO Auto-generated method stub

    routeOfLeafPage.add(currentPage);
    readPageHeader(currentPage);
    if (isLeafPage) {

      routeOfLeafPage.remove(routeOfLeafPage.size() - 1);
      return;
    } else {
      try {
        binaryFile.seek(pageHeader_Offset_rightPagePointer);

        currentPage = binaryFile.readInt();

        searchRightMostLeafNode();

      } catch (IOException e) {
        // TODO Auto-generated catch block
        System.out.println("Unexpected Error");
      }
    }

  }

  public List<LinkedHashMap<String, ArrayList<String>>> searchNonPrimaryCol(ArrayList<String> value) {
    currentPage = 1;
    List<LinkedHashMap<String, ArrayList<String>>> result = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
    searchLeftMostLeafNode();
    while (currentPage > 0) {
      try {
        readPageHeader(currentPage);
        searchRecordsInTheCurrentPage(value, result);
        // printRecordsInTheCurrentPage(result);

        binaryFile.seek(pageHeader_Offset_rightPagePointer);

        currentPage = binaryFile.readInt();

      } catch (Exception e) {
        System.out.println("Unexpected Error");
      }
    }

    return result;

  }

  public List<LinkedHashMap<String, ArrayList<String>>> printAll() {
    currentPage = 1;
    List<LinkedHashMap<String, ArrayList<String>>> result = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
    searchLeftMostLeafNode();
    while (currentPage > 0) {
      try {
        readPageHeader(currentPage);
        printRecordsInTheCurrentPage(result);

        binaryFile.seek(pageHeader_Offset_rightPagePointer);

        currentPage = binaryFile.readInt();

      } catch (Exception e) {
        // TODO Auto-generated catch block
        System.out.println("Unexpected Error");
      }
    }
    return result;

  }

  private void printRecordsInTheCurrentPage(List<LinkedHashMap<String, ArrayList<String>>> result) throws Exception {
    binaryFile.seek(pageHeader_Offset_noOfCells);
    int noOfCol = binaryFile.readUnsignedByte();

    binaryFile.seek(pageHeader_array_offset);
    long point = binaryFile.getFilePointer();
    int address = binaryFile.readUnsignedShort();

    for (int i = 0; i < noOfCol; i++) {

      binaryFile.seek(address);

      binaryFile.readUnsignedShort();
      int currentRowID = binaryFile.readInt();

      LinkedHashMap<String, ArrayList<String>> token = null;
      if (isColumnSchema) {
        token = new LinkedHashMap<String, ArrayList<String>>();
        token.put("rowid", null);
        token.put("table_name", null);
        token.put("column_name", null);
        token.put("data_type", null);
        token.put("ordinal_position", null);
        token.put("is_nullable", null);
      } else if (isTableSchema) {
        token = new LinkedHashMap<String, ArrayList<String>>();
        token.put("rowid", null);
        token.put("table_name", null);

      } else {
        token = mDBColumnFiletree.getSchema(tableName);
      }

      result.add(populateData(address, token));

      point = (point + 2);
      binaryFile.seek(point);
      address = binaryFile.readUnsignedShort();

    }

  }

  private void searchRecordsInTheCurrentPage(ArrayList<String> searchCond,
      List<LinkedHashMap<String, ArrayList<String>>> result) throws Exception {
    binaryFile.seek(pageHeader_Offset_noOfCells);
    int noOfCol = binaryFile.readUnsignedByte();

    binaryFile.seek(pageHeader_array_offset);
    long point = binaryFile.getFilePointer();
    int address = binaryFile.readUnsignedShort();

    for (int i = 0; i < noOfCol; i++) {

      binaryFile.seek(address);

      binaryFile.readUnsignedShort();
      int currentRowID = binaryFile.readInt();
      LinkedHashMap<String, ArrayList<String>> token = null;
      if (isColumnSchema) {
        token = new LinkedHashMap<String, ArrayList<String>>();
        token.put("rowid", null);
        token.put("table_name", null);
        token.put("column_name", null);
        token.put("data_type", null);
        token.put("ordinal_position", null);
        token.put("is_nullable", null);
      } else if (isTableSchema) {
        token = new LinkedHashMap<String, ArrayList<String>>();
        token.put("rowid", null);
        token.put("table_name", null);

      } else {
        token = mDBColumnFiletree.getSchema(tableName);
      }
      token = populateDataWithSearch(searchCond, address, token);
      if (token != null)
        result.add(token);

      point = (point + 2);
      binaryFile.seek(point);
      address = binaryFile.readUnsignedShort();

    }

  }

  private LinkedHashMap<String, ArrayList<String>> populateDataWithSearch(ArrayList<String> searchCond, long cellOffset,
      LinkedHashMap<String, ArrayList<String>> token) {
    // TODO Auto-generated method stub

    ArrayList<String> arrayOfValues = new ArrayList<String>();
    try {
      binaryFile.seek(cellOffset);
      int payLoadSize = binaryFile.readUnsignedShort();
      Integer actualRowID = binaryFile.readInt();
      short noOfColumns = binaryFile.readByte();
      payLoadSize -= 1;
      long offsetForSerialType = binaryFile.getFilePointer();
      long offSetForData = (offsetForSerialType + noOfColumns);

      boolean isMatch = false;
      int i = 0;

      String seachCol = searchCond.get(0);
      String searchDataType = searchCond.get(1);
      String serachVal = searchCond.get(2);

      String value = null;

      long offsetForSerialTypeMatch = offsetForSerialType;
      long offSetForDataMatch = (offSetForData);

      int colIndex = Integer.parseInt(seachCol);

      int currentColIndex = 1;

      for (String key : token.keySet()) {

        binaryFile.seek(offsetForSerialType);
        short b = binaryFile.readByte();
        offsetForSerialType = binaryFile.getFilePointer();
        if (b == 0) {

          binaryFile.seek(offSetForData);
          int p = (binaryFile.readUnsignedByte());
          value = "NULL";
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));

        } else if (b == 1) {

          binaryFile.seek(offSetForData);
          int p = (binaryFile.readUnsignedShort());
          value = "NULL";
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));

        } else if (b == 2) {
          binaryFile.seek(offSetForData);
          int p = (binaryFile.readInt());
          value = "NULL";
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else if (b == 3) {

          binaryFile.seek(offSetForData);
          int p = (int) (binaryFile.readDouble());
          value = "NULL";
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));

        } else if (b == 12) {
          value = "NULL";

        } else if (b == 4) {
          binaryFile.seek(offSetForData);
          value = Integer.toString(binaryFile.readUnsignedByte());
          offSetForData = binaryFile.getFilePointer();
        } else if (b == 5) {
          binaryFile.seek(offSetForData);
          value = (Integer.toString(binaryFile.readUnsignedShort()));
          offSetForData = binaryFile.getFilePointer();
        } else if (b == 6) {
          binaryFile.seek(offSetForData);
          value = (Integer.toString(binaryFile.readInt()));
          offSetForData = binaryFile.getFilePointer();
        } else if (b == 7) {
          binaryFile.seek(offSetForData);
          value = (Long.toString(binaryFile.readLong()));
          offSetForData = binaryFile.getFilePointer();
        } else if (b == 8) {

          binaryFile.seek(offSetForData);
          value = (Float.toString(binaryFile.readFloat()));
          offSetForData = binaryFile.getFilePointer();
        } else if (b == 9) {

          binaryFile.seek(offSetForData);
          value = (Double.toString(binaryFile.readDouble()));
          offSetForData = binaryFile.getFilePointer();

        } else if (b == 10) {
          binaryFile.seek(offSetForData);

          long timeInEpoch = binaryFile.readLong();

          value = Long.toString(timeInEpoch);
          offSetForData = binaryFile.getFilePointer();
        } else if (b == 11) {
          binaryFile.seek(offSetForData);

          long timeInEpoch = binaryFile.readLong();
          value = Long.toString(timeInEpoch);

          // value = (Long.toString(binaryFile.readLong()));
          offSetForData = binaryFile.getFilePointer();
        } else {
          byte[] text = new byte[b - 12];
          binaryFile.seek(offSetForData);

          binaryFile.read(text);
          value = (new String(text));
          offSetForData = binaryFile.getFilePointer();

        }

        if (currentColIndex == colIndex) {

          switch (searchDataType.trim().toLowerCase()) {

          case "tinyint":
            if (value == null && value == serachVal) {
              isMatch = true;
            } else if (value != null && value.equalsIgnoreCase("null") && value.equalsIgnoreCase(serachVal)) {
              isMatch = true;
            } else if (value != null && serachVal != null && !value.equalsIgnoreCase("null")
                && Integer.parseInt(serachVal) == Integer.parseInt(value)) {
              isMatch = true;
            }
            break;
          case "smallint":
            if (value == null && value == serachVal) {
              isMatch = true;
            } else if (value != null && value.equalsIgnoreCase("null") && value.equalsIgnoreCase(serachVal)) {
              isMatch = true;
            } else if (value != null && serachVal != null && !value.equalsIgnoreCase("null")
                && Integer.parseInt(serachVal) == Integer.parseInt(value)) {
              isMatch = true;
            }
            break;
          case "int":
            if (value == null && value == serachVal) {
              isMatch = true;
            } else if (value != null && value.equalsIgnoreCase("null") && value.equalsIgnoreCase(serachVal)) {
              isMatch = true;
            } else if (value != null && serachVal != null && !value.equalsIgnoreCase("null")
                && Integer.parseInt(serachVal) == Integer.parseInt(value)) {
              isMatch = true;
            }
            break;
          case "bigint":
            if (value == null && value == serachVal) {
              isMatch = true;
            } else if (value != null && value.equalsIgnoreCase("null") && value.equalsIgnoreCase(serachVal)) {
              isMatch = true;
            } else if (value != null && serachVal != null && !value.equalsIgnoreCase("null")
                && Long.parseLong(serachVal) == Long.parseLong(value)) {
              isMatch = true;
            }
            break;
          case "real":
            if (value == null && value == serachVal) {
              isMatch = true;
            } else if (value != null && value.equalsIgnoreCase("null") && value.equalsIgnoreCase(serachVal)) {
              isMatch = true;
            } else if (value != null && serachVal != null && !value.equalsIgnoreCase("null")
                && Float.parseFloat(serachVal) == Float.parseFloat(value)) {
              isMatch = true;
            }
            break;
          case "double":
            if (value == null && value == serachVal) {
              isMatch = true;
            } else if (value != null && value.equalsIgnoreCase("null") && value.equalsIgnoreCase(serachVal)) {
              isMatch = true;
            } else if (value != null && serachVal != null && !value.equalsIgnoreCase("null")
                && Double.parseDouble(serachVal) == Double.parseDouble(value)) {
              isMatch = true;
            }
            break;
          case "datetime":
            long epochSeconds = 0;

            if (value != null && value.equalsIgnoreCase("null") && value.equalsIgnoreCase(serachVal)) {
              isMatch = true;
              break;
            }

            if (value != null && !value.equalsIgnoreCase("null")) {
              SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

              Date date;
              try {
                date = df.parse(serachVal);

                ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), zoneId);

                epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
              } catch (Exception e) {

              }

            }

            if (value == null && value == serachVal) {
              isMatch = true;
            } else if (value != null && serachVal != null && !value.equalsIgnoreCase("null")
                && (epochSeconds) == Long.parseLong(value)) {

              Instant ii = Instant.ofEpochSecond(epochSeconds);
              ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
              SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
              Date date = Date.from(zdt2.toInstant());
              value = sdf.format(date);

              isMatch = true;
            }
            break;
          case "date":
            long epochSecondss = 0;
            if (value != null && value.equalsIgnoreCase("null") && value.equalsIgnoreCase(serachVal)) {
              isMatch = true;
              break;
            }

            if (value != null && !value.equalsIgnoreCase("null")) {
              SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

              Date date;
              try {
                date = df.parse(serachVal);

                ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), zoneId);

                epochSecondss = zdt.toInstant().toEpochMilli() / 1000;
              } catch (Exception e) {

              }

            }

            if (value == null && value == serachVal) {
              isMatch = true;
            } else if (value != null && serachVal != null && !value.equalsIgnoreCase("null")
                && (epochSecondss) == Long.parseLong(value)) {

              Instant ii = Instant.ofEpochSecond(epochSecondss);
              ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
              SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
              Date date = Date.from(zdt2.toInstant());
              value = sdf.format(date);
              isMatch = true;
            }
            break;
          case "text":
            if (value == null && value == serachVal) {
              isMatch = true;
            } else if (value != null && serachVal != null && serachVal.equalsIgnoreCase(value)) {
              isMatch = true;
            }

            break;
          }

          break;
        }
        currentColIndex++;
      }

      if (isMatch) {
        offsetForSerialType = offsetForSerialTypeMatch;
        offSetForData = offSetForDataMatch;

        for (String key : token.keySet()) {

          if (i == 0) {
            arrayOfValues.add(actualRowID.toString());
            token.put(key, new ArrayList<String>(arrayOfValues));
            i++;
            arrayOfValues.clear();
            continue;
          }

          binaryFile.seek(offsetForSerialType);
          short b = binaryFile.readByte();
          offsetForSerialType = binaryFile.getFilePointer();
          if (b == 0) {

            binaryFile.seek(offSetForData);
            int p = (binaryFile.readUnsignedByte());
            arrayOfValues.add("NULL");
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));

          } else if (b == 1) {

            binaryFile.seek(offSetForData);
            int p = (binaryFile.readUnsignedShort());
            arrayOfValues.add("NULL");
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));

          } else if (b == 2) {
            binaryFile.seek(offSetForData);
            int p = (binaryFile.readInt());
            arrayOfValues.add("NULL");
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));
          } else if (b == 3) {

            binaryFile.seek(offSetForData);
            int p = (int) (binaryFile.readDouble());
            arrayOfValues.add("NULL");
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));

          } else if (b == 12) {
            arrayOfValues.add("NULL");
            token.put(key, new ArrayList<String>(arrayOfValues));
          } else if (b == 4) {
            binaryFile.seek(offSetForData);
            arrayOfValues.add(Integer.toString(binaryFile.readUnsignedByte()));
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));
          } else if (b == 5) {
            binaryFile.seek(offSetForData);
            arrayOfValues.add(Integer.toString(binaryFile.readUnsignedShort()));
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));
          } else if (b == 6) {
            binaryFile.seek(offSetForData);
            arrayOfValues.add(Integer.toString(binaryFile.readInt()));
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));
          } else if (b == 7) {
            binaryFile.seek(offSetForData);
            arrayOfValues.add(Long.toString(binaryFile.readLong()));
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));
          } else if (b == 8) {

            binaryFile.seek(offSetForData);
            arrayOfValues.add(Float.toString(binaryFile.readFloat()));
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));
          } else if (b == 9) {

            binaryFile.seek(offSetForData);
            arrayOfValues.add(Double.toString(binaryFile.readDouble()));
            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));

          } else if (b == 10) {
            binaryFile.seek(offSetForData);

            Instant ii = Instant.ofEpochSecond(binaryFile.readLong());
            ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
            Date date = Date.from(zdt2.toInstant());
            arrayOfValues.add(sdf.format(date));

            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));
          } else if (b == 11) {
            binaryFile.seek(offSetForData);
            // arrayOfValues.add(Long.toString(binaryFile.readLong()));

            Instant ii = Instant.ofEpochSecond(binaryFile.readLong());
            ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = Date.from(zdt2.toInstant());

            arrayOfValues.add(sdf.format(date));

            offSetForData = binaryFile.getFilePointer();
            token.put(key, new ArrayList<String>(arrayOfValues));
          } else {
            byte[] text = new byte[b - 12];
            binaryFile.seek(offSetForData);

            binaryFile.read(text);
            arrayOfValues.add(new String(text));
            offSetForData = binaryFile.getFilePointer();

            token.put(key, new ArrayList<String>(arrayOfValues));

          }
          arrayOfValues.clear();
        }
      }

      if (!isMatch)
        token = null;

    } catch (Exception e) {
      System.out.println("Unexpected Error");
    }

    return token;
  }

  private LinkedHashMap<String, ArrayList<String>> populateData(long cellOffset,
      LinkedHashMap<String, ArrayList<String>> token) {
    // TODO Auto-generated method stub

    ArrayList<String> arrayOfValues = new ArrayList<String>();
    try {
      binaryFile.seek(cellOffset);
      int payLoadSize = binaryFile.readUnsignedShort();
      Integer actualRowID = binaryFile.readInt();
      short noOfColumns = binaryFile.readByte();
      payLoadSize -= 1;
      long offsetForSerialType = binaryFile.getFilePointer();
      long offSetForData = (offsetForSerialType + noOfColumns);
      int i = 0;
      for (String key : token.keySet()) {

        if (i == 0) {
          arrayOfValues.add(actualRowID.toString());
          token.put(key, new ArrayList<String>(arrayOfValues));
          i++;
          arrayOfValues.clear();
          continue;
        }

        binaryFile.seek(offsetForSerialType);
        short b = binaryFile.readByte();
        offsetForSerialType = binaryFile.getFilePointer();

        if (b == 0) {

          binaryFile.seek(offSetForData);
          int p = (binaryFile.readUnsignedByte());
          arrayOfValues.add("NULL");
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));

        } else if (b == 1) {

          binaryFile.seek(offSetForData);
          int p = (binaryFile.readUnsignedShort());
          arrayOfValues.add("NULL");
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));

        } else if (b == 2) {
          binaryFile.seek(offSetForData);
          int p = (binaryFile.readInt());
          arrayOfValues.add("NULL");
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else if (b == 3) {

          binaryFile.seek(offSetForData);
          int p = (int) (binaryFile.readDouble());
          arrayOfValues.add("NULL");
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));

        } else if (b == 12) {
          arrayOfValues.add("NULL");
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else if (b == 4) {
          binaryFile.seek(offSetForData);
          arrayOfValues.add(Integer.toString(binaryFile.readUnsignedByte()));
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else if (b == 5) {
          binaryFile.seek(offSetForData);
          arrayOfValues.add(Integer.toString(binaryFile.readUnsignedShort()));
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else if (b == 6) {
          binaryFile.seek(offSetForData);
          arrayOfValues.add(Integer.toString(binaryFile.readInt()));
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else if (b == 7) {
          binaryFile.seek(offSetForData);
          arrayOfValues.add(Long.toString(binaryFile.readLong()));
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else if (b == 8) {

          binaryFile.seek(offSetForData);
          arrayOfValues.add(Float.toString(binaryFile.readFloat()));
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else if (b == 9) {

          binaryFile.seek(offSetForData);
          arrayOfValues.add(Double.toString(binaryFile.readDouble()));
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));

        } else if (b == 10) {
          binaryFile.seek(offSetForData);
          // arrayOfValues.add(Long.toString(binaryFile.readLong()));

          long timeInEpoch = binaryFile.readLong();
          Instant ii = Instant.ofEpochSecond(timeInEpoch);
          ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
          Date date = Date.from(zdt2.toInstant());
          arrayOfValues.add(sdf.format(date));

          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else if (b == 11) {
          binaryFile.seek(offSetForData);
          long timeInEpoch = binaryFile.readLong();
          Instant ii = Instant.ofEpochSecond(timeInEpoch);
          ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

          Date date = Date.from(zdt2.toInstant());
          arrayOfValues.add(sdf.format(date));
          offSetForData = binaryFile.getFilePointer();
          token.put(key, new ArrayList<String>(arrayOfValues));
        } else {
          byte[] text = new byte[b - 12];
          binaryFile.seek(offSetForData);

          binaryFile.read(text);
          arrayOfValues.add(new String(text));
          offSetForData = binaryFile.getFilePointer();

          token.put(key, new ArrayList<String>(arrayOfValues));

        }
        arrayOfValues.clear();
      }

    } catch (Exception e) {
      System.out.println("Unexpected Error");
    }

    return token;
  }

  private long[] getCellOffset(int rowId) {
    // TODO Auto-generated method stub
    long[] retVal = new long[2];
    int cellOffset = -1;
    try {
      binaryFile.seek(pageHeader_Offset_noOfCells);

      int noOfColumns = binaryFile.readUnsignedByte();

      binaryFile.seek(pageHeader_array_offset);
      long point = binaryFile.getFilePointer();
      int address = binaryFile.readUnsignedShort();
      for (int i = 0; i < noOfColumns; i++) {

        binaryFile.seek(address);

        binaryFile.readUnsignedShort();
        int currentRowID = binaryFile.readInt();

        if (rowId == currentRowID) {
          cellOffset = address;
          retVal[0] = i;
          retVal[1] = cellOffset;
          return retVal;

        } else {

          point = (point + 2);
          binaryFile.seek(point);
          address = binaryFile.readUnsignedShort();
        }

      }

    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");
    }

    return retVal;
  }

  public boolean isEmptyTable() throws IOException {
    return binaryFile.length() == 0;
  }

  public void insertNewRecord(Map<String, ArrayList<String>> token) throws Exception {
    currentPage = 1;
    int rowId = -1;
    if (isColumnSchema || isTableSchema) {
      tableKey = "rowid";
    }
    rowId = Integer.parseInt(token.get(tableKey).get(1));
    if (rowId < 0)
      throw new Exception("Insertion fails");

    searchLeafPage(rowId, false);

    insertNewRecordInPage(token, rowId, currentPage);

    routeOfLeafPage.clear();

  }

  private void insertNewRecordInPage(Map<String, ArrayList<String>> token, int rowId, int pageNumber) {
    readPageHeader(pageNumber);

    long no_of_Bytes = payloadSizeInBytes(token);
    long cellStartOffset = 0;
    try {
      binaryFile.seek(pageHeader_Offset_startOfCell);

      cellStartOffset = ((long) binaryFile.readUnsignedShort()) - (no_of_Bytes + 6);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");
    }
    if (cellStartOffset < pageHeader_offset + 2) {

      LinkedList<byte[]> page1Cells = new LinkedList<>();
      LinkedList<byte[]> page2Cells = new LinkedList<>();
      try {
        binaryFile.seek(pageHeader_Offset_noOfCells);
        int no_of_Cells = binaryFile.readUnsignedByte();

        int splitCells = no_of_Cells / 2;
        int loc = 0;
        // long point = pageHeader_array_offset;
        splitCells = 1;

        long point = pageHeader_offset - 2;

        binaryFile.seek(point);

        binaryFile.seek(binaryFile.readUnsignedShort());

        binaryFile.readUnsignedShort();

        int currenRowID = binaryFile.readInt();
        while ((currenRowID > rowId)) {
          splitCells++;
          point = point - 2;
          binaryFile.seek(point);
          binaryFile.seek(binaryFile.readUnsignedShort());
          binaryFile.readUnsignedShort();
          currenRowID = binaryFile.readInt();

        }

        if (point == pageHeader_offset - 2) {
          splitCells = 0;
          // No need of split the current page
          if (currentPage == 1) {
            point = pageHeader_array_offset;
            for (int i = 1; i <= no_of_Cells; i++) {

              binaryFile.seek(point);
              loc = binaryFile.readUnsignedShort();

              binaryFile.seek(point);
              binaryFile.writeShort(0);

              point = binaryFile.getFilePointer();

              binaryFile.seek(loc);
              binaryFile.readUnsignedShort();

              binaryFile.seek(loc);
              byte[] cell = readCell(loc);

              page1Cells.add(cell);

              // byte[] c= page1Cells.getFirst();
              // for(byte bb : c) {
              // System.out.println((String.format("%02x", bb)));
              // }

            }

          }

        } else {
          // split the page

          if (currentPage == 1) {
            point = pageHeader_array_offset;
            for (int i = 1; i <= no_of_Cells - splitCells; i++) {

              binaryFile.seek(point);
              loc = binaryFile.readUnsignedShort();

              binaryFile.seek(point);
              binaryFile.writeShort(0);

              point = binaryFile.getFilePointer();

              binaryFile.seek(loc);
              binaryFile.readUnsignedShort();

              binaryFile.seek(loc);
              byte[] cell = readCell(loc);

              page1Cells.add(cell);

              // byte[] c= page1Cells.getFirst();
              // for(byte bb : c) {
              // System.out.println((String.format("%02x", bb)));
              // }

            }
          }

          for (int i = splitCells; i <= 1; i--) {

            point = pageHeader_offset - (2 * i);
            binaryFile.seek(point);
            loc = binaryFile.readUnsignedShort();

            binaryFile.seek(point);
            binaryFile.writeShort(0);

            // point = binaryFile.getFilePointer();

            // binaryFile.seek(loc);
            // binaryFile.readUnsignedShort();
            // rowIdMiddle = binaryFile.readInt();

            binaryFile.seek(loc);
            byte[] cell = readCell(loc);

            page2Cells.add(cell);

            // byte[] c= page1Cells.getFirst();
            // for(byte bb : c) {
            // System.out.println((String.format("%02x", bb)));
            // }

          }
        }

        int rowIdMiddle = 0;
        if (currenRowID > rowId) {
          rowIdMiddle = currenRowID;
        } else {
          rowIdMiddle = rowId;
        }
        // for (int i = 1; i <= splitCells; i++) {
        //
        // binaryFile.seek(point);
        // loc = binaryFile.readUnsignedShort();
        //
        // binaryFile.seek(point);
        // binaryFile.writeShort(0);
        //
        // point = binaryFile.getFilePointer();
        //
        // binaryFile.seek(loc);
        // binaryFile.readUnsignedShort();
        // rowIdMiddle = binaryFile.readInt();
        //
        //
        // binaryFile.seek(loc);
        // byte[] cell = readCell(loc);
        //
        // page1Cells.add(cell);
        //
        // // byte[] c= page1Cells.getFirst();
        // // for(byte bb : c) {
        // // System.out.println((String.format("%02x", bb)));
        // // }
        //
        // }

        if (splitCells > 0) {
          binaryFile.seek(pageHeader_Offset_noOfCells);
          int noOfcells = binaryFile.readUnsignedByte();
          binaryFile.seek(pageHeader_Offset_noOfCells);
          binaryFile.writeByte(noOfcells - splitCells);
        }

        // for (int i = splitCells + 1; i <= no_of_Cells; i++) {
        //
        // binaryFile.seek(point);
        // loc = binaryFile.readUnsignedShort();
        //
        // binaryFile.seek(point);
        // binaryFile.writeShort(0);
        //
        // point = binaryFile.getFilePointer();
        // binaryFile.seek(loc);
        // byte[] cell = readCell(loc);
        // page2Cells.add(cell);
        //
        // }

        // split the page;
        int[] pageNumbers = splitLeafPage(page1Cells, page2Cells);

        // write Right Sibling for both pages
        binaryFile.seek(((pageNumbers[0] * pageSize) - pageSize) + 4);
        int prevRight = binaryFile.readInt();
        binaryFile.seek(((pageNumbers[0] * pageSize) - pageSize) + 4);
        binaryFile.writeInt(pageNumbers[1]);
        binaryFile.seek(((pageNumbers[1] * pageSize) - pageSize) + 4);
        binaryFile.writeInt(prevRight);

        // ch16PageFileExample.displayBinaryHex(binaryFile);

        // Interior Page Logic

        if (routeOfLeafPage.size() > 0 && routeOfLeafPage.get(routeOfLeafPage.size() - 1) > 0) {

          // if interior page exist
          writeCellInterior(routeOfLeafPage.remove(routeOfLeafPage.size() - 1), pageNumbers[0], rowIdMiddle,
              pageNumbers[1]);

        } else {
          // create new interior page
          currentPage = 1;
          createNewInterior(pageNumbers[0], rowIdMiddle, pageNumbers[1]);

        }

        if (rowId < rowIdMiddle) {
          currentPage = pageNumbers[0];
        } else {
          currentPage = pageNumbers[1];
        }
        insertNewRecordInPage(token, rowId, currentPage);

      } catch (IOException e) {
        // TODO Auto-generated catch block
        System.out.println("Unexpected Error");

      }

      // ch16PageFileExample.displayBinaryHex(binaryFile);
      // System.exit(0);

    } else {

      writeCell(currentPage, token, cellStartOffset, no_of_Bytes);
    }
  }

  private boolean searchLeafPage(int rowId, boolean isFound) {
    // TODO Auto-generated method stub

    routeOfLeafPage.add(currentPage);
    readPageHeader(currentPage);
    if (isLeafPage) {

      routeOfLeafPage.remove(routeOfLeafPage.size() - 1);
      return true;
    } else {
      try {
        binaryFile.seek(pageHeader_Offset_noOfCells);

        int noOfColumns = binaryFile.readUnsignedByte();

        binaryFile.seek(pageHeader_array_offset);
        long currentArrayElementOffset = binaryFile.getFilePointer();
        int address;
        for (int i = 0; i < noOfColumns; i++) {
          binaryFile.seek(currentArrayElementOffset);
          address = binaryFile.readUnsignedShort();
          currentArrayElementOffset = binaryFile.getFilePointer();
          binaryFile.seek(address);
          int pageNumber = binaryFile.readInt();
          int delimiterRowId = binaryFile.readInt();
          if (rowId < delimiterRowId) {
            currentPage = pageNumber;
            isFound = searchLeafPage(rowId, false);

            break;
          }
        }

        if (!isFound) {
          binaryFile.seek(pageHeader_Offset_rightPagePointer);
          currentPage = binaryFile.readInt();
          isFound = searchLeafPage(rowId, false);
        }

      } catch (IOException e) {
        // TODO Auto-generated catch block
        System.out.println("Unexpected Error");
      }
      return isFound;
    }

  }

  private byte[] readCell(int loc) {
    // TODO Auto-generated method stub

    try {
      binaryFile.seek(loc);

      int payloadLength = binaryFile.readUnsignedShort();

      byte[] b = new byte[6 + payloadLength];
      binaryFile.seek(loc);

      binaryFile.read(b);
      binaryFile.seek(loc);
      binaryFile.write(new byte[6 + payloadLength]);

      return b;
    } catch (Exception e) {
      System.out.println("Unexpected Error");
    }

    return null;

  }

  private int[] splitLeafPage(LinkedList<byte[]> page1Cells, LinkedList<byte[]> page2Cells) {
    // TODO Auto-generated method stub

    int[] pageNumbers = new int[2];

    // addexisting Page
    try {
      if (currentPage != 1) {
        pageNumbers[0] = currentPage;
        pageHeader_offset = pageHeader_array_offset;
        if (page1Cells.size() > 0) {
          binaryFile.seek(pageHeader_Offset_startOfCell);
          binaryFile.writeShort(currentPage * (pageSize));
        }
        for (byte[] s : page1Cells) {

          long cellStartOffset = 0;

          binaryFile.seek(pageHeader_Offset_startOfCell);

          cellStartOffset = ((long) binaryFile.readUnsignedShort()) - (s.length);
          writeCellInBytes(currentPage, s, cellStartOffset);

        }
      } else {

        // create new Page
        lastPage += 1;

        pageNumbers[0] = lastPage;
        currentPage = lastPage;
        createPage(page1Cells);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");

    }

    // create new Page
    lastPage += 1;

    pageNumbers[1] = lastPage;
    currentPage = lastPage;
    createPage(page2Cells);
    return pageNumbers;

  }

  // token<ColumnName, <data_type,value>>
  private void writeCell(int pageLocation, Map<String, ArrayList<String>> token, long cellStartOffset,
      long no_of_Bytes) {

    try {
      binaryFile.seek(pageHeader_Offset_startOfCell);
      binaryFile.writeShort((int) cellStartOffset);

      binaryFile.seek(pageHeader_Offset_noOfCells);
      short current_Cell_size = binaryFile.readByte();
      binaryFile.seek(pageHeader_Offset_noOfCells);
      binaryFile.write(current_Cell_size + 1);

    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    writeToHeaderArray(cellStartOffset, Integer.parseInt(token.get(tableKey).get(1)));
    try {
      binaryFile.seek(cellStartOffset);
      /**
       * Write cell header
       */
      int rowId_or_pageNo = Integer.parseInt(token.get(tableKey).get(1));
      binaryFile.writeShort((int) no_of_Bytes);
      binaryFile.writeInt(rowId_or_pageNo);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");
    }

    writeCellContent(pageLocation, token);
  }

  private void writeCellInBytes(int pageLocation, byte[] b, long cellStartOffset) {

    try {
      binaryFile.seek(pageHeader_Offset_startOfCell);
      binaryFile.writeShort((int) cellStartOffset);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    byte[] rowId = Arrays.copyOfRange(b, 2, 6);
    int id = java.nio.ByteBuffer.wrap(rowId).getInt();
    writeToHeaderArray(cellStartOffset, id);
    try {
      binaryFile.seek(cellStartOffset);
      binaryFile.write(b);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");
    }

  }

  private long payloadSizeInBytes(Map<String, ArrayList<String>> token) {
    long no_of_Bytes = 0;
    for (String key : token.keySet()) {
      if (key.equals(tableKey))
        continue; // Primary Key not needed in payload
      ArrayList<String> data_type = token.get(key);

      switch (data_type.get(0).trim().toLowerCase()) {
        case "tinyint":
          no_of_Bytes += 1;
          break;
        case "smallint":
          no_of_Bytes += 2;
          break;
        case "int":
          no_of_Bytes += 4;
          break;
        case "bigint":
          no_of_Bytes += 8;
          break;
        case "real":
          no_of_Bytes += 4;
          break;
        case "double":
          no_of_Bytes += 8;
          break;
        case "datetime":
          no_of_Bytes += 8;
          break;
        case "date":
          no_of_Bytes += 8;
          break;
        case "text":
          no_of_Bytes += data_type.get(1).length();
          break;
      }

    }
    // 14
    no_of_Bytes += token.size(); // 1-byte TINYINT for no of columns + n
    // byte serial-type-code
    return no_of_Bytes;
  }

  private void writeToHeaderArray(long cellStartOffset, int rowID) {
    // TODO Auto-generated method stub

    try {
      binaryFile.seek(pageHeader_Offset_noOfCells);
      int No_OfCells = binaryFile.readUnsignedByte();
      // pageHeader_offset = binaryFile.getFilePointer();

      int pos = 0;
      for (int i = 0; i < No_OfCells; i++) {
        binaryFile.seek((currentPage * pageSize - pageSize + 8) + (2 * i));
        binaryFile.seek(binaryFile.readUnsignedShort() + 2);
        if (rowID < binaryFile.readInt()) {
          pos = i;
          break;
        }
      }
      while (pos < No_OfCells) {
        binaryFile.seek((currentPage * pageSize - pageSize + 8) + (2 * (No_OfCells - 1)));
        binaryFile.writeShort(binaryFile.readUnsignedShort());
        No_OfCells--;
      }
      binaryFile.seek((currentPage * pageSize - pageSize + 8) + (2 * (pos)));
      binaryFile.writeShort((int) cellStartOffset);

    }

    catch (Exception e) {
      System.out.println("Unexpected Error");
    }
  }

  private void writeCellContent(int pageLocation2, Map<String, ArrayList<String>> token) {
    // TODO Auto-generated method stub
    try {

      binaryFile.write(token.size() - 1);// no of columns

      writeSerialCodeType(token);

      writePayload(token);
    } catch (Exception e) {
      System.out.println("Unexpected Error");
    }

  }

  private void writePayload(Map<String, ArrayList<String>> token) throws IOException, UnsupportedEncodingException {
    // Payload

    for (String key : token.keySet()) {
      if (key.equals(tableKey))
        continue; // primary key not needed

      ArrayList<String> data_type = token.get(key);

      switch (data_type.get(0).trim().toLowerCase()) {

      case "tinyint":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(Integer.parseInt(data_type.get(1)));
        } else {
          binaryFile.write(128);

        }

        break;
      case "smallint":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.writeShort(Integer.parseInt(data_type.get(1)));
        } else {
          binaryFile.writeShort(-1);
        }

        break;
      case "int":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.writeInt(Integer.parseInt(data_type.get(1)));
        } else {
          binaryFile.writeInt(-1);
        }
        break;
      case "bigint":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.writeLong(Long.parseLong(data_type.get(1)));
        } else {
          binaryFile.writeLong(-1);
        }

        break;
      case "real":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.writeFloat(Float.parseFloat((data_type.get(1))));
        } else {
          binaryFile.writeFloat(-1);
        }

        break;
      case "double":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.writeDouble(Double.parseDouble((data_type.get(1))));
        } else {
          binaryFile.writeDouble(-1);
        }

        break;
      case "datetime":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {

          SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");

          Date date;
          try {
            date = df.parse(data_type.get(1));

            ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), zoneId);

            long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;

            binaryFile.writeLong(epochSeconds);
          } catch (ParseException e) {
            // TODO Auto-generated catch block
            System.out.println("Unexpected Error");
          }
        } else {
          binaryFile.writeLong(-1);

        }

        break;
      case "date":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {

          SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd");

          Date date;
          try {
            date = d.parse(data_type.get(1));

            ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(), zoneId);

            long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;

            binaryFile.writeLong(epochSeconds);
          } catch (ParseException e) {
            // TODO Auto-generated catch block
            System.out.println("Unexpected Error");
          }
        } else {
          binaryFile.writeLong(-1);

        }

        break;
      case "text":
        if (data_type.get(1) != null) {
          String s = data_type.get(1);
          byte[] b = s.getBytes("UTF-8");
          for (byte bb : b)
            binaryFile.write(bb);
        }

        break;

      }

    }
  }

  private void writeSerialCodeType(Map<String, ArrayList<String>> token) throws IOException {
    // n - bytes Serial code Types , one for each column
    for (String key : token.keySet()) {
      if (key.equals(tableKey))
        continue; // primary key not needed
      ArrayList<String> data_type = token.get(key);

      switch (data_type.get(0).trim().toLowerCase()) {

      case "tinyint":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(4);
        } else {
          binaryFile.write(0);
        }
        break;
      case "smallint":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(5);
        } else {
          binaryFile.write(1);
        }
        break;
      case "int":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(6);
        } else {
          binaryFile.write(2);
        }
        break;
      case "bigint":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(7);
        } else {
          binaryFile.write(3);
        }
        break;
      case "real":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(8);
        } else {
          binaryFile.write(2);
        }
        break;
      case "double":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(9);
        } else {
          binaryFile.write(3);
        }
        break;
      case "datetime":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(10);
        } else {
          binaryFile.write(3);
        }
        break;
      case "date":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(11);
        } else {
          binaryFile.write(3);
        }
        break;
      case "text":
        if (data_type.get(1) != null && !data_type.get(1).trim().equalsIgnoreCase("null")) {
          binaryFile.write(12 + (data_type.get(1).length()));
        } else {
          binaryFile.write(12);
        }
        break;

      }

    }
  }

  /**
   * FYI: Header format - https://sqlite.org/fileformat2.html
   */
  private void writePageHeader(int pageLocation, boolean isLeaf, int no_of_Cells, int rightPage) {
    // TODO Auto-generated method stub

    try {

      binaryFile.seek(pageLocation * pageSize - pageSize);

      if (isLeaf) {
        binaryFile.write(13);

        pageHeader_Offset_noOfCells = binaryFile.getFilePointer();
        binaryFile.write(no_of_Cells);

        pageHeader_Offset_startOfCell = binaryFile.getFilePointer();
        binaryFile.writeShort((int) (pageLocation * pageSize));

        pageHeader_Offset_rightPagePointer = binaryFile.getFilePointer();

        binaryFile.writeInt(-1);

        pageHeader_array_offset = binaryFile.getFilePointer();
        pageHeader_offset = pageHeader_array_offset;
      } else {
        binaryFile.write(5);
        pageHeader_Offset_noOfCells = binaryFile.getFilePointer();
        binaryFile.write(0);
        pageHeader_Offset_startOfCell = binaryFile.getFilePointer();
        binaryFile.writeShort((int) (pageLocation * pageSize));
        binaryFile.writeInt(rightPage);

        pageHeader_array_offset = binaryFile.getFilePointer();
        pageHeader_offset = pageHeader_array_offset;
      }

    }

    catch (Exception e) {
      System.out.println("Unexpected Error");

    }
  }

  private void readPageHeader(int pageLocation) {
    try {

      binaryFile.seek((currentPage * pageSize) - pageSize);

      int flag = binaryFile.readUnsignedByte();

      if (flag == 13)
        isLeafPage = true;
      else
        isLeafPage = false;

      pageHeader_Offset_noOfCells = ((currentPage * pageSize) - pageSize) + 1;
      int noOfCells = binaryFile.readUnsignedByte();
      pageHeader_Offset_startOfCell = binaryFile.getFilePointer();
      binaryFile.readUnsignedShort();
      pageHeader_Offset_rightPagePointer = binaryFile.getFilePointer();
      binaryFile.readInt();
      pageHeader_array_offset = binaryFile.getFilePointer();
      pageHeader_offset = binaryFile.getFilePointer() + (2 * noOfCells);

    } catch (Exception e) {
      System.out.println("Unexpected Error" + e.getMessage());

    }

  }

  public boolean close() {
    try {
      binaryFile.close();
      return true;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");
      return false;
    }
  }

  private void createPage(LinkedList<byte[]> pageCells) {
    try {
      binaryFile.setLength(pageSize * currentPage);
      writePageHeader(currentPage, true, pageCells.size(), -1);
      readPageHeader(currentPage);

      pageHeader_offset = pageHeader_array_offset;
      ListIterator<byte[]> iterator = pageCells.listIterator(pageCells.size());

      long cellStartOffset = 0;

      binaryFile.seek(pageHeader_Offset_startOfCell);
      binaryFile.writeShort(currentPage * (pageSize));
      while (iterator.hasPrevious()) {
        byte[] s = iterator.previous();

        binaryFile.seek(pageHeader_Offset_startOfCell);

        cellStartOffset = ((long) binaryFile.readUnsignedShort()) - (s.length);
        writeCellInBytes(currentPage, s, cellStartOffset);

      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      System.out.println("Unexpected Error");
    }
  }

  public String getPrimaryKey() {
    return tableKey;
  }

}
