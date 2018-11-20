import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.lang.Math.*;
import java.io.IOException;
import static java.lang.System.out;

/**
 *
 * @author Chris Irwin Davis
 * @version 1.0
 */
public class HexDump {

  public static void main(String[] args) {
    out.println("HexDump\n(c)2018 Chris Irwin Davis");
    // if(args.length)
    try {
      RandomAccessFile file = new RandomAccessFile(args[0], "r");
      displayBinaryHex(file, 512);
    } catch (IOException e) {
      out.println(e);
    }
  }

  /**
   * <p>
   * This method is used for debugging.
   *
   * @param ram is an instance of {@link RandomAccessFile}.
   *            <p>
   *            This method will display the binary contents of the file to
   *            Stanard Out (stdout)
   */
  static void displayBinaryHex(RandomAccessFile ram, int pageSize) {
    try {
      out.println("Dec\tHex\t 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F");
      ram.seek(0);
      long size = ram.length();
      int row = 1;
      out.print("0000\t0x0000\t");
      while (ram.getFilePointer() < size) {
        out.print(String.format("%02X ", ram.readByte()));
        // out.print(ram.readByte() + " ");
        /* Print the page header */
        if (row % pageSize == 0) {
          out.println();
          out.print("Dec\tHex\t 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F");
        }
        /* Print line header */
        if (row % 16 == 0) {
          out.println();
          out.print(String.format("%04d\t0x%04X\t", row, row));
        }
        row++;
      }
      out.println();
    } catch (IOException e) {
      out.println(e);
    }
  }
}
