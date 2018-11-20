import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.lang.Math.*;
import static java.lang.System.out;


/**
 *
 * @author Chris Irwin Davis
 * @version 1.0
 */
public class DavisBaseBinaryFileExample {

	/* This static variable controls page size. */
	static int pageSizePower = 9;
	/* This strategy insures that the page size is always a power of 2. */
	static int pageSize = (int)Math.pow(2, pageSizePower);


	public static void main(String[] args) {

		out.println("The database table file page size is: " + getPageSize());

		/* This method will initialize the database storage if it doesn't exit */
		initializeDataStore();

		/* Create some user table. An actual NEW user table would begin with a single page.
		 * However, this example demonstrates how to add/remove increments of pages to/from files.
		 *
		 * Whenever the length of a RandomAccessFile is increased, the added space is padded
		 * with 0x00 value bytes.
		 */
		String fileName = "some_user_table.tbl";
		RandomAccessFile binaryFile;
		try {
			binaryFile = new RandomAccessFile("data/" + fileName, "rw");

			/* Initialize the file size to be zero */
			binaryFile.setLength(0);
			out.println("The file is now " + binaryFile.length() + " bytes long");
			out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			out.println();

			/* Increase the file size to be 1024, i.e. 2 x 512B */
			binaryFile.setLength(pageSize * 2);
			out.println("The file is now " + binaryFile.length() + " bytes long");
			out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			out.println();

			/* Increase the file size to be 1536, i.e. 3 x 512B */
			binaryFile.setLength(pageSize * 3);
			out.println("The file is now " + binaryFile.length() + " bytes long");
			out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			out.println();

			/* Re-locate the address pointer at the beginning of page 1 and write
			 * 0x05 (b-tree interior node) to the first header byte */
			binaryFile.seek(0);
			binaryFile.writeByte(0x05);

			/* Re-locate the address pointer at the beginning of page 2 and write
			 * 0x0D (b-tree leaf node) to the first header byte */
			binaryFile.seek(pageSize * 1);
			binaryFile.writeByte(0x0D);

			/* Re-locate the address pointer at the beginning of page 3 and write
			 * 0x0D (b-tree leaf node) to the first header byte */
			binaryFile.seek(pageSize * 2);
			binaryFile.writeByte(0x0D);

			/* Increase the size of the binaryFile by exactly one page, regardless of how
			 * long it currently is. The new bytes will be appended to the end and be all zeros */
			binaryFile.setLength(binaryFile.length() + pageSize);
			out.println("The file is now " + binaryFile.length() + " bytes long");
			out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			out.println();


			// HexDump.displayBinaryHex(binaryFile, headerEveryNBytes);
			HexDump.displayBinaryHex(binaryFile, 512);
			binaryFile.close();

		}
		catch (Exception e) {
			out.println("Unable to open " + fileName);
		}

	}



	/**
	 * This static method creates the DavisBase data storage container
	 * and then initializes two .tbl files to implement the two
	 * system tables, davisbase_tables and davisbase_columns
	 *
	 *  WARNING! Calling this method will destroy the system database
	 *           catalog files if they already exist.
	 */
	static void initializeDataStore() {

		/** Create data directory at the current OS location to hold */
		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]);
				anOldFile.delete();
			}
		}
		catch (SecurityException se) {
			out.println("Unable to create data container directory");
			out.println(se);
		}

		/** Create davisbase_tables system catalog */
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			/* Initially, the file is one page in length */
			davisbaseTablesCatalog.setLength(pageSize);
			/* Set file pointer to the beginnning of the file */
			davisbaseTablesCatalog.seek(0);
			/* Write 0x0D to the page header to indicate that it's a leaf page.
			 * The file pointer will automatically increment to the next byte. */
			davisbaseTablesCatalog.write(0x0D);
			/* Write 0x00 (although its value is already 0x00) to indicate there
			 * are no cells on this page */
			davisbaseTablesCatalog.write(0x00);
			davisbaseTablesCatalog.close();
		}
		catch (Exception e) {
			out.println("Unable to create the database_tables file");
			out.println(e);
		}

		/** Create davisbase_columns systems catalog */
		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			/** Initially the file is one page in length */
			davisbaseColumnsCatalog.setLength(pageSize);
			davisbaseColumnsCatalog.seek(0);       // Set file pointer to the beginnning of the file
			/* Write 0x0D to the page header to indicate a leaf page. The file
			 * pointer will automatically increment to the next byte. */
			davisbaseColumnsCatalog.write(0x0D);
			/* Write 0x00 (although its value is already 0x00) to indicate there
			 * are no cells on this page */
			davisbaseColumnsCatalog.write(0x00);
			davisbaseColumnsCatalog.close();
		}
		catch (Exception e) {
			out.println("Unable to create the database_columns file");
			out.println(e);
		}
	}

	static int getPageSize() {
		return pageSize;
	}
}
