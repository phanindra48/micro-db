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
}
