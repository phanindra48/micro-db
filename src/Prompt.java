public class Prompt {
  private static final String version = "v0.1.0";
  private static final String copyright = "@2018 Phanindra Pydisetty";

  public static void splashScreen() {
    System.out.println(Utils.repeat("-", 80));
    System.out.println("Welcome to MicroDB Lite"); // Display the string.
    System.out.println("MicroDB Lite Version " + getVersion());
    System.out.println(getCopyright());
    System.out.println("\nType \"help;\" to display supported commands.");
    System.out.println(Utils.repeat("-", 80));
  }

  public static String getVersion() {
    return version;
  }

  public static String getCopyright() {
    return copyright;
  }

  public static void displayVersion() {
    System.out.println("MicroDB Lite Version " + getVersion());
    System.out.println(getCopyright());
  }
}
