import java.util.ArrayList;
import java.util.Arrays;

public class Parser {
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

}
