package app.exceptions;

/**
 *
 * @author PA
 */
public class MyFileException extends Exception {

  private boolean open;

  /**
   *
   * @param method class and method name
   * @param msg error or other information
   * @param open error during the opening time (true if yes)
   */
  public MyFileException(String method, String msg, boolean open) {
    super("ERROR in: " + method + "\n" + msg);
    this.open = open;
  }

  @Override
  public String toString() {
    return super.toString() + message();
  }

  @Override
  public String getMessage() {
    return super.getMessage() + message();
  }

  private String message() {
    return open ? "\nFile problem at the opening time" : "\nFile problem at the reading/writing time";
  }

}
