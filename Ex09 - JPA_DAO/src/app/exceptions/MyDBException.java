package app.exceptions;

/**
 *
 * @author PA
 */
public class MyDBException extends Exception {

  /**
   * @param method class and method name
   * @param msg error or other information
   */
  public MyDBException(String method, String msg) {
    super("ERROR JDBC/JPA in: " + method + "\n" + msg);
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public String getMessage() {
    return super.getMessage();
  }

}
