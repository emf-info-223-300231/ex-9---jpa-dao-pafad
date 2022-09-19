package app.workers.extracters;

import app.beans.Departement;
import app.helpers.BeanExtracter;
import app.helpers.DateTimeLib;
import java.util.Date;

/**
 * Extracteur d'une ligne de texte vers un bean "Departement".
 *
 * @author Jean-Claude Stritt
 */
public class DepartementExtracter implements BeanExtracter<Departement> {
  private String sep;

  public DepartementExtracter( String sep ) {
    this.sep = sep;
  }

  @Override
  public Departement textToBean( String text ) {
    String[] tab = text.split(sep);
    Departement d = new Departement();
    if (tab.length > 2) {
      d.setDepartement(tab[0]);
      d.setLocalite(tab[1]);
      Date date = DateTimeLib.stringToSqldate(text);
      d.setDateCreation(date);
    }
    return d;
  }
}
