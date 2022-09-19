package app.workers.dao;

import app.exceptions.MyFileException;
import app.helpers.BeanExtracter;
import app.helpers.SystemLib;
import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Implémentation de la couche DAO pour les fichiers "texte" d'après l'API FileDaoAPI.
 *
 * @author Jean-Claude Stritt
 */
public class FileDao<E> implements FileDaoItf<E> {

  private BeanExtracter<E> extracteur;

  public FileDao(BeanExtracter<E> extracteur) {
    this.extracteur = extracteur;
  }

  /**
   * Lecture d'un fichier texte spécifié.
   * @param fichier
   * @param nomCharset
   * @return 
   * @throws app.exceptions.MyFileException
   */
  @Override
  public List<E> lireFichierTexte(File fichier, String nomCharset) throws MyFileException {
    List<E> ar = new ArrayList<>();

    // prepare le bon charset (UTF8 ou autre) d'après le nom fourni
    Charset cs = Charset.forName(nomCharset);
    // ouverture du fichier et lecture
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fichier), cs))) {
      String line;
      while ((line = reader.readLine()) != null) {
        E e = extracteur.textToBean(line);
        ar.add(e);
      }
    } catch (IOException ex) {
      throw new MyFileException(SystemLib.getFullMethodName(), ex.getMessage(), false);
    } 
    return ar;
  }

}
