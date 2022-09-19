package app.workers.dao;
import app.exceptions.MyFileException;
import java.io.File;
import java.util.List;

/**
 * Définit les services minimaux de la couche DAO pour la lecture de
 * fichiers "texte" contenant des données tabulaires.
 *
 * @author Jean-Claude Stritt
 */
public interface FileDaoItf<E> {

  public List<E> lireFichierTexte( File fichier, String nomCharset ) throws MyFileException;

}
