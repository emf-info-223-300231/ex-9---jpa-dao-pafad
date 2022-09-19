import app.beans.Departement;
import app.beans.Localite;
import app.exceptions.MyDBException;
import app.exceptions.MyFileException;
import app.helpers.SystemLib;
import app.workers.dao.FileDao;
import app.workers.extracters.DepartementExtracter;
import app.workers.extracters.LocaliteExtracter;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import app.workers.dao.FileDaoItf;


/**
 * Classe de test des méthodes de lecture de fichiers "texte"
 * avec séparateur d'information (type csv, txt).
 *
 * @author Jean-Claude Stritt
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class A_FileDaoTest {
  static final String NOM_DOSSIER_DATA = "data";
  static final String NOM_FICHIER_LOC = "npa_ch_20140818_utf8.txt";
  static final String NOM_FICHIER_DEP = "departements_utf8.txt";

  @BeforeClass
  public static void setUpClass() throws Exception {
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
  }

  @Test
  public void a_testerLireFichierTexteLocalites() throws MyDBException, MyFileException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");

    // création de l'extracteur de localités
    LocaliteExtracter locExtr = new LocaliteExtracter("\t");

    // création du worker de fichier
    FileDaoItf<Localite> ficLocWrk = new FileDao<>(locExtr);

    // on prepare l'accès vers le fichier
    Path cheminDossier = Paths.get(NOM_DOSSIER_DATA).toAbsolutePath();
    String nomFic = cheminDossier + File.separator + NOM_FICHIER_LOC;
    File ficLoc = new File(nomFic);

    // lecture des localités
    List<Localite> localites = ficLocWrk.lireFichierTexte(ficLoc, "UTF-8");

    // assertion et affichage résultat
    boolean ok = !localites.isEmpty();
    if (ok) {
      System.out.println("  " + localites.size() + " localités lues !");
    } 
    assertTrue(ok);
  }

  @Test
  public void b_testerLireFichierTexteDepartements() throws MyDBException, MyFileException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");

    // création de l'extracteur de departements
    DepartementExtracter depExtr = new DepartementExtracter(";");

    // création du worker de fichier
    FileDaoItf<Departement> ficDepWrk = new FileDao<>(depExtr);

    // on prepare l'accès vers le fichier
    Path cheminDossier = Paths.get(NOM_DOSSIER_DATA).toAbsolutePath();
    String nomFic = cheminDossier + File.separator + NOM_FICHIER_DEP;
    File ficDep = new File(nomFic);

    // lecture des localités
    List<Departement> departements = ficDepWrk.lireFichierTexte(ficDep, "UTF-8");

    // assertion et affichage résultat
    boolean ok = !departements.isEmpty();
    if (ok) {
      System.out.println("  " + departements.size() + " départements lus !");
    } 
    assertTrue(ok);
  }

}
