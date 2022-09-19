
import app.exceptions.MyDBException;
import app.helpers.DateTimeLib;
import app.helpers.SystemLib;
import app.workers.dao.FileDao;
import app.workers.dao.JpaDao;
import app.workers.extracters.DepartementExtracter;
import app.workers.extracters.LocaliteExtracter;
import java.math.BigDecimal;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import static org.junit.Assert.assertTrue;
import app.workers.dao.FileDaoItf;
import app.workers.dao.JpaDaoItf;
import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * Test des principales fonctionnalités de la couche DAO sur une base de données MySql. Les données pour monter la base
 * se trouvent dans le dossier "data" de ce projet.
 *
 * @author Jean-Claude Stritt
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class B_JpaDaoTest {

  private static final String JPA_PU = "PU_MYSQL";
  static final String NOM_DOSSIER_DATA = "data";
  static final String NOM_FICHIER_LOC = "npa_ch_20140818_utf8.txt";
  static final String NOM_FICHIER_DEP = "departements_utf8.txt";

  private static JpaDaoItf<Personne, Integer> persWrk;
  private static JpaDaoItf<Localite, Integer> locWrk;
  private static JpaDaoItf<Departement, Integer> depWrk;
  private static FileDaoItf<Localite> ficLocWrk;
  private static FileDaoItf<Departement> ficDepWrk;

  private static int lastPK = -1;


  /*
   * SETUP ET TEARDOWN (METHODES AVANT ET APRES LES TESTS)
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    // ouvre les sous-workers de la couche dao
    persWrk = new JpaDao<>(JPA_PU, Personne.class);
    locWrk = new JpaDao<>(JPA_PU, Localite.class);
    depWrk = new JpaDao<>(JPA_PU, Departement.class);
    ficLocWrk = new FileDao<>(new LocaliteExtracter("\t"));
    ficDepWrk = new FileDao<>(new DepartementExtracter(";"));
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
  }


  /*
   * METHODES PRIVEES
   */
  private Personne getNewPersonne() {
    Personne p = new Personne();
    p.setNom("Federer");
    p.setPrenom("Rodger");
    p.setDatenaissance(DateTimeLib.createDate(8, 8, 1981));
    p.setNorue(13);
    p.setRue("Rue à Roger");
    p.setFkLoc(null);
    p.setActif(true);
    p.setSalaire(new BigDecimal(1000000.00));
    p.setDateModif(Timestamp.valueOf(LocalDateTime.now()));
    return p;
  }

  /*
   * TESTS
   */
  @Test
  public void a_testerEstConnectee() throws MyDBException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
    boolean ok = persWrk.estConnectee();
    if (ok) {
      System.out.println("  OK, connecté !");
    } else {
      System.out.println("  KO, pas connecté !");
    }
    assertTrue(ok);
  }

  @Test
  public void b_testerCreer() throws MyDBException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
    Personne p = getNewPersonne();
    persWrk.creer(p);
    lastPK = p.getPkPers();

  }

  @Test
  public void c_testerLire() throws MyDBException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
    boolean ok = lastPK >= 0;
    if (ok) {
      Personne p = persWrk.lire(lastPK);
      ok = p != null;
      System.out.println("  " + p);
    }
    assertTrue(ok);
  }

  @Test
  public void d_testerModifier() throws MyDBException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
    String NOM_TEST = "Durand";
    boolean ok = lastPK >= 0;
    if (ok) {
      Personne p1 = persWrk.lire(lastPK);
      String savedName = p1.getNom();
      p1.setNom(NOM_TEST);
      persWrk.modifier(p1);
      System.out.println("  " + p1);

      Personne p2 = persWrk.lire(p1.getPkPers());
      ok = (p2 != null) && p2.getNom().equals(NOM_TEST);
      if (ok) {
        p1.setNom(savedName);
        persWrk.modifier(p1);
      }
      System.out.println("  " + p1);

    }
    assertTrue(ok);
  }

  @Test
  public void e_testerEffacer() throws MyDBException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
    if (lastPK >= 0) {
      Personne p = persWrk.lire(lastPK);
      System.out.println("  " + p);
      persWrk.effacer(p.getPkPers());
    }

  }

  @Test
  public void f_testerCompter() throws MyDBException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
    long n = persWrk.compter();
    System.out.println("  n = " + n);
    boolean ok = n > 0;
    assertTrue(ok);
  }

  @Test
  public void g_testerRechercher() throws MyDBException {

  }

  @Test
  public void h_testerLireListe() throws MyDBException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
    List<Personne> personnes = persWrk.lireListe();
    boolean ok = !personnes.isEmpty();
    if (ok) {
      int cnt = 0;
      for (Personne p : personnes) {
        cnt++;
        System.out.println("  " + cnt + ". " + p);
        p.setFkLoc(null); // pour pouvoir tester ensuite l'effacement d'une liste complète
        p.setFkDep(null); // pour pouvoir tester ensuite l'effacement d'une liste complète
        persWrk.modifier(p);
      }
    }
    assertTrue(ok);
  }

  @Test
  public void z_testerDeconnecter() throws MyDBException {
    System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
    persWrk.deconnecter();
    locWrk.deconnecter();
    depWrk.deconnecter();
  }

}
