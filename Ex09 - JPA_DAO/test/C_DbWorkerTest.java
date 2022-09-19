
import app.beans.Departement;
import app.beans.Localite;
import app.beans.Personne;
import app.exceptions.MyDBException;
import app.helpers.DateTimeLib;
import app.helpers.SystemLib;
import app.workers.DbWorker;
import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test des principales fonctionnalités de DbWorker.
 *
 * @author Jean-Claude Stritt
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class C_DbWorkerTest {

    static final String NOM_DOSSIER_DATA = "data";
    static final String NOM_FICHIER_LOC = "npa_ch_20140818_utf8.txt";
    static final String NOM_FICHIER_DEP = "departements_utf8.txt";

    private static DbWorker dbWrk;
    private static Personne p = null;

    @BeforeClass
    public static void setUpClass() throws MyDBException {
        dbWrk = new DbWorker();

    }

    @AfterClass
    public static void tearDownClass() throws MyDBException {
        dbWrk.fermerBD();
    }

    private void afficherMessage(boolean ok, String msg1, String msg2) {
        if (ok) {
            System.out.println("  " + msg1);
        } else {
            System.out.println("  " + msg2);
        }
    }

    private Personne preparerNouvellePersonne() {
        Personne p = new Personne();
        p.setPkPers(null);
        p.setNom("Federer");
        p.setPrenom("Roger");
        p.setDatenaissance(DateTimeLib.createDate(8, 8, 1981));
        p.setNorue(15);
        p.setRue("Freiburgerstrasse");
        p.setActif(true);
        p.setSalaire(BigDecimal.valueOf(50000000d));
        p.setFkDep(null);
        p.setFkLoc(null);
        return p;
    }

    @Test
    public void a_testerLirePersonnes() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        List<Personne> personnes = dbWrk.lirePersonnes();
        boolean ok = !personnes.isEmpty();
        if (ok) {
            int cnt = 0;
            for (Personne personne : personnes) {
                cnt++;
                System.out.println("  " + cnt + " - " + personne);
            }
        }
        assertTrue(ok);
    }

    @Test
    public void b_testerCompterPersonnes() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        long nbPers = dbWrk.compterPersonnes();
        boolean ok = nbPers > 0;
        afficherMessage(ok, "Nb de personnes = " + nbPers, "Nb de personnes = " + nbPers);
        assertTrue(ok);
    }

    @Test
    public void c_testerAjouterPersonne() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        p = preparerNouvellePersonne();
        dbWrk.ajouterPersonne(p);
    }

    @Test
    public void d_testerLirePersonne() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        Personne p2 = dbWrk.lirePersonne(p);
    }

    @Test
    public void e_testerModifierPersonne() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        p.setPrenom("Stan");
        dbWrk.modifierPersonne(p);
    }

    @Test
    public void f_testerEffacerPersonne() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        dbWrk.effacerPersonne(p);
    }

    @Test
    public void g_testerLireLocalites() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        List<Localite> localites = dbWrk.lireLocalites();
        boolean ok = !localites.isEmpty();
        if (ok) {
            int cnt = 0;
            for (Localite loc : localites) {
                cnt++;
                if (cnt < 10) {
                    System.out.println("  " + cnt + ". " + loc);
                }
            }
        }
        assertTrue(ok);
    }

    @Test
    public void h_testerCompterLocalites() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        long nbLoc = dbWrk.compterLocalites();
        boolean ok = nbLoc > 0;
        afficherMessage(ok,
                "Nb de localités = " + nbLoc,
                "Nb de localités = " + nbLoc);
        assertTrue(ok);
    }

    @Test
    public void i_testChargerEtSauverLocalites() throws Exception {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");

        // on prepare l'accès vers le fichier
        Path cheminDossier = Paths.get(NOM_DOSSIER_DATA).toAbsolutePath();
        String nomFic = cheminDossier + File.separator + NOM_FICHIER_LOC;
        File ficLoc = new File(nomFic);

        // on charge depuis le fichier texte et on sauve dans la BD
        int n1 = dbWrk.lireEtSauverLocalites(ficLoc, "UTF-8");

        // on vérifie
        long n2 = dbWrk.compterLocalites();
        System.out.println("  n1=" + n1 + ", n2=" + n2);
        boolean ok = n1 > 0 && n2 > 0;
        afficherMessage(ok, n2 + " localités présents dans la BD",
                "Erreur de lecture du fichier " + NOM_FICHIER_LOC + " !!!");
        assertTrue(ok);
    }

    @Test
    public void j_testerLireDepartements() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        List<Departement> departements = dbWrk.lireDepartements();
        boolean ok = !departements.isEmpty();
        if (ok) {
            int cnt = 0;
            for (Departement dep : departements) {
                cnt++;
                System.out.println("  " + cnt + ". " + dep);
            }
        }
        assertTrue(ok);
    }

    @Test
    public void k_testerCompterDepartements() throws MyDBException {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");
        long nbDep = dbWrk.compterDepartements();
        boolean ok = nbDep > 0;
        afficherMessage(ok,
                "Nb de départements = " + nbDep,
                "Nb de départements = " + nbDep);
        assertTrue(ok);
    }

    @Test
    public void l_testChargerEtSauverDepartements() throws Exception {
        System.out.println("*** " + SystemLib.getCurrentMethod() + " ...");

        // on prepare l'accès vers le fichier
        Path cheminDossier = Paths.get(NOM_DOSSIER_DATA).toAbsolutePath();
        String nomFic = cheminDossier + File.separator + NOM_FICHIER_DEP;
        File ficDep = new File(nomFic);

        // on charge depuis le fichier texte et on sauve dans la BD
        int n1 = dbWrk.lireEtSauverDepartements(ficDep, "UTF-8");

        // on vérifie
        long n2 = dbWrk.compterLocalites();
        boolean ok = n1 > 0 && n2 > 0;
        afficherMessage(ok, n2 + " départements présents dans la BD",
                "Erreur de lecture du fichier " + NOM_FICHIER_DEP + " !!!");
        assertTrue(ok);
    }
}
