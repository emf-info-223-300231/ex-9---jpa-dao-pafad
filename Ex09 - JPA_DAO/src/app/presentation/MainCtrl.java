package app.presentation;


import app.exceptions.MyDBException;
import app.helpers.DateTimeLib;
import app.helpers.JfxPopup;
import app.workers.DbWorker;
import app.workers.PersonneManager;
import java.io.File;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.application.Platform;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.DatePicker;
import javafx.scene.control.TextField;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

/**
 *
 * @author PA
 */
public class MainCtrl implements Initializable {

    final static private String PU = "PU_MYSQL";

    private DbWorker dbWrk;
    private PersonneManager persMan;
    private boolean modeAjout;

    @FXML
    private TextField txtNom;
    @FXML
    private TextField txtPrenom;
    @FXML
    private TextField txtPK;
    @FXML
    private TextField txtNo;
    @FXML
    private TextField txtRue;
    @FXML
    private TextField txtSalaire;
    @FXML
    private CheckBox ckbActif;
    @FXML
    private Button btnDebut;
    @FXML
    private Button btnPrevious;
    @FXML
    private Button btnNext;
    @FXML
    private Button btnEnd;
    @FXML
    private Button btnSauver;
    @FXML
    private Button btnAnnuler;
    @FXML
    private DatePicker dateNaissance;
    @FXML
    private ComboBox<?> cbxLocalite;
    @FXML
    private ComboBox<?> cbxDepartement;

    @Override
    public void initialize(URL url, ResourceBundle rb) {
        // Créer le worker et connexion à la db intégrée
        try {
            dbWrk = new DbWorker();
        } catch (MyDBException ex) {
            JfxPopup.displayError("ERREUR", null, ex.getMessage());
            quitter();
        }

        persMan = new PersonneManager();

        setBtnDeplaceVisible(true);

        if (dbWrk.estConnecte()) {
            try {
                persMan.setListe(dbWrk.lirePersonnes());
                afficherPersonne(persMan.personneCourante());

                cbxLocalite.getItems().addAll(dbWrk.lireLocalites());
                cbxDepartement.getItems().addAll(dbWrk.lireDepartements());
            } catch (MyDBException ex) {
                JfxPopup.displayError("ERREUR", null, ex.getMessage());
                quitter();
            }
        } else {
            JfxPopup.displayError("ERREUR", "Une erreur s'est produite", "");
            quitter();
        }
    }

    @FXML
    private void actionPrevious(ActionEvent event) {
        afficherPersonne(persMan.personnePrecedente());
    }

    @FXML
    private void actionNext(ActionEvent event) {
        afficherPersonne(persMan.personneSuivante());
    }

    @FXML
    private void actionEnd(ActionEvent event) {
        afficherPersonne(persMan.personneFin());
    }

    @FXML
    private void debut(ActionEvent event) {
        afficherPersonne(persMan.personneDebut());
    }

    public void quitter() {
        dbWrk.fermerBD(); // ne pas oublier !!!
        Platform.exit();
    }

    @FXML
    private void menuAjouter(ActionEvent event) {
        modeAjout = true;
        setBtnDeplaceVisible(false);
        effaceChamps();
    }

    @FXML
    private void menuModifier(ActionEvent event) {
        modeAjout = false;
        setBtnDeplaceVisible(false);
    }

    @FXML
    private void menuEffacer(ActionEvent event) {
        try {
            dbWrk.effacerPersonne(persMan.personneCourante());
            persMan.setListe(dbWrk.lirePersonnes());
            afficherPersonne(persMan.personneCourante());
        } catch (MyDBException ex) {
            JfxPopup.displayError("ERREUR", null, ex.getMessage());
        }
    }

    @FXML
    private void menuQuitter(ActionEvent event) {
        quitter();
    }

    /**
     * Cache ou affiche les boutons.
     *
     * @param b boolean
     */
    private void setBtnDeplaceVisible(boolean b) {
        btnDebut.setVisible(b);
        btnPrevious.setVisible(b);
        btnNext.setVisible(b);
        btnEnd.setVisible(b);
        btnAnnuler.setVisible(!b);
        btnSauver.setVisible(!b);
    }

    @FXML
    private void annulerPersonne(ActionEvent event) {
        setBtnDeplaceVisible(true);
        afficherPersonne(persMan.personneCourante());
    }

    @FXML
    private void sauverPersonne(ActionEvent event) {
        try {
            if (modeAjout) {
                Personne p = lirePersonne(new Personne());
                dbWrk.ajouterPersonne(p);
                persMan.setListe(dbWrk.lirePersonnes());
                afficherPersonne(persMan.personneFin());
            } else {
                Personne p = lirePersonne(persMan.personneCourante());
                dbWrk.modifierPersonne(p);
            }
            // on remet à jour la liste (pour avoir les info de maj version et date)
            persMan.setListe(dbWrk.lirePersonnes());
            afficherPersonne(persMan.personneCourante());

        } catch (MyDBException ex) {
            JfxPopup.displayError("ERREUR", null, ex.getMessage());
        }

        setBtnDeplaceVisible(true);
    }

    private void afficherPersonne(Personne p) {
        if (p == null) {
            return;
        }
        txtPK.setText(String.valueOf(p.getPkPers()));
        txtPrenom.setText(p.getPrenom());
        txtNom.setText(p.getNom());
        dateNaissance.setValue(DateTimeLib.dateToLocalDate(p.getDatenaissance()));
        txtNo.setText(String.valueOf(p.getNorue()));
        txtRue.setText(p.getRue());
        cbxDepartement.setValue(p.getFkDep());
        cbxLocalite.setValue(p.getFkLoc());
        txtSalaire.setText(String.valueOf(p.getSalaire()));
        ckbActif.setSelected(p.getActif());
    }

    private Personne lirePersonne(Personne p) {
        if (p == null) {
            p = new Personne();
        }
        p.setPrenom(txtPrenom.getText());
        p.setNom(txtNom.getText());
        p.setDatenaissance(DateTimeLib.localDateToDate(dateNaissance.getValue()));
        p.setRue(txtRue.getText());
        cbxDepartement.getSelectionModel();
        p.setFkDep(cbxDepartement.getValue());
        p.setFkLoc(cbxLocalite.getValue());
        p.setActif(ckbActif.isSelected());

        try {
            p.setNorue(Integer.parseInt(txtNo.getText()));
        } catch (NumberFormatException e) {
            p.setNorue(0);
        }
        try {
            p.setSalaire(new BigDecimal(txtSalaire.getText()));
        } catch (Exception e) {
            p.setSalaire(new BigDecimal(0.0));
        }
        return p;
    }

    private void effaceChamps() {
        txtPK.setText("");
        txtPrenom.setText("");
        txtNom.setText("");
        dateNaissance.setValue(null);
        txtNo.setText("");
        txtRue.setText("");
        cbxDepartement.getSelectionModel().select(null);
        cbxLocalite.getSelectionModel().select(null);
        txtSalaire.setText("");
        ckbActif.setSelected(false);
    }

    @FXML
    private void menuChargerLocalites(ActionEvent event) {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Sélectionnez le fichier des localits");
        Path path = Paths.get("data").toAbsolutePath();
        chooser.setInitialDirectory(path.toFile());
        File file = chooser.showOpenDialog(new Stage());
        if (file != null) {

            long t0 = System.currentTimeMillis();
            try {
                int nb = dbWrk.lireEtSauverLocalites(file, "UTF-8");
                long t1 = System.currentTimeMillis();
                String mess = "Les " + nb + " localités ont été chargées en " + (t1 - t0) / 1000. + " s";
                JfxPopup.displayInformation("INFORMATION", "Lecture et chargement des localités", mess);
            } catch (Exception ex) {
                JfxPopup.displayError("ERREUR", null, ex.getMessage());
            }

        }
    }

    @FXML
    private void menuChargerDepartement(ActionEvent event) {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Sélectionnez le fichier des départements");
        Path path = Paths.get("data").toAbsolutePath();
        chooser.setInitialDirectory(path.toFile());
        File file = chooser.showOpenDialog(new Stage());
        if (file != null) {

            long t0 = System.currentTimeMillis();
            try {
                int nb = dbWrk.lireEtSauverDepartements(file, "UTF-8");

                long t1 = System.currentTimeMillis();
                String mess = "Les " + nb + " départements ont été chargées en " + (t1 - t0) / 1000. + " s";
                JfxPopup.displayInformation("INFORMATION", "Lecture et chargement des départements", mess);
            } catch (Exception ex) {
                JfxPopup.displayError("ERREUR", null, ex.getMessage());
            }
        }
    }

    @FXML
    private void menuRechercher(ActionEvent event) {
        String nomARechercher = JfxPopup.askInfo("Recherche", "Rechercher une personne avec le son nom", "Insérer le nom à rechercher");
    }
}
