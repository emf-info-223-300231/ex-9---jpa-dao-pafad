package app.workers;

import app.exceptions.MyDBException;
import java.io.File;
import java.util.List;

/**
 * Interface qui définit les services métier nécessaires avec la base de données
 * liée.
 *
 * @author ramalhom
 */
public interface DbWorkerItf {

    // gestion des personnes
    List<Personne> lirePersonnes() throws MyDBException;

    long compterPersonnes() throws MyDBException;

    void ajouterPersonne(Personne p) throws MyDBException;

    Personne lirePersonne(Personne p) throws MyDBException;

    void modifierPersonne(Personne p) throws MyDBException;

    void effacerPersonne(Personne p) throws MyDBException;
    
    Personne rechercherPersonneAvecNom(String nomARechercher) throws MyDBException;

    // gestion des localités
    List<Localite> lireLocalites() throws MyDBException;

    long compterLocalites() throws MyDBException;

    int lireEtSauverLocalites(File fichier, String nomCharset) throws Exception;

    // gestion des départements
    List<Departement> lireDepartements() throws MyDBException;

    long compterDepartements() throws MyDBException;

    int lireEtSauverDepartements(File fichier, String nomCharset) throws Exception;

    // autres
    void fermerBD();

    boolean estConnecte();
}
