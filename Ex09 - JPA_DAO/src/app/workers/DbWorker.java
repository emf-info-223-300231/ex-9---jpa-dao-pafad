package app.workers;

import app.beans.Departement;
import app.beans.Localite;
import app.beans.Personne;
import app.exceptions.MyDBException;
import app.workers.dao.FileDao;
import app.workers.dao.JpaDao;
import app.workers.extracters.DepartementExtracter;
import app.workers.extracters.LocaliteExtracter;
import java.io.File;
import java.util.List;
import app.workers.dao.FileDaoItf;
import app.workers.dao.JpaDaoItf;

/**
 * Couche "métier" gérant les accès de et vers la base de données.
 *
 * @author ramalhom
 */
public class DbWorker implements DbWorkerItf {

    private static final String JPA_PU = "Ex09_-_JPA_DAOPU";
    private final JpaDaoItf<Personne, Integer> persWrk;
    private final JpaDaoItf<Localite, Integer> locWrk;
    private final JpaDaoItf<Departement, Integer> depWrk;
    private final FileDaoItf<Localite> ficLocWrk;
    private final FileDaoItf<Departement> ficDepWrk;

    /**
     * Constructeur.
     *
     * @throws app.exceptions.MyDBException
     */
    public DbWorker() throws MyDBException {
        persWrk = new JpaDao<>(JPA_PU, Personne.class);
        locWrk = new JpaDao<>(JPA_PU, Localite.class);
        depWrk = new JpaDao<>(JPA_PU, Departement.class);
        ficLocWrk = new FileDao<>(new LocaliteExtracter("\t"));
        ficDepWrk = new FileDao<>(new DepartementExtracter(";"));
    }

    /*
   * AUTRES
     */
    @Override
    public void fermerBD() {
        
        //Déconnecter les trois connexions faites auparavant
        persWrk.deconnecter();
        locWrk.deconnecter();
        depWrk.deconnecter();
        
    }

    @Override
    public boolean estConnecte() {
        return persWrk.estConnectee() || locWrk.estConnectee() || depWrk.estConnectee();
    }

    @Override
    public List<Personne> lirePersonnes() throws MyDBException {
        
        //Retour de la liste des personnes
        return persWrk.lireListe();
        
    }

    @Override
    public long compterPersonnes() throws MyDBException {
        
        //Retou du nombre de personnes
        return persWrk.compter();
        
    }

    @Override
    public void ajouterPersonne(Personne p) throws MyDBException {
        
        //Création de la personne
        persWrk.creer(p);
        
    }

    @Override
    public Personne lirePersonne(Personne p) throws MyDBException {
        
        //Retour de la liste des personnes
        return persWrk.lire(p.getPkPers());
        
    }

    @Override
    public void modifierPersonne(Personne p) throws MyDBException {
        
        //Modification de la personne
        persWrk.modifier(p);
        
    }

    @Override
    public void effacerPersonne(Personne p) throws MyDBException {
        
        //Effacer la personne
        persWrk.effacer(p.getPkPers());
        
    }

    @Override
    public Personne rechercherPersonneAvecNom(String nomARechercher) throws MyDBException {
       
        Personne personne = persWrk.rechercher("nom", nomARechercher);
        
        //Retour du résultat
        return personne; 
        
    }

    @Override
    public List<Localite> lireLocalites() throws MyDBException {
        
        //Retour de la liste des localités
        return locWrk.lireListe();
        
    }

    @Override
    public long compterLocalites() throws MyDBException {
        
        //Retour du nombre de localités
        return locWrk.compter();
        
    }

    @Override
    public int lireEtSauverLocalites(File fichier, String nomCharset) throws Exception {
        
        //Lire la liste des localités
        List<Localite> listeLoc = ficLocWrk.lireFichierTexte(fichier, nomCharset);
        
        //Définition du résultat à -1 pour éviter de le changer deux fois
        int res = -1;
        
        //Si la liste n'est pas null
        if(listeLoc != null){
            
            //S'il y a quelque chose dans la liste
            if(!listeLoc.isEmpty()){
                
                //Sauvegarder
                locWrk.sauverListe(listeLoc);
                
                //Le nombre de localités présentes dans la liste
                res = listeLoc.size();
                
            }
        }
        
        //Retour du résultat
        return res;
    }

    @Override
    public List<Departement> lireDepartements() throws MyDBException {
        
        //Retour de la liste des départtements
        return depWrk.lireListe();
        
    }

    @Override
    public long compterDepartements() throws MyDBException {
        
        //Retour du nombre de departements
        return depWrk.compter();
        
    }

    @Override
    public int lireEtSauverDepartements(File fichier, String nomCharset) throws Exception {
        
        //Lire la liste des départements
        List<Departement> listeDep = ficDepWrk.lireFichierTexte(fichier, nomCharset);
        
        //Initialiser le résultat à -1, c'est moins long qu'affecter deux fois
        int res = -1;
        
        //Si la liste n'est pas null
        if(listeDep != null){
            
            //S'il y a quelque chose dans la liste
            if(!listeDep.isEmpty()){
               
                //Sauvegarde
                depWrk.sauverListe(listeDep);
                
                //Le nombre de departements présents dans la liste
                res = listeDep.size();
                
            }
        }
        
        //Retour du résultat
        return res;
    }

}
