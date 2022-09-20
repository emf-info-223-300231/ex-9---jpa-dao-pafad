package app.workers.dao;

import app.exceptions.MyDBException;
import app.helpers.SystemLib;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.*;

/**
 * Implémentation de la couche DAO d'après l'API JpaDaoAPI. Equivalent à un
 * Worker JPA générique pour gérer n'importe quel entity-bean.
 *
 * @author ramalhom
 * @param <E>
 * @param <PK>
 */
public class JpaDao<E, PK> implements JpaDaoItf<E, PK> {

    private final Class<E> cl;
    private EntityManagerFactory emf;
    private EntityManager em;
    private EntityTransaction et;

    /**
     * Constructeur.
     *
     * @param pu
     * @param cl
     * @throws app.exceptions.MyDBException
     */
    public JpaDao(String pu, Class<E> cl) throws MyDBException {
        this.cl = cl;
        try {
            emf = Persistence.createEntityManagerFactory(pu);
            em = emf.createEntityManager();
            et = em.getTransaction();
        } catch (Exception ex) {
            throw new MyDBException(SystemLib.getFullMethodName(), ex.getMessage());
        }
    }

    /**
     * Ajoute un objet.
     *
     * @param e l'objet à persister dans la BD
     * @throws app.exceptions.MyDBException
     */
    @Override
    public void creer(E e) throws MyDBException {
        
        try {
            //Début de la transaction
            et.begin();

            //Persistance de l'objet
            em.persist(e);

            //Accepter la transaction
            et.commit();
            
        } catch (Exception ex){
            
            //Si la transaction est active
            if (et.isActive()){
                
                //Refuser la transaction
                et.rollback();
                
            }
            
        }
    }

    /**
     * Lit un objet d'après sa PK.
     *
     * @param pk l'identifiant de l'objet à lire
     * @return l'objet lu
     * @throws app.exceptions.MyDBException
     */
    @Override
    public E lire(PK pk) throws MyDBException {
        E e = null;
        try {
            e = em.find(cl, pk);
            if (e != null) {
                em.refresh(e);
            }
        } catch (Exception ex) {
            throw new MyDBException(SystemLib.getFullMethodName(), ex.getMessage());
        }
        return e;
    }

    /**
     * Modifie un objet dans la BD.
     *
     * @param e l'objet à modifier
     * @throws app.exceptions.MyDBException
     */
    @Override
    public void modifier(E e) throws MyDBException {
        try {
            et.begin();
            e = em.merge(e);
            et.commit();
        } catch (OptimisticLockException ex) {
            if (et.isActive()) {
                et.rollback();
            }
            throw new MyDBException(SystemLib.getFullMethodName(), "OptimisticLockException: " + ex.getMessage());
        } catch (RollbackException ex) {
            if (et.isActive()) {
                et.rollback();
            }
            if (ex.getCause() instanceof OptimisticLockException) {
                throw new MyDBException(SystemLib.getFullMethodName(), "OptimisticLockException: " + ex.getMessage());
            } else {
                throw new MyDBException(SystemLib.getFullMethodName(), "RollbackException: " + ex.getMessage());
            }
        } catch (Exception ex) {
            et.rollback();
            throw new MyDBException(SystemLib.getFullMethodName(), ex.getMessage());
        }
    }

    /**
     * Efface un objet d'après son identifiant (PK).
     *
     * @param pk l'identifiant de l'objet à lire
     * @throws app.exceptions.MyDBException
     */
    @Override
    public void effacer(PK pk) throws MyDBException {
        E e = lire(pk);
        if (e != null) {
            try {
                et.begin();
                em.remove(e);
                et.commit();
            } catch (OptimisticLockException ex) {
                et.rollback();
                throw new MyDBException(SystemLib.getFullMethodName(), "OptimisticLockException: " + ex.getMessage());
            } catch (Exception ex) {
                et.rollback();
                throw new MyDBException(SystemLib.getFullMethodName(), ex.getMessage());
            }
        }
    }

    /**
     * Retourne le nombre d'objets actuellement dans une table de la DB.
     *
     * @return le nombre d'objets
     * @throws app.exceptions.MyDBException
     */
    @Override
    public long compter() throws MyDBException {
        //On initialise un nombre à 0 pour compter
        long nb = 0;
        
        try {
         
            //Préparation de la requête
            String jpql = "SELECT count(e) FROM " + cl.getSimpleName() + " e";
            
            //Création de la query
            Query query = em.createQuery(jpql);
            
            //On prend le seul résultat, qui est le nombre d'enregistrements présents
            nb = (Long) query.getSingleResult();
        
        } catch (Exception ex) {
            
            //Lever l'exception
            throw new MyDBException(SystemLib.getFullMethodName(), ex.getMessage());
        
        }
        
        //Retour du nombre d'enregistrements
        return nb;
    }

    /**
     * Rechercher un objet d'après la valeur d'une propriété spécifiée.
     *
     * @param prop la propriété sur laquelle faire la recherche
     * @param valeur la valeur de cette propriété
     * @return l'objet recherché ou null
     * @throws app.exceptions.MyDBException
     */
    @Override
    @SuppressWarnings("unchecked")
    public E rechercher(String prop, Object valeur) throws MyDBException {
        
        //Préparation de la requête
        Query query = em.createQuery("SELECT e FROM Personne e WHERE e." + prop + " = '" + valeur + "'");
        
        //Retour du seul résltat qu'il y aura ou null si rien n'est trouvé
        return (E)query.getSingleResult();
    }

    /**
     * Récupère une liste avec tous les objets de la table.
     *
     * @return une liste d'objets.
     * @throws app.exceptions.MyDBException
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<E> lireListe() throws MyDBException {
        
        //Déclaration d'une liste insanciée
        List<E> liste = new ArrayList<>();
        
        try {
            //Une mini condition est nécéssaire pour ordrer par nom les personnes
            String jpql = "SELECT e FROM " + cl.getSimpleName() + " e" + (cl.getSimpleName().equals("Personne") ? " ORDER BY e.nom" : "");
            
            //Création de la query
            Query query = em.createQuery(jpql);
            
            //Affectation de la liste des résultats à la liste crée auparavant
            liste = query.getResultList();
            
        } catch (Exception ex) {
            
            //Lever l'exception
            throw new MyDBException(SystemLib.getFullMethodName(), ex.getMessage());
            
        }
        
        //Retour de la liste
        return liste;
    }

    /**
     * Efface complètement tout le contenu d'une entité en une seule
     * transaction.
     *
     * @return le nombre d'objets effacés
     * @throws app.exceptions.MyDBException
     */
    @Override
    public int effacerListe() throws MyDBException {
        //Déclaration d'un nombre pour avoir le nombre d'enregistrements supprimés
        int nb = 0;
        
        try{
            //Début de la transaction
            et.begin();

            //Enlever toute la liste
            em.remove(lireListe());

            //Accepter la transaction
            et.commit();
            
        } catch (Exception ex) {
            
            //Si la transaction est active
            if(et.isActive()){
                
                //Refus de la transaction
                et.rollback();
                
            }
            
        }
        
        //Retour du résultat
        return nb;
    }

    /**
     * Sauve une liste globale dans une seule transaction.
     *
     * @param list
     * @return le nombre d'objets sauvegardés
     * @throws app.exceptions.MyDBException
     */
    @Override
    public int sauverListe(List<E> list) throws MyDBException {
        //Déclaration du nombre qui va contenir le nombre d'enregistrements sauvegardés
        int nb = 0;
        
        try{
            
            //Début de la transaction
            et.begin();
        
            //Pour chaque éléments dans la liste
            for (E e : list){
            
                //Je persiste l'objet qui est à l'intérieur de la liste
                em.persist(e);
            
            }
        
            //Accepter la transaction
            et.commit();
        
        } catch (Exception ex){
            
            //On réinitalise le nombre en cas de problème
            nb = 0;
            
            //Lever l'exception
            throw new MyDBException(SystemLib.getFullMethodName(), ex.getMessage());
        
        }
        
        //retour du résultat
        return nb;
             
    }

    /**
     * Déconnexion
     *
     * @throws app.exceptions.MyDBException
     */
    @Override
    public void deconnecter() {
        
        //Clore les connecxions
        em.close();
        emf.close();
        
        //Destruction de l'objet
        em = null;
    }

    @Override
    public boolean estConnectee() {
        
        //Une condition pour savoir si la ou les bases sont ouvertes
        return (em != null) && em.isOpen();
        
    }

}
