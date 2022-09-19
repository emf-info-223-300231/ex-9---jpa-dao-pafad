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
        long nb = 0;
        try {
            String jpql = "SELECT count(e) FROM " + cl.getSimpleName() + " e";
            Query query = em.createQuery(jpql);
            nb = (Long) query.getSingleResult();
        } catch (Exception ex) {
            throw new MyDBException(SystemLib.getFullMethodName(), ex.getMessage());
        }
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
        return null;
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
        List<E> liste = new ArrayList<>();
        try {
            String jpql = "SELECT e FROM " + cl.getSimpleName() + " e";
            Query query = em.createQuery(jpql);
            liste = query.getResultList();
        } catch (Exception ex) {
            throw new MyDBException(SystemLib.getFullMethodName(), ex.getMessage());
        }
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
        int nb;
        return nb;
    }

    /**
     * Sauve une liste globale dans une seule transaction.
     *
     * @param list
     * @return TRUE si l'opération a pu se dérouler correctement
     * @throws app.exceptions.MyDBException
     */
    @Override
    public int sauverListe(List<E> list) throws MyDBException {
        int nb = 0;
        return nb;
    }

    /**
     * Déconnexion
     *
     * @throws app.exceptions.MyDBException
     */
    @Override
    public void deconnecter() {
        em.close();
        emf.close();
        em = null;
    }

    @Override
    public boolean estConnectee() {
        return (em != null) && em.isOpen();
    }

}
