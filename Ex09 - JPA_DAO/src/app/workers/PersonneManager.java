package app.workers;

import app.beans.Personne;
import java.util.ArrayList;
import java.util.List;

/**
 * Classe pour gérer la liste des personnes en mémoire.
 *
 * @author strittjc
 */
public class PersonneManager {

    private List<Personne> liste;
    private int index;

    public PersonneManager() {
        liste = new ArrayList<>();
        index = 0;
    }

    public void setListe(List<Personne> personnes) {
        liste = personnes;
    }

    public void majPersonne(Personne p) {
        liste.set(index, p);
    }

    public Personne personneCourante() {
        Personne p = null;
        if (!liste.isEmpty()) {
            p = liste.get(index);
        }
        return p;
    }

    public Personne personnePrecedente() {
        index = Math.max(0, index - 1);
        return personneCourante();
    }

    public Personne personneSuivante() {
        index = Math.max(0, Math.min(liste.size() - 1, index + 1));
        return personneCourante();
    }

    public Personne personneDebut() {
        index = 0;
        return personneCourante();
    }

    public Personne personneFin() {
        index = Math.max(0, liste.size() - 1);
        return personneCourante();
    }

}
