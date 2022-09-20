/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package app.beans;

import java.io.Serializable;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author AbrahamL
 */
@Entity
@Table(name = "t_localite")
@XmlRootElement
@NamedQueries(
{
    @NamedQuery(name = "Localite.findAll", query = "SELECT l FROM Localite l"),
    @NamedQuery(name = "Localite.findByPkLoc", query = "SELECT l FROM Localite l WHERE l.pkLoc = :pkLoc"),
    @NamedQuery(name = "Localite.findByNpa", query = "SELECT l FROM Localite l WHERE l.npa = :npa"),
    @NamedQuery(name = "Localite.findByLocalite", query = "SELECT l FROM Localite l WHERE l.localite = :localite"),
    @NamedQuery(name = "Localite.findByCanton", query = "SELECT l FROM Localite l WHERE l.canton = :canton")
})
public class Localite implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @Basic(optional = false)
    @Column(name = "PK_LOC")
    private Integer pkLoc;
    @Column(name = "npa")
    private Integer npa;
    @Column(name = "localite")
    private String localite;
    @Column(name = "canton")
    private String canton;
    @OneToMany(mappedBy = "fkLoc")
    private List<Personne> personneList;

    public Localite() {
    }

    public Localite(Integer pkLoc) {
        this.pkLoc = pkLoc;
    }

    public Integer getPkLoc() {
        return pkLoc;
    }

    public void setPkLoc(Integer pkLoc) {
        this.pkLoc = pkLoc;
    }

    public Integer getNpa() {
        return npa;
    }

    public void setNpa(Integer npa) {
        this.npa = npa;
    }

    public String getLocalite() {
        return localite;
    }

    public void setLocalite(String localite) {
        this.localite = localite;
    }

    public String getCanton() {
        return canton;
    }

    public void setCanton(String canton) {
        this.canton = canton;
    }

    @XmlTransient
    public List<Personne> getPersonneList() {
        return personneList;
    }

    public void setPersonneList(List<Personne> personneList) {
        this.personneList = personneList;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (pkLoc != null ? pkLoc.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Localite))
        {
            return false;
        }
        Localite other = (Localite) object;
        if ((this.pkLoc == null && other.pkLoc != null) || (this.pkLoc != null && !this.pkLoc.equals(other.pkLoc)))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return npa + pkLoc + " " + localite;
    }
    
}
