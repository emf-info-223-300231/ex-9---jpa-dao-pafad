/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package app.beans;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author AbrahamL
 */
@Entity
@Table(name = "t_departement")
@XmlRootElement
@NamedQueries(
{
    @NamedQuery(name = "Departement.findAll", query = "SELECT d FROM Departement d"),
    @NamedQuery(name = "Departement.findByPkDep", query = "SELECT d FROM Departement d WHERE d.pkDep = :pkDep"),
    @NamedQuery(name = "Departement.findByDepartement", query = "SELECT d FROM Departement d WHERE d.departement = :departement"),
    @NamedQuery(name = "Departement.findByLocalite", query = "SELECT d FROM Departement d WHERE d.localite = :localite"),
    @NamedQuery(name = "Departement.findByDateCreation", query = "SELECT d FROM Departement d WHERE d.dateCreation = :dateCreation")
})
public class Departement implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PK_DEP")
    private Integer pkDep;
    @Column(name = "departement")
    private String departement;
    @Column(name = "localite")
    private String localite;
    @Column(name = "date_creation")
    @Temporal(TemporalType.DATE)
    private Date dateCreation;
    @OneToMany(mappedBy = "fkDep")
    private List<Personne> personneList;

    public Departement() {
    }

    public Departement(Integer pkDep) {
        this.pkDep = pkDep;
    }

    public Integer getPkDep() {
        return pkDep;
    }

    public void setPkDep(Integer pkDep) {
        this.pkDep = pkDep;
    }

    public String getDepartement() {
        return departement;
    }

    public void setDepartement(String departement) {
        this.departement = departement;
    }

    public String getLocalite() {
        return localite;
    }

    public void setLocalite(String localite) {
        this.localite = localite;
    }

    public Date getDateCreation() {
        return dateCreation;
    }

    public void setDateCreation(Date dateCreation) {
        this.dateCreation = dateCreation;
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
        hash += (pkDep != null ? pkDep.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Departement))
        {
            return false;
        }
        Departement other = (Departement) object;
        if ((this.pkDep == null && other.pkDep != null) || (this.pkDep != null && !this.pkDep.equals(other.pkDep)))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return departement + " (" + localite + ")";
    }
    
}
