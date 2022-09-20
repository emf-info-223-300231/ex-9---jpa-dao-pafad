/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package app.beans;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Version;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author AbrahamL
 */
@Entity
@Table(name = "t_personne")
@XmlRootElement
@NamedQueries(
{
    @NamedQuery(name = "Personne.findAll", query = "SELECT p FROM Personne p"),
    @NamedQuery(name = "Personne.findByPkPers", query = "SELECT p FROM Personne p WHERE p.pkPers = :pkPers"),
    @NamedQuery(name = "Personne.findByPrenom", query = "SELECT p FROM Personne p WHERE p.prenom = :prenom"),
    @NamedQuery(name = "Personne.findByNom", query = "SELECT p FROM Personne p WHERE p.nom = :nom"),
    @NamedQuery(name = "Personne.findByDatenaissance", query = "SELECT p FROM Personne p WHERE p.dateNaissance = :dateNaissance"),
    @NamedQuery(name = "Personne.findByNorue", query = "SELECT p FROM Personne p WHERE p.noRue = :noRue"),
    @NamedQuery(name = "Personne.findByRue", query = "SELECT p FROM Personne p WHERE p.rue = :rue"),
    @NamedQuery(name = "Personne.findByActif", query = "SELECT p FROM Personne p WHERE p.actif = :actif"),
    @NamedQuery(name = "Personne.findBySalaire", query = "SELECT p FROM Personne p WHERE p.salaire = :salaire"),
    @NamedQuery(name = "Personne.findByDateModif", query = "SELECT p FROM Personne p WHERE p.dateModif = :dateModif"),
    @NamedQuery(name = "Personne.findByNoModif", query = "SELECT p FROM Personne p WHERE p.noModif = :noModif")
})
public class Personne implements Serializable {

    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PK_PERS")
    private Integer pkPers;
    @Column(name = "Prenom")
    private String prenom;
    @Column(name = "Nom")
    private String nom;
    @Column(name = "Date_naissance")
    @Temporal(TemporalType.DATE)
    private Date dateNaissance;
    @Column(name = "No_rue")
    private Integer noRue;
    @Column(name = "Rue")
    private String rue;
    @Column(name = "Actif")
    private Boolean actif;
    // @Max(value=?)  @Min(value=?)//if you know range of your decimal fields consider using these annotations to enforce field validation
    @Column(name = "Salaire")
    private BigDecimal salaire;
    @Basic(optional = false)
    @Column(name = "date_modif")
    @Version
    private Timestamp dateModif;
    @Basic(optional = false)
    @Column(name = "no_modif")
    private int noModif;
    @JoinColumn(name = "FK_DEP", referencedColumnName = "PK_DEP")
    @ManyToOne
    private Departement fkDep;
    @JoinColumn(name = "FK_LOC", referencedColumnName = "PK_LOC")
    @ManyToOne
    private Localite fkLoc;

    public Personne() {
    }

    public Personne(Integer pkPers) {
        this.pkPers = pkPers;
    }

    public Personne(Integer pkPers, Timestamp dateModif, int noModif) {
        this.pkPers = pkPers;
        this.dateModif = dateModif;
        this.noModif = noModif;
    }

    public Integer getPkPers() {
        return pkPers;
    }

    public void setPkPers(Integer pkPers) {
        this.pkPers = pkPers;
    }

    public String getPrenom() {
        return prenom;
    }

    public void setPrenom(String prenom) {
        this.prenom = prenom;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public Date getDateNaissance() {
        return dateNaissance;
    }

    public void setDatenaissance(Date dateNaissance) {
        this.dateNaissance = dateNaissance;
    }

    public Integer getNoRue() {
        return noRue;
    }

    public void setNoRue(Integer norue) {
        this.noRue = norue;
    }

    public String getRue() {
        return rue;
    }

    public void setRue(String rue) {
        this.rue = rue;
    }

    public Boolean getActif() {
        return actif;
    }

    public void setActif(Boolean actif) {
        this.actif = actif;
    }

    public BigDecimal getSalaire() {
        return salaire;
    }

    public void setSalaire(BigDecimal salaire) {
        this.salaire = salaire;
    }

    public Timestamp getDateModif() {
        return dateModif;
    }

    public void setDateModif(Timestamp dateModif) {
        this.dateModif = dateModif;
    }

    public int getNoModif() {
        return noModif;
    }

    public void setNoModif(int noModif) {
        this.noModif = noModif;
    }

    public Departement getFkDep() {
        return fkDep;
    }

    public void setFkDep(Departement fkDep) {
        this.fkDep = fkDep;
    }

    public Localite getFkLoc() {
        return fkLoc;
    }

    public void setFkLoc(Localite fkLoc) {
        this.fkLoc = fkLoc;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (pkPers != null ? pkPers.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof Personne))
        {
            return false;
        }
        Personne other = (Personne) object;
        if ((this.pkPers == null && other.pkPers != null) || (this.pkPers != null && !this.pkPers.equals(other.pkPers)))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return  nom + " " + prenom;
    }
    
}
