CREATE DATABASE  IF NOT EXISTS `223_personne_3tables` /*!40100 DEFAULT CHARACTER SET latin1 */;
USE `223_personne_3tables`;
-- MySQL dump 10.13  Distrib 5.1.40, for Win32 (ia32)
--
-- Host: 127.0.0.1    Database: 223_personne_3tables
-- ------------------------------------------------------
-- Server version	5.1.43-community

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `t_personne`
--

DROP TABLE IF EXISTS `t_personne`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t_personne` (
  `PK_PERS` int(11) NOT NULL AUTO_INCREMENT,
  `Prenom` varchar(100) DEFAULT NULL,
  `Nom` varchar(100) DEFAULT NULL,
  `Date_naissance` date DEFAULT NULL,
  `No_rue` int(11) DEFAULT NULL,
  `Rue` varchar(100) DEFAULT NULL,
  `FK_LOC` int(11) DEFAULT NULL,
  `FK_DEP` int(11) DEFAULT NULL,
  `Actif` tinyint(1) DEFAULT NULL,
  `Salaire` decimal(12,2) DEFAULT NULL,
  `date_modif` timestamp not null default current_timestamp on update current_timestamp,
  `no_modif` int(11) not null DEFAULT '0',
  PRIMARY KEY (`PK_PERS`),
  KEY `PK_LOC` (`FK_LOC`),
  KEY `PK_DEP` (`FK_DEP`),
  CONSTRAINT `PK_DEP` FOREIGN KEY (`FK_DEP`) REFERENCES `t_departement` (`PK_DEP`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `PK_LOC` FOREIGN KEY (`FK_LOC`) REFERENCES `t_localite` (`PK_LOC`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `t_personne`
--

LOCK TABLES `t_personne` WRITE;
/*!40000 ALTER TABLE `t_personne` DISABLE KEYS */;
INSERT INTO `t_personne` VALUES 
(1,'Max','Dupond','1968-02-29',444,'Pérolles',null,null,0,'25000.00','1970-01-01 00:00:01', 0),
(2,'Bernard','Haller','1943-05-13',14,'Rte du Centre',null,null,1,'100000.00','1970-01-01 00:00:01', 0),
(3,'Alexia','Baurer','1990-01-01',12,'Ch. de la Gare',null,null,1,'75000.00','1970-01-01 00:00:01', 0),
(4,'Louis','Rossier','1932-07-31',25,'Rte des Murailles',null,null,1,'45000.00','1970-01-01 00:00:01', 0),
(5,'André','Magnin','1988-08-08',65,'Les Dailles',null,null,0,'47500.00','1970-01-01 00:00:01', 0),
(6,'Edouard','Gumy','1972-12-01',23,'Gorges du Gotéron',null,null,0,'52134.00','1970-01-01 00:00:01', 0),
(9,'Jacques-André','de la Patelière','1958-10-15',1,'Rue du château',null,null,1,'125110.00','1970-01-01 00:00:01', 0),
(11,'Chen','Talow','1972-02-29',1,'School Road',null,null,1,'12000.00','1970-01-01 00:00:01', 0),
(12,'Mike','Gandi','1966-06-06',236,'Bourbon Street',null,null,1,'14500.00','1970-01-01 00:00:01', 0),
(13,'Robert','Moret','1966-03-03',5,'La Jorettaz',null,null,1,'39000.00','1970-01-01 00:00:01', 0),
(14,'Gabrielle','Vuarnoz','1955-05-05',12,'Gai-Logis',null,null,0,'63000.00','1970-01-01 00:00:01', 0),
(15,'Francis','Fragnière','1969-02-12',1,'Le Buth',null,null,0,'41156.00','1970-01-01 00:00:01', 0),
(16,'Laurence2','Toffel','1980-03-21',35,'Wilerweg',null,null,1,'57300.00','1970-01-01 00:00:01', 0),
(17,'Henri','Marmier','1976-10-13',8,'Rue du château',null,null,0,'43050.00','1970-01-01 00:00:01', 0),
(18,'Christel','Charrière','2019-03-14',2,'Pl. de la Perraisa',null,null,1,'23000.00','1970-01-01 00:00:01', 0),
(19,'Thomas','Weibel','1996-06-30',6,'Ober Tasberg',null,null,0,NULL,'1970-01-01 00:00:01', 0),
(20,'Frédéric','Bielmann','2000-01-01',35,'Le Chêne',null,null,0,NULL,'1970-01-01 00:00:01', 0),
(21,'Urs','Lehmann','2008-08-08',3,'Bernertor',null,null,0,NULL,'1970-01-01 00:00:01', 0);

/*!40000 ALTER TABLE `t_personne` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;


--
-- Table structure for table `t_localite`
--

DROP TABLE IF EXISTS `t_localite`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t_localite` (
  `PK_LOC` int(11) NOT NULL,
  `npa` int(11) DEFAULT NULL,
  `localite` varchar(50) DEFAULT NULL,
  `canton` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`PK_LOC`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `t_localite`
--

LOCK TABLES `t_localite` WRITE;
/*!40000 ALTER TABLE `t_localite` DISABLE KEYS */;
/*!40000 ALTER TABLE `t_localite` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `t_departement`
--

DROP TABLE IF EXISTS `t_departement`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `t_departement` (
  `PK_DEP` int(11) NOT NULL AUTO_INCREMENT,
  `departement` varchar(45) DEFAULT NULL,
  `localite` varchar(45) DEFAULT NULL,
  `date_creation` date DEFAULT NULL,
  PRIMARY KEY (`PK_DEP`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `t_departement`
--

LOCK TABLES `t_departement` WRITE;
/*!40000 ALTER TABLE `t_departement` DISABLE KEYS */;
/*!40000 ALTER TABLE `t_departement` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2010-08-20 16:34:40
