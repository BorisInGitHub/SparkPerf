#Performance pour la recherche dans une infra BigData/Hadoop


##But
Optimiser la recherche d'un élément dans des données par un identifiant direct.

##Contexte
On utilise la recherche via SPARK pour permettre une recherche via des comparateurs spécifiques (code Java voir Groovy) de données mises dans une infrastructure Hadoop.

Plusieurs pistes :
* Hbase avec ces indexs
* Hive avec ces indexs
* Index de Hadoop directement
* Librairie Spark gérant les indexs (pas encore suivie par Spark)
* Spark SQL qui a peut-être des indexs
* ...


Vu le temps imparti pour des résultats concrets, nous allons chercher à être le plus efficace et donc certaines pistes risques de ne pas être exploitées.

##Environnement
* VM MapR en local sur mon poste (il n'y a pas d'intérêt de tester la performance sur plusieurs nœuds si ces nœuds sont des VMs sur la même machine hôte). 
* MapR est en version 5.1 => Hive en 1.2.1 (ou 0.13)
* le fichier de données comporte 2 colonnes (id et nom) et contient 10 000 000 de lignes.

##Cas étudiés
* le cas actuel, tout est mis via SPARK (stockage parquet) et l'on fait la recherche en lisant chaque valeur de la colonne
* Spark dans Hadoop
* une table Hive sans index
* une table Hive avec index 
* une table dans HBase

##Résultats

|Cas|Préparation des données|Search by Id|Count All|
|---|---|---|---|
|Hive brut (sans index ou autre)|3490ms|2334ms|24220ms|
|Hive brut (sans index ou autre) - Key en String|802ms|**4416ms**|22121ms|
|Hive avec index|123761ms|2415ms|25034ms|
|Spark brut en local (pas sur la VM)|ms|ms|ms|
|Spark brut yarn-client|ms|ms|ms|
|HBase|ms|ms|ms|

_Rq: L'index de Hive n'a pas l'air de fonctionner dans mon cas !_

_**TO BE CONTINUED ...**_
