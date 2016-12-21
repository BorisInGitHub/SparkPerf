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
* La machine de test est mon environnement de développement : CPU 4 coeurs (i5-6500 @ 3.2GHz) et 16Go de RAM.
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
|Hive brut (sans index ou autre)|3 490ms|2 334ms|24 220ms|
|Hive brut (sans index ou autre) - Key en String|802ms|**4 416ms**|22 121ms|
|Hive avec index|123 761ms|2 415ms|2 5034ms|
|Spark SQL 1.6.1 en local (pas sur la VM) (via librairie Spark CSV)|14 934ms|877ms|714ms|
|Spark SQL 1.6.1 en local (pas sur la VM)|6 025ms|859ms|735ms|
|Spark SQL 1.6.1 en local (pas sur la VM) (fichiers splittés)|197 302ms|1 027ms|762ms|
|Spark 1.6.1 en local (pas sur la VM) (Classes utilisateurs)|ms|ms|ms|
|Spark 1.6.1 via yarn-client|ms|ms|ms|
|HBase|45 031ms|10ms|Not applicable|
|Mongo (3.4)|40 830ms|4ms|2ms|
|PostgreSQL (9.6.1)|274 231ms|2ms|661ms|

Remarques: 
* L'index de Hive n'a pas l'air de fonctionner dans mon cas. En faite Hive va utiliser l'index pour savoir sur quelle partition envoyer les données. Comme nous avons qu'une seule partition, l'index Hive ne sert à rien dans ce cas d'usage !
* PostgreSQL, pour le chargement des données, nous avons utiliser la méthode INSERT, il est recommandé d'utiliser la méthode COPY (beaucoup plus rapide)
* Je n'ai pas trouvé comment faire un count avec HBase (sinon un genre de selectAll puis on compte manuellement ...)

_**TO BE CONTINUED ...**_
