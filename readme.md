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
* Cassandra
* Base de données (SQL ou NoSQL) : Mongo, HBase pur, ElasticSearch, PostgreSQL, ...
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



## Mode Yarn-client

Utilisation:
* Lancer la VM Mapr (via VirtualVox)
* Se connecter sur elle (`mapr`/`mapr`) et regarder son adresse IP via la commande `ifconfig` (exemple 192.168.1.244)
* Configurer dans notre hosts `/etc/hosts` 
```
192.168.1.244    maprdemo
```
* Configurer le CLDB 
```
sudo /opt/mapr/server/configure.sh -N demo.mapr.com -c -C maprdemo:7222 -HS maprdemo -RM maprdemo
```
* Modifier le `core-site.xml` et `yarn-site.xml` qui sont dans le répertoire `resources` du projet (Pas utile je crois avec Mapr)
* Modifier dans le code JAVA de la classe `SparkSQLCsvLibPerf`               
```                                        
                                           .set("spark.yarn.dist.files", "maprfs://demo.mapr.com/user/spark/hivePerf-1.0-SNAPSHOT-worker.jar")
                                           .set("spark.yarn.jar", "maprfs://demo.mapr.com/user/spark/spark-assembly.jar")
                                           .set("spark.yarn.am.extraLibraryPath", "maprfs://demo.mapr.com/user/spark/hivePerf-1.0-SNAPSHOT-worker.jar");
```
* Compilation with ```mvn clean install```
* Copy the worker on the hadoop cluster and put it on HDFS
```
# From the current directory
scp target/hivePerf-1.0-SNAPSHOT-worker.jar mapr@maprdemo:///tmp/

ssh mapr@maprdemo 
# On the host maprdemo do
hadoop fs -fs maprfs://demo.mapr.com -mkdir -p /user/spark
hadoop fs -fs maprfs://demo.mapr.com -put /opt/mapr/spark/spark-1.6.1/lib/spark-assembly-1.6.1-mapr-1602-hadoop2.7.0-mapr-1602.jar /user/spark/spark-assembly.jar
hadoop fs -fs maprfs://demo.mapr.com -rm /user/spark/hivePerf-1.0-SNAPSHOT-worker.jar
hadoop fs -fs maprfs://demo.mapr.com -put /tmp/hivePerf-1.0-SNAPSHOT-worker.jar /user/spark/hivePerf-1.0-SNAPSHOT-worker.jar
hadoop fs -fs maprfs://demo.mapr.com -chmod -R 777 /user/spark
hadoop fs -fs maprfs://demo.mapr.com -ls /user/spark/
````                                       
* Ajouter les 2 variables d'environnement (via le launcher d'Idea par exemple)
````
HADOOP_CONF_DIR=/etc/hadoop/conf/
YARN_CONF_DIR=/etc/hadoop/conf/
````
Dans ce cas, l'application va lire les fichiers core-site.xml et yarn-site.xml ssur le poste local dans /etc/hadoop/conf/
* Et un peu de mémoire via les paramètres de la VM
````
-ea -Xmx4096m -Xms512m -XX:PermSize=512m -XX:MaxPermSize=4096m
````

* Copie datas sur le serveur et suppression de la précédante exécution parquet
````
scp /home/breynard/IdeaProjects/SparkPerf/src/main/oldResources/dataLong1709858110454081480100.txt mapr@maprdemo:///tmp/

  ssh mapr@maprdemo

hadoop fs -fs maprfs://demo.mapr.com -put /tmp/dataLong1709858110454081480100.txt /tmp/dataLong1709858110454081480100.txt
hadoop fs -fs maprfs://demo.mapr.com -ls /tmp/
hadoop fs -fs maprfs://demo.mapr.com -rm -r /tmp/spark.pq
hadoop fs -fs maprfs://demo.mapr.com -ls /tmp/
````


Aide :
 
* Version Spark dans MapR
``` 
ls /opt/mapr/spark/spark-1.6.1/lib/
```

* Log resourceManager
``` 
cd /opt/mapr/hadoop/hadoop-2.7.0/logs/
```

_**TO BE CONTINUED ...**_
