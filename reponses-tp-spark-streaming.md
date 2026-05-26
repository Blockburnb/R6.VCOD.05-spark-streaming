# Reponses au TP - Spark Streaming

## 4.1 Creation de la session Spark

- `SparkSession.builder` : construit la session.
- `appName("CensusStreaming")` : donne un nom a l'appli.
- `master("spark://spark-master:7077")` : relie au master.
- `config("spark.sql.shuffle.partitions", "4")` : fixe les partitions de shuffle.
- `getOrCreate()` : cree ou reuse la session.

## 4.2 Definition du schema

Le schema dit a Spark comment lire les colonnes. Ca evite qu'il devine tout seul.

## 5.1 Lecture du repertoire surveille

La requete peut etre ecrite en deux parties pour mieux comprendre son role :

```python
stream_reader = (
    spark.readStream
    .format("csv")
    .schema(schema_defined)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .load(STREAM_PATH)
)

stream_memory_query = (
    stream_reader.writeStream
    .outputMode("append")
    .format("memory")
    .queryName("stream_data_check")
    .trigger(processingTime="5 seconds")
    .start()
)
```

`stream_reader` lit les fichiers. `stream_memory_query` envoie le resultat dans la table memoire.

## 6.1 Ajouter des fichiers progressivement

### 1. Pourquoi les donnees n’apparaissent-elles pas immediatement ?

Parce que Spark traite par micro-batch. Il faut attendre le prochain passage.

### 2. Quel est le role de `maxFilesPerTrigger` ?

Il limite le nombre de fichiers lus a chaque fois. Ici, c'est un fichier par trigger.

### 3. Quelle difference existe-t-il entre batch et micro-batch ?

Le batch traite tout d'un coup. Le micro-batch traite petit a petit.

## 7. Suppression d'un fichier pendant le streaming

### 1. Les donnees disparaissent-elles du resultat ?

Non, elles restent.

### 2. Pourquoi ?

Parce que Spark garde ce qu'il a deja lu.

### 3. Spark relit-il les anciens fichiers ?

Non, il evite de le relire.

### 4. Comment Spark memorise-t-il les fichiers deja traites ?

Spark garde un suivi interne, souvent avec le checkpoint.

### 5. Que se passe-t-il quand on remet le fichier supprime ?

S'il est vu comme nouveau, il peut etre relu. Sinon, non.

## 8. Utilisation des tables memoire

Les tables memoire servent a voir le stream avec Spark SQL.

### 8.1 Creation d’une table memoire

Elle enregistre le flux dans une table en memoire.

### 8.2 Interroger les donnees en SQL

Elle permet de lire la table avec du SQL.

## 9. Aggregations temps reel avec DataFrames

### 9.1 Comptage des lignes

Ca compte le nombre total de lignes.

### 9.2 Affichage temps reel

`complete` affiche tout le resultat a chaque fois.

`append` affiche seulement les nouvelles lignes.

## 10. GroupBy en streaming

### 10.1 Nombre de personnes par niveau d'education

`groupBy("education")` regroupe par niveau d'education puis compte.

### 10.2 Affichage du resultat

Elle affiche le resultat dans la console.

### Travail demande

Les autres agregations font pareil : on regroupe et on calcule.

- Par sexe : `groupBy("sex").count()`.
- Par pays : `groupBy("native-country").count()`.
- Moyenne des heures : `groupBy().avg("hours-per-week")`.
- Moyenne d'age par profession : `groupBy("occupation").avg("age")`.