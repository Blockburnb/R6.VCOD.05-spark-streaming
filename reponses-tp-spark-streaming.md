# Reponses au TP - Spark Structured Streaming

## 4.1 Creation de la session Spark

- `SparkSession.builder` : cree la session Spark.
- `appName("CensusStreaming")` : nomme l'application.
- `master("spark://spark-master:7077")` : connecte Spark au master.
- `config("spark.sql.shuffle.partitions", "4")` : fixe le nombre de partitions de shuffle.
- `getOrCreate()` : cree ou reutilise la session.

## 4.2 Definition du schema

Le schema donne a Spark la structure et le type des colonnes avant la lecture. Cela evite l'inference automatique et rend le streaming plus fiable.

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

`stream_reader` lit les fichiers. `stream_memory_query` ecrit le resultat dans la table memoire `stream_data_check`.

## 6.1 Ajouter des fichiers progressivement

### 1. Pourquoi les donnees n’apparaissent-elles pas immediatement ?

Parce que le stream traite les donnees par micro-batch. Il faut attendre le prochain trigger.

### 2. Quel est le role de `maxFilesPerTrigger` ?

Il limite le nombre de fichiers lus a chaque micro-batch. Ici, Spark en traite un seul a la fois.

### 3. Quelle difference existe-t-il entre batch et micro-batch ?

Le batch traite un grand ensemble en une fois. Le micro-batch decoupe le traitement en petites etapes regulieres.

## 7. Suppression d'un fichier pendant le streaming

### 1. Les donnees disparaissent-elles du resultat ?

Non, les donnees deja lues restent affichees.

### 2. Pourquoi ?

Parce que Spark garde les donnees deja traitees. Supprimer le fichier source ne supprime pas les lignes deja chargees.

### 3. Spark relit-il les anciens fichiers ?

Non, Spark evite de relire un fichier deja traite.

### 4. Comment Spark memorise-t-il les fichiers deja traites ?

Spark garde un suivi interne, souvent via le checkpoint, pour savoir quels fichiers ont deja ete lus.

### 5. Que se passe-t-il quand on remet le fichier supprime ?

S'il est detecte comme nouveau, Spark peut le relire. Sinon, il est ignore.

## 8. Utilisation des tables memoire

Les tables memoire servent a voir le resultat du stream avec Spark SQL pendant que la requete tourne.