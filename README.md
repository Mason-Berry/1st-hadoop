# GenreComboAnalysis MapReduce Job - Setup & Execution Guide

This guide walks through compiling, packaging, and running the `GenreComboAnalysis.java` MapReduce program on the provided IMDb dataset using Hadoop.

---

## 📁 1. Place the Java File

* Copy `GenreComboAnalysis.java` into your Hadoop working directory:

  ```bash
  ~/hadoop/GenreComboAnalysis.java
  ```

---

## ⚙️ 2. Compile the Java Program

From inside the `~/hadoop` directory:

```bash
mkdir -p classes
javac -d classes -cp $(hadoop classpath) GenreComboAnalysis.java
```

---

## 📦 3. Package the Compiled Classes

```bash
jar -cvf genrecombo.jar -C classes/ .
```

This creates a JAR file named `genrecombo.jar` for use in Hadoop.

---

## 📂 4. Prepare HDFS Input Directory

Ensure the dataset is in the Hadoop Distributed File System (HDFS):

```bash
hdfs dfs -mkdir -p /imdb/input
hdfs dfs -put ~/hadoop_input/Spring2025-Project3-IMDbData.txt /imdb/input/
```

> ⚠️ Skip this step if the file already exists in HDFS.

---

## 🪟 5. Remove Previous Output (if needed)

Hadoop will not overwrite existing output directories:

```bash
hdfs dfs -rm -r /imdb/output
```

---

## 🚀 6. Run the Hadoop Job

```bash
hadoop jar genrecombo.jar GenreComboAnalysis /imdb/input /imdb/output
```

---

## 📄 7. View the Output

```bash
hdfs dfs -cat /imdb/output/part-r-00000
```

Expected format:

```
[1991-2000],Action;Thriller     55
[2001-2010],Comedy;Romance      400
...
```

---

## 🧠 Notes for Interpreting Results

* Only `titleType == "movie"` entries are considered.
* Only movies with **rating ≥ 7.0** are included.
* Each movie must contain **both genres** in a target genre pair.
* Valid genre pairs:

  * Action & Thriller
  * Adventure & Drama
  * Comedy & Romance
* Time periods are grouped into:

  * 1991–2000
  * 2001–2010
  * 2011–2020

---

For questions or to verify counts, refer to the original data or verification scripts.
