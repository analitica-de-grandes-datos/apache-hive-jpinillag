/*

Pregunta
===========================================================================

Realice una consulta que compute la cantidad de veces que aparece cada valor 
de la columna `t0.c5`  por año.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.
    >>> Escriba su respuesta a partir de este punto <<<
*/
DROP TABLE IF EXISTS data;

CREATE TABLE data (c1 INT, 
    c2 STRING, 
    c3 INT,
    c4 STRING,
    c5 STRING,
    c6 STRING) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'data0.csv' OVERWRITE INTO TABLE data;

CREATE TABLE respuesta AS SELECT SUBSTRING(c4 ,0,4) AS ano,c5_letra FROM data LATERAL VIEW explode(SPLIT(c5,':')) exploded_table AS c5_letra;

INSERT OVERWRITE LOCAL DIRECTORY './output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT ano,c5_letra,count(*) FROM respuesta GROUP BY ano,c5_letra;