/* 
Pregunta
===========================================================================

Escriba una consulta que retorne los valores únicos de la columna `t0.c5` 
(ordenados). 

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

CREATE TABLE respuesta AS SELECT EXPLODE(SPLIT(c5,':')) FROM data;

INSERT OVERWRITE LOCAL DIRECTORY './output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT DISTINCT * FROM respuesta;