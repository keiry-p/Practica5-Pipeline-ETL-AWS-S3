# Práctica 5: Pipeline ETL Automatizado a AWS S3

## Objetivo
Implementar un pipeline ETL en Python que extraiga datos reales de AWS S3, aplique transformaciones y almacene los resultados en formato Apache Parquet.

## Tecnologías
- Python
- AWS S3
- Boto3
- Pandas
- Scikit-learn
- PyArrow

## Fases del pipeline

### 1. Extracción
Lectura del archivo raw/transacciones.csv desde AWS S3 mediante Boto3.

### 2. Transformación
- Cálculo de monto_compras.
- Discretización con pd.cut.
- Normalización con MinMaxScaler.
- Generación de score_promocional.

### 3. Carga
- Serialización a Apache Parquet con io.BytesIO.
- Compresión Snappy.
- Carga a processed/etl_output.parquet en AWS S3.
- Verificación del archivo mediante head_object.

## Resultados
- Registros procesados: 10
- Columnas finales: 10
- Archivo Parquet generado: 7,108 bytes
- Destino: processed/etl_output.parquet

## Instalación
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

## Ejecución
Configurar credenciales temporales de AWS Academy en conexion_s3_privada.py.

Ejecutar:
python pipeline_etl.py

## Seguridad
El archivo conexion_s3_privada.py contiene credenciales temporales y está excluido del repositorio mediante .gitignore.
