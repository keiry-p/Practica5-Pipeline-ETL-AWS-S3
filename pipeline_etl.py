
import io
import boto3
import pandas as pd

from sklearn.preprocessing import MinMaxScaler
from botocore.exceptions import ClientError

from conexion_s3_privada import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
    AWS_REGION
)

BUCKET_NAME = "aws-academy-datacenter-bucket-23-02-003"
KEY_RAW = "raw/transacciones.csv"
KEY_PROCESSED = "processed/etl_output.parquet"


def conectar_s3():
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION
    )
    return session.client("s3")


def extraer(s3):
    print("=== 1. FASE DE EXTRACCION (Extract) ===")

    respuesta = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=KEY_RAW
    )

    contenido = respuesta["Body"].read()
    df = pd.read_csv(io.BytesIO(contenido))

    print("Archivo de origen:", KEY_RAW)
    print("Registros obtenidos:", len(df))
    print(df.head().to_string(index=False))

    return df


def transformar(df):
    print("\n=== 2. FASE DE TRANSFORMACION (Transform) ===")

    df = df.copy()

    # Validacion de columnas requeridas
    columnas = {
        "id_transaccion", "cliente", "producto",
        "cantidad", "precio_unitario", "fecha"
    }

    faltantes = columnas - set(df.columns)

    if faltantes:
        raise ValueError(
            f"Columnas faltantes: {sorted(faltantes)}"
        )

    # Convertir columnas a tipos adecuados
    df["cantidad"] = pd.to_numeric(
        df["cantidad"], errors="raise"
    )

    df["precio_unitario"] = pd.to_numeric(
        df["precio_unitario"], errors="raise"
    )

    df["fecha"] = pd.to_datetime(
        df["fecha"], errors="raise"
    )

    # Feature Engineering: monto de venta
    df["monto_compras"] = (
        df["cantidad"] * df["precio_unitario"]
    )

    # Binning: categorias segun monto
    df["categoria_compra"] = pd.cut(
        df["monto_compras"],
        bins=[-float("inf"), 1000, 5000, float("inf")],
        labels=["Baja", "Media", "Alta"]
    )

    # Normalizacion MinMaxScaler
    scaler = MinMaxScaler()

    df["monto_normalizado"] = scaler.fit_transform(
        df[["monto_compras"]]
    )

    # Feature Engineering: score promocional
    df["score_promocional"] = (
        df["monto_normalizado"] * 100
    ).round(2)

    print("Dataset transformado:")
    print(df.head().to_string(index=False))

    print("\nRegistros:", len(df))
    print("Columnas:", len(df.columns))

    return df


def cargar(s3, df):
    print("\n=== 3. FASE DE CARGA (Load) ===")

    # Generar Parquet comprimido en memoria
    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    buffer.seek(0)

    # Subida real a AWS S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=KEY_PROCESSED,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    # Verificar que el archivo exista
    respuesta = s3.head_object(
        Bucket=BUCKET_NAME,
        Key=KEY_PROCESSED
    )

    print("Archivo cargado correctamente.")
    print("Destino:", KEY_PROCESSED)
    print("Tamano:", respuesta["ContentLength"], "bytes")


def pipeline_etl_completo():
    try:
        s3 = conectar_s3()

        df_original = extraer(s3)
        df_procesado = transformar(df_original)
        cargar(s3, df_procesado)

        print("\nPIPELINE ETL COMPLETADO CORRECTAMENTE.")

    except ClientError as error:
        print("\nERROR DE AWS:")
        print("Codigo:", error.response["Error"]["Code"])
        print("Mensaje:", error.response["Error"]["Message"])

    except Exception as error:
        print("\nERROR EN EL PIPELINE:")
        print(type(error).__name__, str(error))


if __name__ == "__main__":
    pipeline_etl_completo()
