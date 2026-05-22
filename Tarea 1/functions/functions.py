# =========================================================
# Imports
# =========================================================

from pyspark.sql import DataFrame, Window
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ShortType,
    DateType,
    FloatType
)
from pyspark.sql.functions import (
    col,
    sum,
    avg,
    count,
    countDistinct,
    desc,
    row_number
)

# =========================================================
# Load functions
# =========================================================

def load_cyclists(
    spark: SparkSession,
    file_path: str
) -> DataFrame:
    """
    Load cyclists CSV file.
    """

    # TODO:
    #
    # Define schema
    cyclist_schema = StructType([
        StructField("cedula", LongType(), False),
        StructField("nombre_completo", StringType(), False),
        StructField("provincia", StringType(), False)
    ])
    # Read CSV
    cyclist_df = spark.read.csv(
        file_path,
        header=False,
        schema=cyclist_schema
    )
    # Return dataframe
    return cyclist_df


def load_routes(
    spark: SparkSession,
    file_path: str
) -> DataFrame:
    """
    Load routes CSV file.
    """

    # TODO:
    #
    # Define schema
    routes_schema = StructType([
        StructField("codigo_ruta", ShortType(), False),
        StructField("nombre_ruta", StringType(), False),
        StructField("kilometros", FloatType(), False)
    ])
    # Read CSV
    routes_df = spark.read.csv(
        file_path,
        header=False,
        schema=routes_schema
    )
    # Return dataframe
    return routes_df


def load_activities(
    spark: SparkSession,
    file_path: str
) -> DataFrame:
    """
    Load activities CSV file.
    """

    # TODO:
    #
    # Define schema
    activities_schema = StructType([
        StructField("codigo_ruta", ShortType(), False),
        StructField("cedula", LongType(), False),
        StructField("fecha", DateType(), False)
    ])
    # Read CSV
    activities_df = spark.read.csv(
        file_path,
        header=False,
        schema=activities_schema
    )
    # Return dataframe
    return activities_df


# =========================================================
# Join functions
# =========================================================

def join_data(
    cyclists_df: DataFrame,
    routes_df: DataFrame,
    activities_df: DataFrame
) -> DataFrame:
    """
    Join all input datasets.
    """

    # TODO:
    #
    # Join cyclists with activities
    join_cyclists_activities_df = cyclists_df.join(activities_df, on="cedula", how="left")
    # Join result with routes
    join_full_df = join_cyclists_activities_df.join(routes_df, on="codigo_ruta", how="left")
    # Return final dataframe
    return join_full_df


# =========================================================
# Aggregation functions
# =========================================================

def calculate_total_kilometers(
    joined_df: DataFrame
) -> DataFrame:
    """
    Calculate total kilometers by cyclist.
    """

    # TODO:
    #
    # Group data
    # Sum kilometers
    total_per_cyclist_df = joined_df.groupby("cedula", "nombre_completo").agg(sum("kilometros").alias("kilometros_totales")).fillna(0, subset=["kilometros_totales"])
    # Return dataframe
    return total_per_cyclist_df


def calculate_daily_average(
    joined_df: DataFrame
) -> DataFrame:
    """
    Calculate average daily kilometers.
    """

    # TODO:
    #
    no_null_km_df = joined_df.fillna(0, subset=["kilometros"])
    # Aggregate by date
    by_day_df = no_null_km_df.groupBy("cedula", "nombre_completo", "provincia", "fecha").agg(sum("kilometros").alias("kilometros_diarios"))
    # Calculate averages
    avg_df = by_day_df.groupBy("cedula", "nombre_completo", "provincia").agg(avg("kilometros_diarios").alias("promedio_diario"))
    # Return dataframe
    return avg_df


def calculate_province_totals(
    joined_df: DataFrame
) -> DataFrame:
    """
    Calculate total kilometers by province.
    """

    # TODO:
    #
    no_null_km_df = joined_df.fillna(0, subset=["kilometros"])
    # Group by province
    # Sum kilometers
    total_km_by_province_df = no_null_km_df.groupBy("provincia").agg(sum("kilometros").alias("kilometros_totales_provincia"))
    # Return dataframe
    return total_km_by_province_df


# =========================================================
# Ranking functions
# =========================================================

def get_top_cyclists_by_total_km(
    totals_df: DataFrame,
    top_n: int = 5
) -> DataFrame:
    """
    Return top cyclists by total kilometers.
    """

    # TODO:
    #
    window_spec = Window.partitionBy("provincia").orderBy(
        col("kilometros_totales").desc(),
        col("promedio_diario").desc() 
    )
    # Order dataframe
    df_con_puesto = totals_df.withColumn(
        "puesto", 
        row_number().over(window_spec)
    )
    # Limit top N
    df_top_n = df_con_puesto.filter(col("puesto") <= top_n)
    df_final = df_top_n.drop("puesto")
    # Return dataframe
    return df_final


def get_top_cyclists_by_daily_average(
    averages_df: DataFrame,
    top_n: int = 5
) -> DataFrame:
    """
    Return top cyclists by daily average.
    """

    # TODO:
    #
    window_spec = Window.partitionBy("provincia").orderBy(
        col("promedio_diario").desc(),
        col("kilometros_totales").desc()
    )
    # Order dataframe
    df_top_n = averages_df.withColumn("ranking", row_number().over(window_spec)) \
                          .filter(col("ranking") <= top_n) \
                          .drop("ranking")
    # Return dataframe
    return df_top_n


# =========================================================
# Utility functions
# =========================================================

def validate_dataframe(
    df: DataFrame
) -> bool:
    """
    Validate dataframe content.
    """
    try:
            # 1. Validate schema: Verificamos que el DataFrame tenga columnas
            if len(df.columns) == 0:
                print("Error de validación: El DataFrame no tiene esquema (0 columnas).")
                return False
                
            # Validamos que no esté vacío antes de hacer operaciones pesadas
            total_filas = df.count()
            if total_filas == 0:
                print("Error de validación: El DataFrame está completamente vacío.")
                return False

            # 2. Validate duplicates: Comparamos conteo total vs conteo sin duplicados
            filas_unicas = df.dropDuplicates().count()
            if total_filas != filas_unicas:
                print(f"Error de validación: Se encontraron {total_filas - filas_unicas} filas duplicadas.")
                return False

            # 3. Validate nulls: Buscamos nulos columna por columna
            for columna in df.columns:
                nulos_en_columna = df.filter(col(columna).isNull()).count()
                if nulos_en_columna > 0:
                    print(f"Error de validación: Hay {nulos_en_columna} valores nulos en '{columna}'.")
                    return False

            # Si supera todas las pruebas, es válido
            print("Validación exitosa: El DataFrame es correcto.")
            return True
            
    except Exception as e:
        print(f"Excepción durante la validación: {e}")
        return False
