# =========================================================
# Imports
# =========================================================

import pytest

from pyspark.sql import Row
from datetime import date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType, LongType,
    ShortType, DateType,
    FloatType
)
from functions.functions import (
    load_cyclists,
    load_routes,
    load_activities,
    join_data,
    calculate_total_kilometers,
    calculate_daily_average,
    calculate_province_totals,
    get_top_cyclists_by_total_km,
    get_top_cyclists_by_daily_average
)

# =========================================================
# Load function tests
# =========================================================

def test_load_cyclists(spark_session, tmp_path):
    """
    Test cyclist loading function.
    """

    # 1. Crear CSV temporal
    file_path = tmp_path / "ciclista_test.csv"
    file_path.write_text("101,Juan Perez,San Jose\n102,Maria,Alajuela\n")
    
    # 2. Cargar CSV
    df = load_cyclists(spark_session, str(file_path))
    
    # 3. Validar
    assert df.count() == 2, "El dataframe debería tener 2 filas."
    assert "cedula" in df.columns, "El esquema no contiene 'cedula'."
    assert df.schema["cedula"].dataType.typeName() == "long", "Cédula debe ser tipo numérico."

def test_load_routes(spark_session, tmp_path):
    """
    Test routes loading function.
    """
    file_path = tmp_path / "ruta_test.csv"
    file_path.write_text("1,Ruta A,15.5\n2,Ruta B,20.0\n")
    
    df = load_routes(spark_session, str(file_path))
    
    assert df.count() == 2
    assert "kilometros" in df.columns
    assert df.schema["kilometros"].dataType.typeName() == "float"


def test_load_activities(spark_session, tmp_path):
    """
    Test activities loading function.
    """
    file_path = tmp_path / "actividad_test.csv"
    file_path.write_text("1,101,2026-05-01\n")
    
    df = load_activities(spark_session, str(file_path))
    
    assert df.count() == 1
    assert "fecha" in df.columns
    assert df.schema["fecha"].dataType.typeName() == "date"


# =========================================================
# Join tests
# =========================================================

def test_join_data(spark_session):
    """
    Test dataframe joins.
    """

# Creamos dataframes individuales en memoria
    c_df = spark_session.createDataFrame(
        [(1, "John Doe", "San Jose"), (2, "Jane", "Alajuela")], 
        ["cedula", "nombre_completo", "provincia"]
    )
    r_df = spark_session.createDataFrame(
        [(100, "Route A", 25.5)], 
        ["codigo_ruta", "nombre_ruta", "kilometros"]
    )
    a_df = spark_session.createDataFrame(
        [(100, 1, date(2026, 5, 1))], 
        ["codigo_ruta", "cedula", "fecha"]
    )

    # Execute join
    result_df = join_data(c_df, r_df, a_df)

    # Assertions
    assert result_df.count() == 2, "Debe tener 2 filas (Left Join preserva a Jane que no tiene actividades)"
    
    # Validar que John tiene sus kilómetros y Jane tiene null
    john_row = result_df.filter(result_df.cedula == 1).first()
    jane_row = result_df.filter(result_df.cedula == 2).first()
    
    assert john_row["kilometros"] == 25.5
    assert jane_row["kilometros"] is None


# =========================================================
# Aggregation tests
# =========================================================

def test_calculate_total_kilometers(spark_session):
    """
    Test total kilometers aggregation.
    """

    # Dataframe pre-unido (Simulando 2 actividades para John, 0 para Jane)
    data = [
        (1, "John", "San Jose", 100, date(2026, 5, 1), 10.0),
        (1, "John", "San Jose", 101, date(2026, 5, 2), 15.0),
        (2, "Jane", "Alajuela", None, None, None)
    ]
    df = spark_session.createDataFrame(data, ["cedula", "nombre_completo", "provincia", "codigo_ruta", "fecha", "kilometros"])
    
    res = calculate_total_kilometers(df).collect()
    
    res_dict = {row["cedula"]: row["kilometros_totales"] for row in res}
    assert res_dict[1] == 25.0, "Los kilómetros de John deben sumar 25.0"
    assert res_dict[2] == 0.0, "Los nulos deben convertirse en 0.0"


def test_calculate_daily_average(spark_session):
    """
    Test daily average calculation.
    """
    data = [
        # John corrió 10km y 20km el MISMO día (Total 30km en 1 día -> Promedio: 30)
        (1, "John", "San Jose", date(2026, 5, 1), 10.0),
        (1, "John", "San Jose", date(2026, 5, 1), 20.0),
        # Pedro corrió 10km en un día y 10km en otro (Total 20km en 2 días -> Promedio: 10)
        (3, "Pedro", "San Jose", date(2026, 5, 1), 10.0),
        (3, "Pedro", "San Jose", date(2026, 5, 2), 10.0)
    ]
    df = spark_session.createDataFrame(data, ["cedula", "nombre_completo", "provincia", "fecha", "kilometros"])
    
    res = calculate_daily_average(df).collect()
    res_dict = {row["cedula"]: row["promedio_diario"] for row in res}
    
    assert res_dict[1] == 30.0
    assert res_dict[3] == 10.0


def test_calculate_province_totals(spark_session):
    """
    Test province totals aggregation.
    """

    data = [
        ("San Jose", 10.0), ("San Jose", 15.0), 
        ("Alajuela", 20.0), ("Cartago", None)
    ]
    df = spark_session.createDataFrame(data, ["provincia", "kilometros"])
    
    res = calculate_province_totals(df).collect()
    res_dict = {row["provincia"]: row["kilometros_totales_provincia"] for row in res}
    
    assert res_dict["San Jose"] == 25.0
    assert res_dict["Alajuela"] == 20.0
    assert res_dict["Cartago"] == 0.0


# =========================================================
# Ranking tests
# =========================================================

def test_get_top_cyclists_by_total_km(spark_session):
    """
    Test top cyclists ranking by total kilometers.
    """

    # Data combinada final para evaluar el TOP 2 de San José
    data = [
        (1, "A", "SJ", 100.0, 50.0),  # #2 (100km totales)
        (2, "B", "SJ", 200.0, 100.0), # #1 (200km totales)
        (3, "C", "SJ", 50.0, 25.0),   # #3 (Debe ser filtrado)
    ]
    df = spark_session.createDataFrame(data, ["cedula", "nombre_completo", "provincia", "kilometros_totales", "promedio_diario"])
    
    # Ejecutar con límite de 2
    res = get_top_cyclists_by_total_km(df, top_n=2).collect()
    
    assert len(res) == 2, "Debe limitar al top N especificado"
    assert res[0]["cedula"] == 2, "El ciclista B (200km) debe ser el primero"
    assert res[1]["cedula"] == 1, "El ciclista A (100km) debe ser el segundo"


def test_get_top_cyclists_by_daily_average(spark_session):
    """
    Test top cyclists ranking by daily average.
    """

    data = [
        # Empate en promedio diario (50.0). Desempata el que tenga MÁS km totales
        (1, "Empatado 1", "SJ", 100.0, 50.0), # #2
        (2, "Empatado 2", "SJ", 150.0, 50.0), # #1 (Gana por km totales)
    ]
    df = spark_session.createDataFrame(data, ["cedula", "nombre_completo", "provincia", "kilometros_totales", "promedio_diario"])
    
    res = get_top_cyclists_by_daily_average(df, top_n=5).collect()
    
    assert res[0]["cedula"] == 2, "El desempate por kilómetros totales falló"
    assert res[1]["cedula"] == 1


# =========================================================
# Edge case tests
# =========================================================

def test_empty_dataframe(spark_session):
    """
    Test empty dataframe behavior.
    """
# 1. Definimos un esquema estricto en lugar de solo nombres
    schema_vacio = StructType([
        StructField("cedula", LongType(), True),
        StructField("nombre_completo", StringType(), True),
        StructField("provincia", StringType(), True),
        StructField("codigo_ruta", ShortType(), True),
        StructField("fecha", DateType(), True),
        StructField("kilometros", FloatType(), True)
    ])
    
    # 2. Creamos el DataFrame vacío con su estructura formal
    empty_df = spark_session.createDataFrame([], schema=schema_vacio)
    
    # 3. Validamos que nuestras funciones no colapsen al recibir 0 filas
    res_totals = calculate_total_kilometers(empty_df)
    
    # El resultado de procesar una tabla vacía debe ser otra tabla vacía, no un error (Crash)
    assert res_totals.count() == 0, "El procesamiento de un dataframe vacío falló."


def test_null_values(spark_session):
    """
    Test null handling.
    """

# 1. Definimos el esquema estricto para que Spark no tenga que adivinar los 'None'
    schema = StructType([
        StructField("cedula", LongType(), True),
        StructField("nombre_completo", StringType(), True),
        StructField("provincia", StringType(), True),
        StructField("codigo_ruta", ShortType(), True),
        StructField("fecha", DateType(), True),
        StructField("kilometros", FloatType(), True)
    ])

    # 2. Simulamos el resultado de un Left Join donde no hubo coincidencia en rutas
    data = [(1, "Juan", "SJ", None, None, None)]
    
    # 3. Creamos el DataFrame usando el esquema formal
    df = spark_session.createDataFrame(data, schema=schema)
    
    # 4. Validamos que nuestra función convierta correctamente esos nulos en ceros
    res = calculate_total_kilometers(df).collect()
    assert res[0]["kilometros_totales"] == 0.0, "Los valores nulos no fueron reemplazados por 0"


def test_duplicate_activities(spark_session):
    """
    Test duplicated activity handling.
    """

    data = [
        # Misma persona, misma fecha, misma ruta
        (1, "Juan", "SJ", 100, date(2026, 5, 1), 10.0),
        (1, "Juan", "SJ", 100, date(2026, 5, 1), 10.0)
    ]
    df = spark_session.createDataFrame(data, ["cedula", "nombre_completo", "provincia", "codigo_ruta", "fecha", "kilometros"])
    
    # El promedio diario de 1 día que sumó 20km debe ser 20.0
    res = calculate_daily_average(df).collect()
    assert res[0]["promedio_diario"] == 20.0, "No manejó correctamente múltiples actividades en el mismo día"
