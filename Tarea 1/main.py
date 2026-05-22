# =========================================================
# Imports
# =========================================================

import sys
import os

from pyspark.sql import SparkSession

# =========================================================
# Custom imports
# =========================================================

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
# Constants
# =========================================================

# EXAMPLE_CONSTANT = "VALUE"
NUMBER_OF_ARGUMENTS = 4

# =========================================================
# Helper functions
# =========================================================

def validate_arguments():
    """
    Validate command-line arguments.
    """

    # Validate number of arguments
    if len(sys.argv) != NUMBER_OF_ARGUMENTS:
        print("Error: Estructura incorrecta. Uso: spark-submit main.py ciclista.csv ruta.csv actividad.csv")
        sys.exit(1)  # Detiene la ejecución del programa con un código de error
    # Validate file exist
    # Validate file extension
    files_to_validate = sys.argv[1:]

    for file in files_to_validate:
        if not file.lower().endswith('.csv'):
            print(f"Error: El archivo '{file}' no es un formato CSV válido.")
            sys.exit(1)
        
        if not os.path.exists(f"data/{file}"):
            print(f"Error: No se encontró el archivo '{file}'.")
            sys.exit(1)
    
    print("¡Validaciones exitosas! Procediendo a iniciar Spark...")


# =========================================================
# Main function
# =========================================================

def main():

    # -----------------------------------------------------
    # Validate arguments
    # -----------------------------------------------------

    validate_arguments()

    # -----------------------------------------------------
    # Read command-line arguments
    # -----------------------------------------------------

    # Example:
    #
    cyclist_file = sys.argv[1]
    routes_file = sys.argv[2]
    activities_file = sys.argv[3]

    # -----------------------------------------------------
    # Create Spark session
    # -----------------------------------------------------

    spark = SparkSession.builder \
        .appName("Tarea1") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # -----------------------------------------------------
    # Load input data
    # -----------------------------------------------------

    #
    cyclists_df = load_cyclists(spark, f"data/{cyclist_file}")
    cyclists_df.show()
    routes_df = load_routes(spark, f"data/{routes_file}")
    routes_df.show()
    activities_df = load_activities(spark, f"data/{activities_file}")
    activities_df.show()

    # -----------------------------------------------------
    # Data joins
    # -----------------------------------------------------

    #
    joined_df = join_data(cyclists_df, routes_df, activities_df)
    joined_df.show()
    #

    # -----------------------------------------------------
    # Intermediate aggregations
    # -----------------------------------------------------

    # TODO:
    #
    # totals_df = ...
    totales_df = calculate_total_kilometers(joined_df)
    totales_df.show()
    promedios_df = calculate_daily_average(joined_df)
    promedios_df.show()
    cyclist_stats_df = totales_df.join(
        promedios_df, 
        on=["cedula", "nombre_completo"], 
        how="inner"
    )
    cyclist_stats_df.show()
    totals_by_province_df = calculate_province_totals(joined_df)
    totals_by_province_df.show()
    # -----------------------------------------------------
    # Final calculations
    # -----------------------------------------------------

    # TODO:
    #
    # top_cyclists_df = ...
    top_by_total_km_df = get_top_cyclists_by_total_km(cyclist_stats_df, top_n=5)
    top_by_daily_avg_df = get_top_cyclists_by_daily_average(cyclist_stats_df, top_n=5)

    # -----------------------------------------------------
    # Show results
    # -----------------------------------------------------

    # TODO:
    #
    # top_cyclists_df.show()
    print("=========================================================")
    print(" RESULTADOS: AGREGACIONES PARCIALES Y TOP DE CICLISTAS")
    print("=========================================================")
    
    print("\n[1] TOTAL DE KILÓMETROS POR PROVINCIA:")
    totals_by_province_df.show(truncate=False)

    print("\n[2] TOP 5 CICLISTAS POR KILÓMETROS TOTALES (Por Provincia):")
    top_by_total_km_df.show(truncate=False)

    print("\n[3] TOP 5 CICLISTAS POR PROMEDIO DIARIO (Por Provincia):")
    top_by_daily_avg_df.show(truncate=False)

    # -----------------------------------------------------
    # Export results (optional)
    # -----------------------------------------------------

    # TODO:
    #
    # top_cyclists_df.write...
    top_by_total_km_df.coalesce(1).write.csv(
        "data/output_top_km", 
        header=True, 
        mode="overwrite"
    )
    
    top_by_daily_avg_df.coalesce(1).write.csv(
        "data/output_top_avg", 
        header=True, 
        mode="overwrite"
    )

    # -----------------------------------------------------
    # Stop Spark session
    # -----------------------------------------------------

    spark.stop()

# -----------------------------------------------------
# Entry point
# -----------------------------------------------------

if __name__ == "__main__":
    main()
