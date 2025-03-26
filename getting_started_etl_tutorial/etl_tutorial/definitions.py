import dagster as dg
from dagster import AssetMaterialization, Output, Out
import pandas as pd
import numpy as np

# Define el asset inicial que crea un DataFrame
@dg.asset
def initial_dataframe() -> pd.DataFrame:
    """Crea un DataFrame inicial con datos de ventas"""
    # Crear un dataframe de ejemplo con ventas por producto
    df = pd.DataFrame({
        'producto': ['A', 'B', 'C', 'D', 'E'],
        'ventas': [150, 200, 120, 300, 250],
        'costo': [50, 80, 40, 100, 90]
    })
    return df

# Define operación para calcular el margen
@dg.op
def calcular_margen(context, df: pd.DataFrame) -> pd.DataFrame:
    """Calcula el margen de ganancia para cada producto"""
    context.log.info(f"Calculando margen para {len(df)} productos")
    
    # Añadir columna de margen
    df['margen'] = df['ventas'] - df['costo']
    df['margen_porcentaje'] = (df['margen'] / df['ventas'] * 100).round(2)
    
    return df

# Define operación para filtrar productos rentables
@dg.op
def filtrar_rentables(context, df: pd.DataFrame) -> pd.DataFrame:
    """Filtra productos con margen mayor al 50%"""
    context.log.info(f"Filtrando productos rentables")
    
    df_rentable = df[df['margen_porcentaje'] > 50].copy()
    context.log.info(f"Encontrados {len(df_rentable)} productos rentables")
    
    return df_rentable

# Define el op final que materializa el DataFrame como asset
@dg.op(out={"result": Out(is_required=False)})
def materializar_resultados(context, df: pd.DataFrame):
    """Materializa el DataFrame de resultados como un asset"""
    context.log.info(f"Materializando resultados para {len(df)} productos")
    
    # Calcular métricas generales
    promedio_margen = df['margen'].mean()
    total_ventas = df['ventas'].sum()
    
    # Materializar el asset con metadatos - convertir valores NumPy a Python nativos
    yield AssetMaterialization(
        asset_key="productos_rentables",
        description="DataFrame con productos rentables",
        metadata={
            "cantidad_productos": int(len(df)),
            "promedio_margen": float(promedio_margen),
            "total_ventas": float(total_ventas)
        }
    )
    
    # También puedes retornar el DataFrame si es necesario
    yield Output(df, output_name="result")

import random
# Definir un asset
@dg.asset
def seed_value(context):
    value = random.randint(1, 100)
    context.log.info(f"Generando valor semilla: {value}")
    return value

@dg.op
def op_a(context, seed_value):
    modified_value = seed_value * 2
    context.log.info(f"op_a modificó a: {modified_value}")
    return modified_value

@dg.op
def op_b(context, seed_value):
    modified_value = seed_value + 10
    context.log.info(f"op_b modificó a: {modified_value}")
    return modified_value

@dg.op
def op_c(context, seed_value):
    modified_value = seed_value // 2
    context.log.info(f"op_c modificó a: {modified_value}")
    return modified_value
@dg.op
def sum_op(context, a, b, c):
    context.log.info(f"Sumando valores: {a}, {b}, {c}")
    return a + b + c
# Definir un grafo que organiza la ejecución
@dg.graph
def my_parallel_graph(asset_init_retorna):
    a = op_a(asset_init_retorna)
    b = op_b(asset_init_retorna)
    c = op_c(asset_init_retorna)
    return sum_op(a, b, c)

@dg.asset
def print_valor(context, valor: int):
    context.log.info(f"Valor: {valor}")
    return valor


# Define el asset procesado intermedio
@dg.asset
def productos_procesados(context, initial_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Asset intermedio que procesa los datos aplicando cálculos y filtros"""
    # Calcular margen
    df_con_margen = calcular_margen(context, initial_dataframe)
    
    # Filtrar productos rentables
    df_rentable = filtrar_rentables(context, df_con_margen)
    
    # Registrar metadatos
    context.log.info(f"Procesados {len(df_rentable)} productos rentables")
    
    return df_rentable

# Define el asset final
@dg.asset
def final_dataframe(context, productos_procesados: pd.DataFrame) -> None:
    """Asset final que materializa la salida del procesamiento"""
    context.log.info(f"Materializando DataFrame final con {len(productos_procesados)} productos")
    
    # No necesitamos retornar nada, pero podemos registrar metadatos
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(productos_procesados)),
            "productos": dg.MetadataValue.md(productos_procesados.head().to_markdown())
        }
    )

# Definir el job con el grafo paralelo correctamente
@dg.job
def analisis_ventas_job():
    df_inicial = initial_dataframe()
    df_con_margen = calcular_margen(df_inicial)
    df_rentable = filtrar_rentables(df_con_margen)
    materializar_resultados(df_rentable)
    


    
# Configura el job para usar el ejecutor multiproceso
@dg.job(executor_def=dg.multiprocess_executor)
def new_job():
    a = seed_value()
    funcion = my_parallel_graph(a)
    print_valor(funcion)


import dagster as dg
from datetime import datetime, timedelta

@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[new_job],
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def sensor_de_exito_diario(context):
    # Imprimir los atributos disponibles en el contexto
    context.log.info("Atributos disponibles en el contexto:")
    for attr in dir(context):
        if not attr.startswith('_'):  # Ignorar atributos privados
            context.log.info(f"  - {attr}")

    # Intentar acceder a la instancia
    context.log.info("Accediendo a la instancia:")
    instance = context.instance
    context.log.info(f"  Instance type: {type(instance)}")
    
    # Intentar acceder al run actual
    context.log.info("Accediendo a la información de la ejecución:")
    run = context.dagster_run if hasattr(context, 'dagster_run') else None
    context.log.info(f"  Run: {run}")
    
    return None
    
# Definiciones de Dagster
defs = dg.Definitions(
    assets=[initial_dataframe, productos_procesados, final_dataframe],
    jobs=[analisis_ventas_job, new_job],
    sensors=[sensor_de_exito_diario]
)