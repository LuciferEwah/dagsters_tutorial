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


# Define el asset inicial que crea un DataFrame
@dg.asset
def final_dataframe(context, df: pd.DataFrame) -> None:
    """Crea un DataFrame inicial con datos de ventas"""
    context.log.info(f"Materializando largo de df: {df}")
    return None

# Define el job que conecta todo el pipeline
@dg.job
def analisis_ventas_job():
    df_inicial = initial_dataframe()
    df_con_margen = calcular_margen(df_inicial)
    df_rentable = filtrar_rentables(df_con_margen)
    materializar = materializar_resultados(df_rentable)
    final_dataframe(materializar)

# Definiciones de Dagster
defs = dg.Definitions(
    assets=[initial_dataframe],
    jobs=[analisis_ventas_job]
)
