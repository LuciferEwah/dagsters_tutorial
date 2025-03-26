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
def sensor_de_exito_detallado(context):
    run = context.dagster_run
    
    if not run:
        context.log.info("No se encontró información de la ejecución")
        return None
    
    # Información básica
    context.log.info("=" * 80)
    context.log.info("DETALLES DE LA EJECUCIÓN EXITOSA".center(80))
    context.log.info("=" * 80)
    
    # Sección 1: Información principal
    context.log.info("\n📋 INFORMACIÓN GENERAL")
    context.log.info("-" * 80)
    context.log.info(f"Job:                  {run.job_name}")
    context.log.info(f"Run ID:               {run.run_id}")
    context.log.info(f"Estado:               {run.status}")
    context.log.info(f"Snapshot ID (Job):    {run.job_snapshot_id}")
    context.log.info(f"Snapshot ID (Plan):   {run.execution_plan_snapshot_id}")
    
    # Sección 2: Tiempos de ejecución
    if hasattr(run, 'start_time') and run.start_time:
        start_time = datetime.fromtimestamp(run.start_time)
        context.log.info(f"\n⏱️ TIEMPO DE EJECUCIÓN")
        context.log.info("-" * 80)
        context.log.info(f"Inicio:               {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if hasattr(run, 'end_time') and run.end_time:
            end_time = datetime.fromtimestamp(run.end_time)
            duracion = timedelta(seconds=run.end_time - run.start_time)
            context.log.info(f"Fin:                  {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            context.log.info(f"Duración:             {duracion}")
    
    # Sección 3: Pasos ejecutados
    if hasattr(run, 'step_keys_to_execute') and run.step_keys_to_execute:
        context.log.info(f"\n🔄 FLUJO DE EJECUCIÓN ({len(run.step_keys_to_execute)} pasos)")
        context.log.info("-" * 80)
        for i, step in enumerate(run.step_keys_to_execute, 1):
            context.log.info(f"  {i}. {step}")
    
    # Sección 4: Tags y metadatos
    if hasattr(run, 'tags') and run.tags:
        context.log.info(f"\n🏷️ TAGS ({len(run.tags)} tags)")
        context.log.info("-" * 80)
        for key, value in run.tags.items():
            # Si parece ser JSON, intenta formatearlo mejor
            if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                try:
                    import json
                    parsed = json.loads(value)
                    context.log.info(f"  {key}:")
                    formatted = json.dumps(parsed, indent=4)
                    for line in formatted.split('\n'):
                        context.log.info(f"    {line}")
                except:
                    context.log.info(f"  {key}: {value}")
            else:
                context.log.info(f"  {key}: {value}")
    
    # Sección 5: Eventos (usando los parámetros correctos)
    try:
        context.log.info(f"\n📊 EVENTOS")
        context.log.info("-" * 80)
        
        # Obtener los logs para esta ejecución
        logs = context.instance.get_logs_for_run(run.run_id)
        
        if logs:
            context.log.info(f"Total de logs: {len(logs)}")
            
            # Contar eventos por tipo
            event_types = {}
            for log in logs:
                if hasattr(log, 'dagster_event') and log.dagster_event:
                    event_type = log.dagster_event.event_type.value
                    event_types[event_type] = event_types.get(event_type, 0) + 1
            
            if event_types:
                context.log.info("\nResumen de eventos:")
                for event_type, count in sorted(event_types.items(), key=lambda x: -x[1]):
                    context.log.info(f"  {event_type}: {count}")
        else:
            context.log.info("No se encontraron logs para esta ejecución")
    except Exception as e:
        context.log.error(f"Error al obtener logs: {str(e)}")
    
    # Sección 6: Ejecuciones recientes
    try:
        context.log.info(f"\n📈 EJECUCIONES RECIENTES")
        context.log.info("-" * 80)
        
        # Obtener ejecuciones recientes
        runs = context.instance.get_runs(
            filters=dg.RunsFilter(job_name=run.job_name),
            limit=5
        )
        
        if runs:
            context.log.info(f"Últimas {len(runs)} ejecuciones del job '{run.job_name}':")
            context.log.info(f"{'ID':<12} | {'ESTADO':<10} | {'INICIO':<19} | {'FIN':<19} | {'DURACIÓN':<12}")
            context.log.info("-" * 80)
            
            for run_item in runs:
                start_str = "N/A"
                end_str = "N/A"
                duration_str = "N/A"
                
                if hasattr(run_item, 'start_time') and run_item.start_time:
                    start_str = datetime.fromtimestamp(run_item.start_time).strftime('%Y-%m-%d %H:%M:%S')
                
                if hasattr(run_item, 'end_time') and run_item.end_time:
                    end_str = datetime.fromtimestamp(run_item.end_time).strftime('%Y-%m-%d %H:%M:%S')
                
                if hasattr(run_item, 'start_time') and run_item.start_time and hasattr(run_item, 'end_time') and run_item.end_time:
                    duration = timedelta(seconds=run_item.end_time - run_item.start_time)
                    duration_str = str(duration)
                
                run_id_short = run_item.run_id[:10]
                context.log.info(f"{run_id_short:<12} | {run_item.status.value:<10} | {start_str:<19} | {end_str:<19} | {duration_str:<12}")
        else:
            context.log.info(f"No se encontraron ejecuciones recientes para el job '{run.job_name}'")
    except Exception as e:
        context.log.error(f"Error al obtener ejecuciones recientes: {str(e)}")
    
    # Sección 7: Origen del código
    try:
        context.log.info(f"\n📂 ORIGEN DEL CÓDIGO")
        context.log.info("-" * 80)
        
        if hasattr(run, 'job_code_origin') and run.job_code_origin:
            origin = run.job_code_origin
            context.log.info(f"Job: {getattr(origin, 'job_name', 'N/A')}")
            
            if hasattr(origin, 'repository_origin') and origin.repository_origin:
                repo_origin = origin.repository_origin
                context.log.info(f"Ejecutable: {getattr(repo_origin, 'executable_path', 'N/A')}")
                
                if hasattr(repo_origin, 'code_pointer') and repo_origin.code_pointer:
                    code_pointer = repo_origin.code_pointer
                    context.log.info(f"Módulo: {getattr(code_pointer, 'module', 'N/A')}")
                    context.log.info(f"Función: {getattr(code_pointer, 'fn_name', 'N/A')}")
                    context.log.info(f"Directorio: {getattr(code_pointer, 'working_directory', 'N/A')}")
        
        if hasattr(run, 'remote_job_origin') and run.remote_job_origin:
            remote_origin = run.remote_job_origin
            context.log.info(f"\nOrigen remoto:")
            
            if hasattr(remote_origin, 'repository_origin') and remote_origin.repository_origin:
                repo_origin = remote_origin.repository_origin
                
                if hasattr(repo_origin, 'code_location_origin') and repo_origin.code_location_origin:
                    location = repo_origin.code_location_origin
                    context.log.info(f"Ubicación: {getattr(location, 'location_name', 'N/A')}")
                    context.log.info(f"Host: {getattr(location, 'host', 'N/A')}")
                    context.log.info(f"Puerto: {getattr(location, 'port', 'N/A')}")
                    
                    if hasattr(location, 'additional_metadata') and location.additional_metadata:
                        meta = location.additional_metadata
                        for key, value in meta.items():
                            context.log.info(f"  {key}: {value}")
    except Exception as e:
        context.log.error(f"Error al obtener origen del código: {str(e)}")
    
    context.log.info("=" * 80)
    
    return None
    
# Definiciones de Dagster
defs = dg.Definitions(
    assets=[initial_dataframe, productos_procesados, final_dataframe],
    jobs=[analisis_ventas_job, new_job],
    sensors=[sensor_de_exito_detallado]
)