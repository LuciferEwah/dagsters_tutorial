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
def sensor_tiempos_detallados(context):
    run = context.dagster_run
    
    if not run:
        context.log.info("No se encontró información de la ejecución")
        return None
    
    try:
        context.log.info("=" * 80)
        context.log.info("ANÁLISIS DE TIEMPOS DE EJECUCIÓN".center(80))
        context.log.info("=" * 80)
        
        # Obtener eventos STEP_START y STEP_SUCCESS
        start_events = context.instance.get_records_for_run(
            run_id=run.run_id, 
            of_type=dg.DagsterEventType.STEP_START
        )
        
        success_events = context.instance.get_records_for_run(
            run_id=run.run_id, 
            of_type=dg.DagsterEventType.STEP_SUCCESS
        )
        
        # Extraer información de tiempos
        step_times = {}
        
        # Procesar eventos de inicio
        for event in start_events:
            if hasattr(event, 'event_log_entry') and event.event_log_entry:
                entry = event.event_log_entry
                if (hasattr(entry, 'dagster_event') and entry.dagster_event and
                    hasattr(entry.dagster_event, 'step_key') and entry.dagster_event.step_key):
                    
                    step_key = entry.dagster_event.step_key
                    timestamp = entry.timestamp
                    
                    if step_key not in step_times:
                        step_times[step_key] = {}
                    
                    step_times[step_key]['start'] = timestamp
        
        # Procesar eventos de finalización
        for event in success_events:
            if hasattr(event, 'event_log_entry') and event.event_log_entry:
                entry = event.event_log_entry
                if (hasattr(entry, 'dagster_event') and entry.dagster_event and
                    hasattr(entry.dagster_event, 'step_key') and entry.dagster_event.step_key):
                    
                    step_key = entry.dagster_event.step_key
                    timestamp = entry.timestamp
                    
                    if step_key not in step_times:
                        step_times[step_key] = {}
                    
                    step_times[step_key]['end'] = timestamp
                    
                    # Extraer duración desde el evento directamente
                    if (hasattr(entry.dagster_event, 'event_specific_data') and
                        hasattr(entry.dagster_event.event_specific_data, 'duration_ms')):
                        step_times[step_key]['duration_ms'] = entry.dagster_event.event_specific_data.duration_ms
        
        # Mostrar tiempos de ejecución
        if step_times:
            # Ordenar pasos en el orden de ejecución
            ordered_steps = []
            for step_key in ["seed_value", 
                            "my_parallel_graph.op_a", 
                            "my_parallel_graph.op_b", 
                            "my_parallel_graph.op_c", 
                            "my_parallel_graph.sum_op", 
                            "print_valor"]:
                if step_key in step_times:
                    ordered_steps.append(step_key)
            
            # Añadir cualquier otro paso que no esté en la lista predefinida
            for step_key in step_times:
                if step_key not in ordered_steps:
                    ordered_steps.append(step_key)
            
            context.log.info("\nTiempos de ejecución por paso:")
            context.log.info(f"{'PASO':<30} | {'INICIO':<20} | {'FIN':<20} | {'DURACIÓN':<12} | {'TIEMPO REAL'}")
            context.log.info("-" * 100)
            
            # Variables para calcular tiempo total
            first_timestamp = None
            last_timestamp = None
            
            for step_key in ordered_steps:
                times = step_times[step_key]
                start_str = "N/A"
                end_str = "N/A"
                duration_str = "N/A"
                duration_ms_str = "N/A"
                
                if 'start' in times:
                    start_time = times['start']
                    start_dt = datetime.fromtimestamp(start_time)
                    start_str = start_dt.strftime('%H:%M:%S.%f')[:-3]
                    
                    # Actualizar primer timestamp
                    if first_timestamp is None or start_time < first_timestamp:
                        first_timestamp = start_time
                
                if 'end' in times:
                    end_time = times['end']
                    end_dt = datetime.fromtimestamp(end_time)
                    end_str = end_dt.strftime('%H:%M:%S.%f')[:-3]
                    
                    # Actualizar último timestamp
                    if last_timestamp is None or end_time > last_timestamp:
                        last_timestamp = end_time
                    
                    if 'start' in times:
                        duration = end_time - times['start']
                        duration_str = f"{duration:.3f}s"
                
                if 'duration_ms' in times:
                    duration_ms_str = f"{times['duration_ms']:.2f}ms"
                
                context.log.info(f"{step_key:<30} | {start_str:<20} | {end_str:<20} | {duration_str:<12} | {duration_ms_str}")
            
            # Mostrar duración total
            if first_timestamp and last_timestamp:
                total_duration = last_timestamp - first_timestamp
                context.log.info("-" * 100)
                context.log.info(f"{'TIEMPO TOTAL':<30} | {datetime.fromtimestamp(first_timestamp).strftime('%H:%M:%S.%f')[:-3]:<20} | {datetime.fromtimestamp(last_timestamp).strftime('%H:%M:%S.%f')[:-3]:<20} | {total_duration:.3f}s")
        
        # Buscar mensajes de log específicos
        all_records = context.instance.get_records_for_run(run_id=run.run_id)
        
        info_messages = []
        for record in all_records:
            if hasattr(record, 'event_log_entry') and record.event_log_entry:
                entry = record.event_log_entry
                
                if hasattr(entry, 'message') and entry.message and hasattr(entry, 'timestamp'):
                    if any(pattern in entry.message for pattern in [
                        "Generando valor semilla", 
                        "modificó a", 
                        "Sumando valores", 
                        "Valor:"
                    ]):
                        info_messages.append({
                            'timestamp': entry.timestamp,
                            'message': entry.message
                        })
        
        if info_messages:
            # Ordenar por timestamp
            info_messages.sort(key=lambda x: x['timestamp'])
            
            context.log.info("\nMensajes de log de operaciones:")
            context.log.info(f"{'TIMESTAMP':<20} | {'MENSAJE'}")
            context.log.info("-" * 80)
            
            for msg in info_messages:
                timestamp_dt = datetime.fromtimestamp(msg['timestamp'])
                timestamp_str = timestamp_dt.strftime('%H:%M:%S.%f')[:-3]
                
                context.log.info(f"{timestamp_str:<20} | {msg['message']}")
        
        context.log.info("=" * 80)
        
    except Exception as e:
        context.log.error(f"Error: {str(e)}")
        import traceback
        context.log.error(traceback.format_exc())
    
    return None
# Definiciones de Dagster
defs = dg.Definitions(
    assets=[initial_dataframe, productos_procesados, final_dataframe],
    jobs=[analisis_ventas_job, new_job],
    sensors=[sensor_tiempos_detallados]
)