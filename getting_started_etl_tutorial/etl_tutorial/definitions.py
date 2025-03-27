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
def op_c(context, seed_value: int):
    modified_value = seed_value // 2
    context.log.info(f"op_c modificó a: {modified_value}")
    return modified_value
@dg.op
def sum_op(context, a, b, c) -> int:
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
def print_valor(context, valor: int) -> int:
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
import dagster as dg
import pandas as pd
import re

import dagster as dg
import pandas as pd
import re
from datetime import datetime

@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[new_job],
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def sensor_analisis_detallado(context):
    run = context.dagster_run
    
    if not run:
        context.log.info("No se encontró información de la ejecución")
        return None
    
    try:
        # Eventos del Motor para obtener tiempo total de ejecución
        engine_events = context.instance.get_records_for_run(
            run_id=run.run_id, 
            of_type={dg.DagsterEventType.ENGINE_EVENT}
        )
        
        # Extraer tiempo total de ejecución
        total_duration = None
        for event in engine_events.records:
            if "exiting" in event.event_log_entry.message:
                match = re.search(r'after ([\d.]+)s', event.event_log_entry.message)
                if match:
                    total_duration = float(match.group(1))
                    break

        # Preparar contenedores para DataFrames
        input_data = []
        output_data = []
        failure_data = []
        step_duration_data = []

        # Obtener eventos de entrada
        input_events = context.instance.get_records_for_run(
            run_id=run.run_id, 
            of_type={dg.DagsterEventType.STEP_INPUT}
        )
        
        # Obtener eventos de salida para cálculo de duraciones
        output_events = context.instance.get_records_for_run(
            run_id=run.run_id, 
            of_type={dg.DagsterEventType.STEP_OUTPUT}
        )

        # Crear un diccionario de tiempos de salida de pasos
        output_times = {
            event.event_log_entry.step_key: event.event_log_entry.timestamp
            for event in output_events.records
        }

        # Recopilar datos de eventos de entrada con cálculo de duración
        for event in input_events.records:
            entry = event.event_log_entry
            step_key = entry.step_key
            input_timestamp = entry.timestamp

            # Calcular duración del paso
            output_timestamp = output_times.get(step_key)
            duration = output_timestamp - input_timestamp if output_timestamp else None

            input_data.append({
                'Step': step_key,
                'Input Name': entry.dagster_event.event_specific_data.input_name,
                'Type_': entry.user_message,  # Esto mostrará el mensaje original,
                'Type': entry.dagster_event.event_specific_data.type_check_data.label,
                'Type Check': '✅ Passed' if entry.dagster_event.event_specific_data.type_check_data.success else '❌ Failed'
            })

            # Recopilar datos de duración de pasos
            if duration is not None:
                step_duration_data.append({
                    'Step': step_key,
                    'Duration (s)': round(duration, 4)
                })
        
        # Recopilar datos de eventos de salida
        for event in output_events.records:
            entry = event.event_log_entry
            output_data.append({
                'Step': entry.step_key,
                'Output Name': entry.dagster_event.event_specific_data.step_output_handle.output_name,
                'Type_': entry.user_message,  # Mensaje original con el tipo
                'Type': entry.dagster_event.event_specific_data.type_check_data.label,
                'Type Check': '✅ Passed' if entry.dagster_event.event_specific_data.type_check_data.success else '❌ Failed'
            })


        # Convertir a DataFrames
        df_input = pd.DataFrame(input_data) if input_data else pd.DataFrame()
        df_output = pd.DataFrame(output_data) if output_data else pd.DataFrame()
        df_failure = pd.DataFrame(failure_data) if failure_data else pd.DataFrame()
        df_step_duration = pd.DataFrame(step_duration_data) if step_duration_data else pd.DataFrame()

        # Imprimir información del run
        context.log.info(f"Run ID: {run.run_id}")
        context.log.info(f"Job Name: {run.job_name}")
        context.log.info(f"Total Job Duration: {total_duration} seconds")

        # Imprimir DataFrames
        if not df_input.empty:
            context.log.info("\n📥 EVENTOS DE ENTRADA:")
            context.log.info(df_input.to_string(index=False))

        if not df_output.empty:
            context.log.info("\n📤 EVENTOS DE SALIDA:")
            context.log.info(df_output.to_string(index=False))

        if not df_step_duration.empty:
            context.log.info("\n⏱️ DURACIÓN DE PASOS:")
            context.log.info(df_step_duration.to_string(index=False))

        if not df_failure.empty:
            context.log.info("\n❌ EVENTOS DE FALLA:")
            context.log.info(df_failure.to_string(index=False))

    except Exception as e:
        context.log.error(f"Error en el análisis: {str(e)}")
        import traceback
        context.log.error(traceback.format_exc())
    
    return None


import pandas as pd
import dagster as dg
import re

@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.FAILURE,
    monitored_jobs=[new_job],
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def sensor_tiempos_error_df(context):
    run = context.dagster_run
    
    if not run:
        context.log.info("No se encontró información de la ejecución")
        return None
    
    try:
        # Preparar listas para el DataFrame
        errores = []
        
        # Obtener eventos de falla
        failure_events = context.instance.get_records_for_run(
            run_id=run.run_id, 
            of_type={dg.DagsterEventType.STEP_FAILURE}
        )
        
        context.log.info("=" * 80)
        context.log.info("ANÁLISIS DETALLADO DE ERROR".center(80))
        context.log.info("=" * 80)
        
        # Información básica del run
        run_id = run.run_id
        job_name = run.job_name
        
        # Analizar eventos de falla
        for event in failure_events.records:
            error_info = event.event_log_entry.dagster_event.event_specific_data.error
            
            # Extraer detalles del error
            if error_info and error_info.cause:
                # Obtener el paso donde ocurrió el error
                step_key = event.event_log_entry.step_key
                
                # Inicializar variables
                error_location = None
                error_message = error_info.cause.message.strip() if error_info.cause.message else "No disponible"
                error_code = error_info.cause.message.split(':')[-1].strip() if error_info.cause.message else "No disponible"
                
                # Ubicación del Error
                error_stack = error_info.cause.stack
                if error_stack:
                    # Buscar la línea de código específica
                    for line in error_stack:
                        if 'definitions.py' in line and 'line' in line:
                            error_location = line.strip()
                            break
                
                # Intentar extraer el número de línea con regex
                linea_num = None
                if error_location:
                    match = re.search(r'line (\d+)', error_location)
                    if match:
                        linea_num = int(match.group(1))
                
                # Compilar información para el dataframe
                error_data = {
                    'run_id': run_id,
                    'job_name': job_name,
                    'step_key': step_key,
                    'error_location': error_location,
                    'linea_num': linea_num,
                    'error_message': error_message,
                    'error_code': error_code,
                    'timestamp': event.event_log_entry.timestamp
                }
                
                errores.append(error_data)
        
        # Crear DataFrame
        if errores:
            df_errores = pd.DataFrame(errores)
            
            # Convertir timestamp a datetime
            df_errores['timestamp'] = pd.to_datetime(df_errores['timestamp'], unit='s')
            
            # Ordenar por timestamp
            df_errores = df_errores.sort_values('timestamp')
            
            # Mostrar DataFrame
            context.log.info("\n📊 DataFrame de Errores:")
            context.log.info(f"\n{df_errores.to_string()}")
            
            # Guardar DataFrame (opcional)
            # df_errores.to_csv(f"errores_{run_id}.csv", index=False)
            
            return None
        else:
            context.log.info("No se encontraron errores para analizar")
            return None
    
    except Exception as e:
        context.log.error(f"Error en el análisis: {str(e)}")
        import traceback
        context.log.error(traceback.format_exc())
        return None

# Definiciones de Dagster
defs = dg.Definitions(
    assets=[initial_dataframe, productos_procesados, final_dataframe],
    jobs=[analisis_ventas_job, new_job],
    sensors=[sensor_tiempos_error_df,sensor_analisis_detallado]
)