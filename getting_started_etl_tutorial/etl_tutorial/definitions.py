import dagster as dg
import pandas as pd
from dagster import OpExecutionContext, asset, op, job, In, Out, graph, AssetIn, AssetKey

# Asset inicial que crea un dataframe simple
@dg.asset(
    compute_kind="python",
    group_name="datos_iniciales",
    key="datos_iniciales"
)
def datos_iniciales():
    # Creamos un dataframe simple con algunos números
    data = {
        'id': [0, 1],
        'valor': [1, 1]
    }
    df = pd.DataFrame(data)
    
    # Calculamos la suma total
    suma_total = df['valor'].sum()
    
    # Devolvemos el resultado con metadatos
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "preview": dg.MetadataValue.md(df.to_markdown(index=False)),
            "suma_total": dg.MetadataValue.int(suma_total)
        },
        value=suma_total  # Este valor será utilizado directamente por el op
    )

# Op que recibe el valor del asset y lo pasa a las operaciones
@dg.op(
    name="obtener_valor_inicial",
    ins={"asset_valor": In(int)},
    out=Out(int)
)
def obtener_valor_inicial(context: OpExecutionContext, asset_valor: int) -> int:
    context.log.info(f"Valor obtenido del asset: {asset_valor}")
    return asset_valor

# Operación que suma diez al valor
@dg.op(
    name="sumar_diez",
    ins={"valor": In(int)},
    out=Out(int)
)
def sumar_diez(context: OpExecutionContext, valor: int) -> int:
    resultado = valor + 10
    context.log.info(f"Sumando 10 al valor {valor}: {resultado}")
    return resultado

# Operación que suma cinco al valor
@dg.op(
    name="sumar_cinco",
    ins={"valor": In(int)},
    out=Out(int)
)
def sumar_cinco(context: OpExecutionContext, valor: int) -> int:
    resultado = valor + 5
    context.log.info(f"Sumando 5 al valor {valor}: {resultado}")
    return resultado

# Operación final que guarda los resultados
@dg.op(
    name="guardar_resultados",
    ins={
        "valor_inicial": In(int),
        "resultado_diez": In(int),
        "resultado_cinco": In(int)
    }
)
def guardar_resultados(
    context: OpExecutionContext,
    valor_inicial: int,
    resultado_diez: int,
    resultado_cinco: int
):
    # Creamos un DataFrame con los resultados
    data = {
        'operacion': ['suma_inicial', 'suma_diez', 'suma_cinco'],
        'resultado': [valor_inicial, resultado_diez, resultado_cinco]
    }
    df_resultados = pd.DataFrame(data)
    
    # Mostramos los resultados
    context.log.info(f"Resultados finales:\n{df_resultados}")
    context.log.info(f"Suma total: {resultado_diez + resultado_cinco}")
    
    return df_resultados

# Asset final que muestra los resultados
@dg.asset(
    compute_kind="python",
    group_name="resultados",
    key="resultados_finales",
    deps=["datos_iniciales"]  # Indicamos que depende del asset inicial
)
def resultados_finales(context: OpExecutionContext):
    # Obtenemos el valor del asset datos_iniciales
    # En un entorno real, esto podría hacerse con I/O managers
    context.log.info("Procesando datos y generando resultados...")
    
    # Obtenemos el valor materializado del asset datos_iniciales
    # En un entorno real, esto se puede hacer con get_asset_value_loader
    # Para simplificar, vamos a recalcularlo
    data = {
        'id': [0, 1],
        'valor': [1, 1]
    }
    df = pd.DataFrame(data)
    valor_inicial = df['valor'].sum()
    
    # Ejecutamos las operaciones
    resultado_diez = valor_inicial + 10
    resultado_cinco = valor_inicial + 5
    
    # Creamos un DataFrame con los resultados
    data_resultados = {
        'operacion': ['suma_inicial', 'suma_diez', 'suma_cinco'],
        'resultado': [valor_inicial, resultado_diez, resultado_cinco]
    }
    df_resultados = pd.DataFrame(data_resultados)
    
    # Devolvemos el resultado con metadatos
    return dg.MaterializeResult(
        metadata={
            "resultados": dg.MetadataValue.md(df_resultados.to_markdown(index=False)),
            "suma_total": dg.MetadataValue.int(resultado_diez + resultado_cinco)
        }
    )

# Definimos un job que conecta el asset con las operaciones
@dg.job(
    name="job_operaciones_suma"
)
def job_operaciones_suma():
    # Cargamos el valor del asset
    valor_asset = obtener_valor_inicial(asset_valor=datos_iniciales())
    
    # Realizamos las operaciones
    resultado_diez = sumar_diez(valor_asset)
    resultado_cinco = sumar_cinco(valor_asset)
    
    # Guardamos los resultados
    guardar_resultados(valor_asset, resultado_diez, resultado_cinco)

# Definiciones de Dagster
defs = dg.Definitions(
    assets=[datos_iniciales, resultados_finales],
    jobs=[job_operaciones_suma]
)