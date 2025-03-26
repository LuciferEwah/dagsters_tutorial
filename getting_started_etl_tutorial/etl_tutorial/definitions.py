import dagster as dg
import pandas as pd
from dagster import OpExecutionContext, asset, op, job, In, Out, graph

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
    
    # Calculamos la suma total y convertimos a int nativo de Python
    suma_total = int(df['valor'].sum())
    
    # Guardamos el DataFrame en un archivo CSV para que puedan acceder los ops
    df.to_csv("data/datos_iniciales.csv", index=False)
    
    # Devolvemos el resultado con metadatos
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "preview": dg.MetadataValue.md(df.to_markdown(index=False)),
            "suma_total": dg.MetadataValue.int(suma_total)
        }
    )

# Operación para leer el valor del asset desde el archivo
@dg.op(
    name="leer_valor_inicial",
    out=Out(int)
)
def leer_valor_inicial(context: OpExecutionContext):

    df = pd.read_csv("data/datos_iniciales.csv")
    valor = int(df['valor'].sum())
    context.log.info(f"Valor leído del CSV: {valor}")
    return valor


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
    
    # Guardamos los resultados para que el asset final pueda leerlos
    df_resultados.to_csv("data/resultados.csv", index=False)
    return df_resultados

# Definimos un grafo que conecta todas las operaciones
@dg.graph
def flujo_operaciones():
    valor_inicial = leer_valor_inicial()
    resultado_diez = sumar_diez(valor_inicial)
    resultado_cinco = sumar_cinco(valor_inicial)
    guardar_resultados(valor_inicial, resultado_diez, resultado_cinco)

# Definimos un job a partir del grafo
@dg.job(
    name="job_operaciones_suma"
)
def job_operaciones_suma():
    flujo_operaciones()

# Asset final que lee los resultados del job
@dg.asset(
    compute_kind="python",
    group_name="resultados",
    key="resultados_finales",
    deps=["datos_iniciales"]  # Indicamos que depende del asset inicial
)
def resultados_finales(context: OpExecutionContext):
    try:
        # Leemos los resultados guardados por el job
        df_resultados = pd.read_csv("data/resultados.csv")
        suma_total = int(df_resultados['resultado'].sum())
        
        context.log.info(f"Resultados finales leídos del archivo:\n{df_resultados}")
        
        return dg.MaterializeResult(
            metadata={
                "resultados": dg.MetadataValue.md(df_resultados.to_markdown(index=False)),
                "suma_total": dg.MetadataValue.int(suma_total)
            }
        )
    except Exception as e:
        context.log.error(f"Error al leer los resultados: {e}")
        
        # Si no podemos leer los resultados, generamos unos de ejemplo
        # Esto es solo para demostración
        data = {
            'operacion': ['suma_inicial', 'suma_diez', 'suma_cinco'],
            'resultado': [2, 12, 7]
        }
        df_fallback = pd.DataFrame(data)
        
        return dg.MaterializeResult(
            metadata={
                "resultados": dg.MetadataValue.md(df_fallback.to_markdown(index=False)),
                "suma_total": dg.MetadataValue.int(21),
                "nota": dg.MetadataValue.text("Datos generados como fallback debido a un error")
            }
        )

# Definiciones de Dagster
defs = dg.Definitions(
    assets=[datos_iniciales, resultados_finales],
    jobs=[job_operaciones_suma]
)