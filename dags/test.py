"""
DAG de prueba en Airflow con flujo ETL simple:
1) Extract: genera datos simulados.
2) Transform: calcula métricas básicas.
3) Load: muestra el resultado final.
"""

# Importaciones estándar de Python para manejo de fechas y reintentos.
import json
from datetime import datetime, timedelta
from pathlib import Path

# Importaciones principales de Airflow para definir el DAG y sus tareas.
from airflow import DAG
from airflow.operators.python import PythonOperator


# Argumentos por defecto aplicados a todas las tareas del DAG.
# - owner: responsable lógico del DAG.
# - depends_on_past: evita dependencia con ejecuciones anteriores.
# - retries/retry_delay: política de reintentos en caso de fallo.
default_args = {
	"owner": "airflow",
	"depends_on_past": False,
	"retries": 1,
	"retry_delay": timedelta(minutes=1),
}


def extract_data(**context):
	"""Extrae datos simulados y los guarda en XCom con la clave 'raw_data'."""
	data = {
		"records": [10, 20, 30, 40],
		"source": "test_source",
	}
	context["ti"].xcom_push(key="raw_data", value=data)


def transform_data(**context):
	"""Transforma datos extraídos y publica métricas en XCom como 'transformed_data'."""
	raw_data = context["ti"].xcom_pull(task_ids="extract", key="raw_data")
	records = raw_data.get("records", [])
	transformed = {
		"total": sum(records),
		"count": len(records),
		"average": (sum(records) / len(records)) if records else 0,
	}
	context["ti"].xcom_push(key="transformed_data", value=transformed)


def load_data(**context):
	"""Carga final: recupera el resultado transformado y lo imprime en logs."""
	ti = context["ti"]
	transformed = ti.xcom_pull(task_ids="transform", key="transformed_data")
	print(f"Resultado final de prueba: {transformed}")

	# Usa logical_date cuando existe (Airflow 2), con fallback para compatibilidad.
	execution_date = context.get("logical_date") or context.get("execution_date") or datetime.utcnow()
	execution_stamp = execution_date.strftime("%Y%m%dT%H%M%S")

	# Guarda el resultado en temp/data para reutilizarlo en futuras tareas o DAGs.
	data_dir = Path(__file__).resolve().parents[1] / "temp" / "data"
	data_dir.mkdir(parents=True, exist_ok=True)
	output_file = data_dir / f"final_result_{execution_stamp}.json"

	with output_file.open("w", encoding="utf-8") as file_obj:
		json.dump(transformed, file_obj, indent=2)

	# Publica en XCom el resultado y metadatos de persistencia.
	ti.xcom_push(key="final_result", value=transformed)
	ti.xcom_push(key="final_result_file", value=str(output_file))
	ti.xcom_push(key="final_result_execution_date", value=execution_date.isoformat())


# Definición del DAG:
# - dag_id: identificador único.
# - start_date/schedule: inicio y frecuencia de ejecución.
# - catchup=False: no ejecuta periodos atrasados.
# - tags: etiquetas para organización en la UI.
with DAG(
	dag_id="basic_test_dag",
	description="DAG basico de ejemplo para pruebas en Airflow",
	default_args=default_args,
	start_date=datetime(2026, 3, 1),
	schedule="@daily",
	catchup=False,
	tags=["test", "example"],
) as dag:
	# Tarea 1: extracción de datos.
	extract = PythonOperator(
		task_id="extract",
		python_callable=extract_data,
	)

	# Tarea 2: transformación de datos.
	transform = PythonOperator(
		task_id="transform",
		python_callable=transform_data,
	)

	# Tarea 3: carga/salida del resultado.
	load = PythonOperator(
		task_id="load",
		python_callable=load_data,
	)

	# Orden de ejecución del flujo: extract -> transform -> load.
	extract >> transform >> load
