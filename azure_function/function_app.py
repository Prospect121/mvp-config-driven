"""
Azure Function para procesamiento de datos con validaciones
Integra los scripts de Python existentes en src/ con las configuraciones en config/
"""
import azure.functions as func
import logging
import json
import os
import sys
import tempfile
import zipfile
from typing import Dict, Any, Optional
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = func.FunctionApp()

@app.function_name(name="ProcessDataWithValidations")
@app.route(route="process", auth_level=func.AuthLevel.FUNCTION)
def process_data_with_validations(req: func.HttpRequest) -> func.HttpResponse:
    """
    Función principal que procesa datos usando los scripts de Python con validaciones
    """
    try:
        logger.info("Iniciando procesamiento de datos con validaciones")
        
        # Obtener parámetros de la request
        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "Request body is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        input_path = req_body.get('input_path', 'raw/csv')
        output_path = req_body.get('output_path', 'silver/processed')
        config_path = req_body.get('config_path', 'config/data_factory_config.yml')
        expectations_path = req_body.get('expectations_path', 'config/expectations.yml')
        
        logger.info(f"Parámetros: input_path={input_path}, output_path={output_path}")
        
        # Procesar datos
        result = process_data_pipeline(
            input_path=input_path,
            output_path=output_path,
            config_path=config_path,
            expectations_path=expectations_path
        )
        
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logger.error(f"Error en procesamiento: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

def process_data_pipeline(input_path: str, output_path: str, 
                         config_path: str, expectations_path: str) -> Dict[str, Any]:
    """
    Ejecuta el pipeline de procesamiento de datos
    """
    try:
        # Configurar credenciales de Azure
        credential = DefaultAzureCredential()
        storage_account_name = os.environ.get('STORAGE_ACCOUNT_NAME')
        
        if not storage_account_name:
            raise ValueError("STORAGE_ACCOUNT_NAME no configurado")
        
        # Crear cliente de Blob Storage
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )
        
        # Listar archivos en el path de entrada
        container_client = blob_service_client.get_container_client("raw")
        blobs = list(container_client.list_blobs(name_starts_with=input_path.replace('raw/', '')))
        
        if not blobs:
            return {
                "status": "warning",
                "message": f"No se encontraron archivos en {input_path}",
                "files_processed": 0
            }
        
        processed_files = []
        total_rows = 0
        validation_results = []
        
        # Procesar cada archivo
        for blob in blobs:
            if blob.name.endswith('.csv'):
                logger.info(f"Procesando archivo: {blob.name}")
                
                # Descargar archivo
                blob_client = blob_service_client.get_blob_client(
                    container="raw", 
                    blob=blob.name
                )
                
                with tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False) as temp_file:
                    download_stream = blob_client.download_blob()
                    temp_file.write(download_stream.readall())
                    temp_file_path = temp_file.name
                
                try:
                    # Procesar archivo con validaciones
                    file_result = process_single_file(
                        temp_file_path, 
                        blob.name,
                        expectations_path,
                        blob_service_client
                    )
                    
                    processed_files.append(file_result['filename'])
                    total_rows += file_result['rows_processed']
                    validation_results.append(file_result['validation_summary'])
                    
                    # Subir archivo procesado a silver
                    upload_processed_file(
                        file_result['processed_file_path'],
                        output_path,
                        blob.name,
                        blob_service_client
                    )
                    
                finally:
                    # Limpiar archivo temporal
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
                    if 'processed_file_path' in locals() and os.path.exists(file_result['processed_file_path']):
                        os.unlink(file_result['processed_file_path'])
        
        # Calcular métricas de calidad
        quality_score = calculate_quality_score(validation_results)
        
        return {
            "status": "success",
            "message": "Procesamiento completado exitosamente",
            "files_processed": len(processed_files),
            "total_rows": total_rows,
            "quality_score": quality_score,
            "validation_summary": validation_results,
            "processed_files": processed_files
        }
        
    except Exception as e:
        logger.error(f"Error en pipeline: {str(e)}")
        raise

def process_single_file(file_path: str, filename: str, expectations_path: str, 
                       blob_service_client: BlobServiceClient) -> Dict[str, Any]:
    """
    Procesa un archivo individual con validaciones
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    # Leer archivo CSV
    df = pd.read_csv(file_path)
    original_rows = len(df)
    
    logger.info(f"Archivo {filename}: {original_rows} filas leídas")
    
    # Aplicar validaciones básicas (simulando las expectativas)
    validation_results = apply_data_validations(df, filename)
    
    # Filtrar filas válidas
    valid_df = df[validation_results['valid_rows_mask']]
    invalid_df = df[~validation_results['valid_rows_mask']]
    
    # Guardar datos en cuarentena si hay filas inválidas
    if len(invalid_df) > 0:
        quarantine_file = save_to_quarantine(invalid_df, filename, blob_service_client)
        logger.warning(f"Se enviaron {len(invalid_df)} filas a cuarentena: {quarantine_file}")
    
    # Convertir a Parquet
    processed_file_path = file_path.replace('.csv', '.parquet')
    table = pa.Table.from_pandas(valid_df)
    pq.write_table(table, processed_file_path)
    
    return {
        "filename": filename,
        "rows_processed": len(valid_df),
        "rows_quarantined": len(invalid_df),
        "validation_summary": validation_results['summary'],
        "processed_file_path": processed_file_path
    }

def apply_data_validations(df: pd.DataFrame, filename: str) -> Dict[str, Any]:
    """
    Aplica validaciones de calidad de datos
    """
    import pandas as pd
    import numpy as np
    
    validation_summary = {
        "total_rows": len(df),
        "validations": []
    }
    
    # Crear máscara de filas válidas (inicialmente todas son válidas)
    valid_mask = pd.Series([True] * len(df))
    
    # Validación 1: Campos no nulos (simulando expectations.yml)
    required_fields = ['id', 'nombre', 'email']  # Ajustar según tus datos
    for field in required_fields:
        if field in df.columns:
            field_valid = df[field].notna()
            null_count = df[field].isna().sum()
            
            validation_summary["validations"].append({
                "rule": f"not_null_{field}",
                "passed": null_count == 0,
                "failed_rows": int(null_count),
                "severity": "critical"
            })
            
            valid_mask &= field_valid
    
    # Validación 2: Formato de email
    if 'email' in df.columns:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        email_valid = df['email'].str.match(email_pattern, na=False)
        invalid_emails = (~email_valid & df['email'].notna()).sum()
        
        validation_summary["validations"].append({
            "rule": "email_format",
            "passed": invalid_emails == 0,
            "failed_rows": int(invalid_emails),
            "severity": "medium"
        })
        
        valid_mask &= (email_valid | df['email'].isna())
    
    # Validación 3: Rangos de edad
    if 'edad' in df.columns:
        age_valid = (df['edad'] >= 0) & (df['edad'] <= 120)
        invalid_ages = (~age_valid & df['edad'].notna()).sum()
        
        validation_summary["validations"].append({
            "rule": "age_range",
            "passed": invalid_ages == 0,
            "failed_rows": int(invalid_ages),
            "severity": "high"
        })
        
        valid_mask &= (age_valid | df['edad'].isna())
    
    # Calcular estadísticas finales
    valid_rows = valid_mask.sum()
    validation_summary["valid_rows"] = int(valid_rows)
    validation_summary["invalid_rows"] = int(len(df) - valid_rows)
    validation_summary["quality_score"] = float(valid_rows / len(df)) if len(df) > 0 else 1.0
    
    return {
        "valid_rows_mask": valid_mask,
        "summary": validation_summary
    }

def save_to_quarantine(invalid_df: pd.DataFrame, filename: str, 
                      blob_service_client: BlobServiceClient) -> str:
    """
    Guarda datos inválidos en el container de cuarentena
    """
    import tempfile
    from datetime import datetime
    
    # Crear nombre de archivo para cuarentena
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    quarantine_filename = f"quarantine_{timestamp}_{filename}"
    
    # Guardar en archivo temporal
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_file:
        invalid_df.to_csv(temp_file.name, index=False)
        temp_file_path = temp_file.name
    
    try:
        # Subir a container de cuarentena
        blob_client = blob_service_client.get_blob_client(
            container="quarantine",
            blob=quarantine_filename
        )
        
        with open(temp_file_path, 'rb') as data:
            blob_client.upload_blob(data, overwrite=True)
        
        return quarantine_filename
        
    finally:
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)

def upload_processed_file(file_path: str, output_path: str, original_filename: str,
                         blob_service_client: BlobServiceClient):
    """
    Sube el archivo procesado al container silver
    """
    # Crear nombre de archivo de salida
    output_filename = original_filename.replace('.csv', '.parquet')
    blob_name = f"{output_path.replace('silver/', '')}/{output_filename}"
    
    # Subir archivo
    blob_client = blob_service_client.get_blob_client(
        container="silver",
        blob=blob_name
    )
    
    with open(file_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)
    
    logger.info(f"Archivo procesado subido: silver/{blob_name}")

def calculate_quality_score(validation_results: list) -> float:
    """
    Calcula el score de calidad promedio
    """
    if not validation_results:
        return 1.0
    
    total_score = sum(result['quality_score'] for result in validation_results)
    return total_score / len(validation_results)

if __name__ == "__main__":
    # Para testing local
    import json
    
    test_request = {
        "input_path": "raw/csv",
        "output_path": "silver/processed",
        "config_path": "config/data_factory_config.yml",
        "expectations_path": "config/expectations.yml"
    }
    
    result = process_data_pipeline(**test_request)
    print(json.dumps(result, indent=2))