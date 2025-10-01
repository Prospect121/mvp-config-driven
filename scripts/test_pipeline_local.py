#!/usr/bin/env python3
"""
Script simple para probar el pipeline de datos localmente usando pandas.
Este script simula el procesamiento que haría Spark en un entorno de desarrollo.
"""

import pandas as pd
import yaml
import json
import os
from datetime import datetime
from pathlib import Path

def load_config(config_path):
    """Carga la configuración del pipeline"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def process_payments_data(config):
    """Procesa los datos de pagos según la configuración"""
    print(f"🚀 Iniciando pipeline: {config['id']}")
    
    # 1. Leer datos de origen
    source_path = config['source']['path']
    print(f"📖 Leyendo datos desde: {source_path}")
    
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Archivo de datos no encontrado: {source_path}")
    
    df = pd.read_csv(source_path)
    print(f"✅ Leídos {len(df)} registros desde el origen")
    
    # 2. Mostrar muestra de datos originales
    print("\n📊 Muestra de datos originales:")
    print(df.head())
    print(f"\nColumnas: {list(df.columns)}")
    print(f"Tipos de datos:\n{df.dtypes}")
    
    # 3. Aplicar transformaciones
    print("\n🔄 Aplicando transformaciones...")
    
    # Filtrar solo pagos completados
    df_filtered = df[df['status'] == 'completed'].copy()
    print(f"✅ Filtrados {len(df_filtered)} pagos completados de {len(df)} totales")
    
    # Agregar fecha de procesamiento
    df_filtered['processing_date'] = datetime.now()
    
    # Calcular monto en USD
    def convert_to_usd(row):
        if row['currency'] == 'USD':
            return row['amount']
        elif row['currency'] == 'EUR':
            return row['amount'] * 1.1
        elif row['currency'] == 'GBP':
            return row['amount'] * 1.25
        else:
            return row['amount']
    
    df_filtered['amount_usd'] = df_filtered.apply(convert_to_usd, axis=1)
    
    # 4. Validaciones de calidad de datos
    print("\n🔍 Ejecutando validaciones de calidad de datos...")
    
    # Verificar que los montos sean positivos
    negative_amounts = df_filtered[df_filtered['amount'] <= 0]
    if len(negative_amounts) > 0:
        print(f"⚠️  ADVERTENCIA: {len(negative_amounts)} registros con montos negativos o cero")
    else:
        print("✅ Todos los montos son positivos")
    
    # Verificar que payment_id no sea nulo
    null_payment_ids = df_filtered[df_filtered['payment_id'].isnull()]
    if len(null_payment_ids) > 0:
        print(f"❌ ERROR: {len(null_payment_ids)} registros con payment_id nulo")
    else:
        print("✅ Todos los payment_id son válidos")
    
    # Verificar monedas válidas
    valid_currencies = ['USD', 'EUR', 'GBP']
    invalid_currencies = df_filtered[~df_filtered['currency'].isin(valid_currencies)]
    if len(invalid_currencies) > 0:
        print(f"⚠️  ADVERTENCIA: {len(invalid_currencies)} registros con monedas no válidas")
        print(f"Monedas encontradas: {df_filtered['currency'].unique()}")
    else:
        print("✅ Todas las monedas son válidas")
    
    # 5. Mostrar estadísticas finales
    print("\n📈 Estadísticas del procesamiento:")
    print(f"• Registros procesados: {len(df_filtered)}")
    print(f"• Países únicos: {df_filtered['country'].nunique()}")
    print(f"• Monedas únicas: {df_filtered['currency'].nunique()}")
    print(f"• Monto total (USD): ${df_filtered['amount_usd'].sum():,.2f}")
    print(f"• Monto promedio (USD): ${df_filtered['amount_usd'].mean():.2f}")
    
    # 6. Guardar resultados
    output_path = config['destination']['path']
    os.makedirs(output_path, exist_ok=True)
    
    # Guardar como CSV para facilitar la inspección
    output_file = os.path.join(output_path, 'processed_payments.csv')
    df_filtered.to_csv(output_file, index=False)
    print(f"\n💾 Datos procesados guardados en: {output_file}")
    
    # Guardar también como JSON para inspección
    output_json = os.path.join(output_path, 'processed_payments.json')
    df_filtered.to_json(output_json, orient='records', date_format='iso', indent=2)
    print(f"💾 Datos también guardados en formato JSON: {output_json}")
    
    # 7. Generar reporte de métricas
    metrics = {
        'pipeline_id': config['id'],
        'execution_time': datetime.now().isoformat(),
        'input_records': len(df),
        'output_records': len(df_filtered),
        'data_quality_score': 1.0 - (len(negative_amounts) + len(null_payment_ids)) / len(df_filtered) if len(df_filtered) > 0 else 0,
        'countries_processed': df_filtered['country'].nunique(),
        'currencies_processed': df_filtered['currency'].nunique(),
        'total_amount_usd': float(df_filtered['amount_usd'].sum()),
        'average_amount_usd': float(df_filtered['amount_usd'].mean())
    }
    
    metrics_file = os.path.join(output_path, 'pipeline_metrics.json')
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"📊 Métricas guardadas en: {metrics_file}")
    
    print(f"\n🎉 Pipeline completado exitosamente!")
    return df_filtered, metrics

def main():
    """Función principal"""
    config_path = "config/sample_pipeline.yml"
    
    try:
        # Cargar configuración
        config = load_config(config_path)
        
        # Procesar datos
        processed_data, metrics = process_payments_data(config)
        
        print(f"\n✨ Resumen final:")
        print(f"• Pipeline: {config['id']}")
        print(f"• Registros procesados: {metrics['output_records']}")
        print(f"• Calidad de datos: {metrics['data_quality_score']:.2%}")
        print(f"• Monto total procesado: ${metrics['total_amount_usd']:,.2f} USD")
        
    except Exception as e:
        print(f"❌ Error en el pipeline: {e}")
        raise

if __name__ == "__main__":
    main()