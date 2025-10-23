import sys
import argparse

# Import within package to avoid path issues when installed as wheel
from pipelines import spark_job_with_db as job


def main() -> None:
    parser = argparse.ArgumentParser(description="Ejecuta el pipeline de Spark basado en YAML")
    parser.add_argument("--config", required=True, help="Ruta al dataset.yml (abfs:// o local)")
    parser.add_argument("--env", default="config/env.yml", help="Ruta al env.yml (abfs:// o local)")
    parser.add_argument("--db-config", default="config/database.yml", help="Ruta al database.yml (abfs:// o local)")
    parser.add_argument("--environment", default="default", help="Nombre del entorno (default/dev/qa/prod)")
    args = parser.parse_args()

    # Reutilizar la lógica existente de job.main() ajustando sys.argv
    sys.argv = [job.__file__, args.config, args.env, args.db_config, args.environment]
    job.main()


if __name__ == "__main__":
    main()