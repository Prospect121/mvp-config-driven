# CI/CD helpers

Esta carpeta contiene utilidades para integrar `mvp-config-driven` en pipelines:

- `docker-compose.override.yml`: sobreescribe paths en CI para usar data/ en lugar de configuraciones de prueba.
- `check_config.sh`: valida sintaxis YAML/JSON.
- `lint.yml`: workflow ejemplo para GitHub Actions.
- `test_dataset.yml`: configuración de dataset para pruebas en CI.

## Ejecución local
```bash
./ci/check_config.sh
docker compose -f docker-compose.yml -f ci/docker-compose.override.yml up -d
