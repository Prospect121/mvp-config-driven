# CI/CD helpers

Esta carpeta contiene utilidades para integrar `mvp-config-driven` en pipelines:

- `docker-compose.override.yml`: sobreescribe paths en CI.
- `check_config.sh`: valida sintaxis YAML/JSON.
- `lint.yml`: workflow ejemplo para GitHub Actions.
- `test_dataset.yml`: dataset reducido para pruebas rápidas.

## Ejecución local
```bash
./ci/check_config.sh
docker compose -f docker-compose.yml -f ci/docker-compose.override.yml up -d
