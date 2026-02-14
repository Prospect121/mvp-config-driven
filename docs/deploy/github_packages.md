# Publicacion temporal via GitHub Releases

Esta guia describe como publicar `datacore` como asset de GitHub Release (temporal) y consumirlo con `pip` usando la URL del wheel.

## 1) Publicar wheel en GitHub Releases

El workflow `publish-release` construye `dist/*` y los sube como assets del Release asociado al tag.

Puedes ejecutarlo de dos formas:

1. Manualmente desde `workflow_dispatch` (indicando el tag).
2. Empujando un tag como `v1.1.0`.

## 2) Configurar consumidor (pip)

En el repo consumidor, usa un token con acceso al repo si es privado (scope `repo`) y ejecuta:

```bash
export GH_OWNER="Prospect121"
export GH_REPO="mvp-config-driven"
export GH_RELEASE_TOKEN="<token_repo>"
export DC_VERSION="1.1.0"

pip install \
  "https://${GH_RELEASE_TOKEN}:x-oauth-basic@github.com/${GH_OWNER}/${GH_REPO}/releases/download/v${DC_VERSION}/datacore-${DC_VERSION}-py3-none-any.whl"
```

Si el repo es publico, puedes omitir el token:

```bash
pip install "https://github.com/Prospect121/mvp-config-driven/releases/download/v1.1.0/datacore-1.1.0-py3-none-any.whl"
```

## 3) Uso desde control-api

Una vez instalado, el control plane puede llamar la API estable:

```python
from datacore.api import validate_config_payload, build_plan, run_pipeline
```

## 4) Convencion de versiones

- Incrementa `pyproject.toml` con semver.
- Crea tag `vX.Y.Z` al publicar release.
- Mantener compatibilidad del contrato de plan (`plan.schema.json`) o versionar si se rompe.
