# Publicación y consumo privado en GitHub Packages

Esta guía describe cómo publicar `datacore` en un registro privado PyPI de GitHub Packages y cómo consumirlo desde `ecosystem-pipeline-studio`.

## 1) Publicar paquete privado

El workflow `publish-private` publica artefactos `dist/*` a:

`https://upload.pkg.github.com/<OWNER>/`

Puedes ejecutarlo de dos formas:

1. Manualmente desde `workflow_dispatch`.
2. Empujando un tag como `v1.1.0`.

## 2) Configurar consumidor (pip)

En el repo consumidor, agrega un token con acceso `read:packages` y configura `pip`:

```bash
export GH_OWNER="Prospect121"
export GH_PKG_TOKEN="<token_con_read_packages>"
pip install \
  --extra-index-url "https://__token__:${GH_PKG_TOKEN}@pip.pkg.github.com/${GH_OWNER}/simple/" \
  datacore==1.1.0
```

También puedes usar `requirements.txt`:

```txt
--extra-index-url https://__token__:${GH_PKG_TOKEN}@pip.pkg.github.com/Prospect121/simple/
datacore==1.1.0
```

## 3) Uso desde control-api

Una vez instalado, el control plane puede llamar la API estable de librería:

```python
from datacore.api import validate_config_payload, build_plan, run_pipeline
```

## 4) Convención de versiones

- Incrementa `pyproject.toml` con semver.
- Crea tag `vX.Y.Z` al publicar release.
- Mantén compatibilidad del contrato de plan (`plan.schema.json`) o versiona explícitamente si se rompe.
