FROM bitnami/spark:3.5.1

# Instala numpy/pandas/pyarrow compatibles en el Python embebido de Bitnami
USER root
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
RUN /opt/bitnami/python/bin/python3 -m pip install --no-cache-dir --upgrade \
    "numpy==1.26.4" \
    "pandas==2.2.2" \
    "pyarrow==14.0.2"

# Vuelve al usuario no-root utilizado por Bitnami
USER 1001