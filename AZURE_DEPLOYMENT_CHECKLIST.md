# Checklist de Despliegue en Azure
## MVP Config-Driven Data Pipeline

Generado el: 2025-09-30T22:41:15.788626

### ✅ Pre-requisitos
- [ ] Azure CLI instalado y configurado
- [ ] Terraform instalado
- [ ] Permisos de Contributor en la suscripción de Azure
- [ ] Grupos de Azure AD creados:
  - [ ] DataEngineers
  - [ ] DataScientists
  - [ ] DataPipelineAdmins

### ✅ Configuración
- [ ] Archivo terraform.tfvars configurado
- [ ] Variables de entorno de Azure configuradas
- [ ] Configuración de backend de Terraform (opcional)

### ✅ Despliegue de Infraestructura
```bash
# 1. Inicializar Terraform
cd terraform
terraform init

# 2. Planificar despliegue
terraform plan -var-file="terraform.tfvars"

# 3. Aplicar cambios
terraform apply -var-file="terraform.tfvars"
```

### ✅ Post-Despliegue
- [ ] Verificar recursos creados en Azure Portal
- [ ] Configurar secretos en Key Vault
- [ ] Probar conectividad a recursos
- [ ] Configurar alertas y monitoreo
- [ ] Ejecutar pipeline de prueba

### ✅ Recursos Creados
- [ ] Resource Group: rg-mvp-data-pipeline-dev
- [ ] Storage Account: stmvpdatapipelinedev
- [ ] SQL Server: sql-mvp-data-pipeline-dev
- [ ] Event Hub: eh-mvp-data-pipeline-dev
- [ ] Key Vault: kv-mvp-data-pipeline-dev
- [ ] Data Factory: adf-mvp-data-pipeline-dev
- [ ] Application Insights: ai-mvp-data-pipeline-dev

### ✅ Validación
- [ ] Pipeline ejecuta correctamente
- [ ] Datos se procesan y almacenan
- [ ] Monitoreo funciona
- [ ] Alertas configuradas
- [ ] Seguridad implementada

### 🚨 Troubleshooting
Si encuentras problemas:
1. Verificar logs en Application Insights
2. Revisar configuración de red y firewall
3. Validar permisos de RBAC
4. Consultar documentación en docs/DEPLOYMENT.md

### 📞 Contacto
Para soporte técnico, contactar al equipo de Data Engineering.
