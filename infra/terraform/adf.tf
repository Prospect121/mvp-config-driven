# Azure Data Factory
resource "azurerm_data_factory" "df" {
  name                = "${var.prefix}-adf-cu"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

# Pipeline con Web activity que dispara el Job de Databricks
resource "azurerm_data_factory_pipeline" "run_job" {
  name            = "${var.prefix}-run-job"
  data_factory_id = azurerm_data_factory.df.id
  variables = {
    startTime = ""
  }
  activities_json = jsonencode([
    {
      name = "RunJob",
      type = "WebActivity",
      typeProperties = {
        url    = "https://${azurerm_databricks_workspace.dbw.workspace_url}/api/2.1/jobs/run-now",
        method = "POST",
        headers = {
          Authorization = "Bearer ${databricks_token.pat.token_value}",
          "Content-Type" = "application/json"
        },
        body = jsonencode({
          job_id = databricks_job.pipeline.id
        })
      }
    },
    {
      name = "SetStartTime",
      type = "SetVariable",
      dependsOn = [
        {
          activity = "RunJob",
          dependencyConditions = ["Succeeded"]
        }
      ],
      typeProperties = {
        variableName = "startTime",
        value = {
          value = "@utcNow()",
          type  = "Expression"
        }
      }
    },
    {
      name = "WaitForCompletion",
      type = "Until",
      dependsOn = [
        {
          activity = "SetStartTime",
          dependencyConditions = ["Succeeded"]
        }
      ],
      typeProperties = {
        expression = {
          value = "@or(equals(activity('GetRunStatus').output.state.life_cycle_state, 'TERMINATED'), greaterOrEquals(div(sub(ticks(utcNow()), ticks(variables('startTime'))), 10000000), 600))",
          type  = "Expression"
        },
        activities = [
          {
            name = "GetRunStatus",
            type = "WebActivity",
            typeProperties = {
              url    = "@concat('https://${azurerm_databricks_workspace.dbw.workspace_url}/api/2.1/jobs/runs/get?run_id=', activity('RunJob').output.run_id)",
              method = "GET",
              headers = {
                Authorization = "Bearer ${databricks_token.pat.token_value}",
                "Content-Type" = "application/json"
              }
            }
          },
          {
            name = "Wait30s",
            type = "Wait",
            typeProperties = {
              waitTimeInSeconds = 30
            }
          }
        ]
      }
    },
    {
      name = "GetFinalStatus",
      type = "WebActivity",
      dependsOn = [
        {
          activity = "WaitForCompletion",
          dependencyConditions = ["Succeeded"]
        }
      ],
      typeProperties = {
        url    = "@concat('https://${azurerm_databricks_workspace.dbw.workspace_url}/api/2.1/jobs/runs/get?run_id=', activity('RunJob').output.run_id)",
        method = "GET",
        headers = {
          Authorization = "Bearer ${databricks_token.pat.token_value}",
          "Content-Type" = "application/json"
        }
      }
    },
    {
      name = "CheckSuccess",
      type = "IfCondition",
      dependsOn = [
        {
          activity = "GetFinalStatus",
          dependencyConditions = ["Succeeded"]
        }
      ],
      typeProperties = {
        expression = {
          value = "@equals(activity('GetFinalStatus').output.state.result_state, 'SUCCESS')",
          type  = "Expression"
        },
        ifTrueActivities = [],
        ifFalseActivities = [
          {
            name = "FailRun",
            type = "Fail",
            typeProperties = {
              message = "Databricks run failed"
            }
          }
        ]
      }
    }
  ])
}