# Local Workspace Configuration
#
# Copy this file to .claude/local-config.md and fill in your Fabric workspace details.
# The local-config.md file is gitignored — it stays on your machine only.

## Fabric Workspace

- **Workspace**: <your workspace name>
- **Workspace ID**: `<your-workspace-guid>`
- **Lakehouse**: CricketLakehouse
- **Lakehouse ID**: `<your-lakehouse-guid>`
- **Notebook**: CricketETL (`<your-notebook-guid>`)
- **Semantic Model**: CricketAnalytics (`<your-model-guid>`)

## Portal URLs

- **Portal base URL**: `https://app.powerbi.com` (or your tenant URL)
- **Workspace portal URL**: `https://app.powerbi.com/groups/<workspace-id>`
- **Notebook portal URL**: `https://app.powerbi.com/groups/<workspace-id>/synapsenotebooks/<notebook-id>`

## SQL Endpoint

- **SQL Endpoint**: `<your-sql-endpoint>.datawarehouse.fabric.microsoft.com`
- **SQL Endpoint ID**: `<your-endpoint-guid>`

## DataFactory Connections

- **Web (GitHub)**: `<connection-guid>`
- **Lakehouse**: `<connection-guid>`

## Livy Session

- **Session ID**: `<session-guid>` (recreate if expired)

### Creating a new Livy session
```bash
TOKEN=$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
curl -s -X POST "https://api.fabric.microsoft.com/v1/workspaces/<WORKSPACE_ID>/lakehouses/<LAKEHOUSE_ID>/livyApi/versions/2023-12-01/sessions" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{}'
```
