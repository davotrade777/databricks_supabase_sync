# Databricks → Supabase Sync (Node.js)

Repositorio refactorizado a Node.js para sincronizar transportistas desde Databricks hacia Supabase.

## Stack actual

- **Lambda AWS**: Node.js 20 (`lambda_function/handler.js`)
- **Infra**: SAM (`template.yaml`)
- **Scripts locales**: Node (`src/scripts`)
- **Dashboard local**: Node/Express (`src/dashboard/server.js`)

## Flujo ETL

1. Lee tabla origen `prod.gldlogistica.db_trade_dim_app_transportistas` desde Databricks (OAuth M2M).
2. Deduplica por `codigo_transportista`.
3. Inserta solo nuevos en Supabase `transportistas`.
4. Ignora duplicados de PK de forma segura.
5. Actualiza `etl_watermarks`.
6. Guarda log estructurado JSON en S3 (`patek-philippe-etl-logs-052124708820`).

## Comandos locales

```bash
npm install
npm run test:connection
npm run sync:incremental
npm run sync:historical
npm run dashboard
```

Dashboard local: `http://localhost:5050`

## Deploy Lambda

```bash
sam build
sam deploy --no-confirm-changeset
```

## Variables de entorno (archivo local)

Se leen desde `transportistas_sync/.env`:

- `DATABRICKS_PRD_HOST`
- `DATABRICKS_PRD_HTTP_PATH`
- `DATABRICKS_PRD_CLIENT_ID`
- `DATABRICKS_PRD_CLIENT_SECRET`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- opcionales: `DBX_TABLE`, `SUPABASE_TABLE`, `FETCH_SIZE`, `LAMBDA_NAME`

## Arquitectura Escalable (multi-pipeline)

- Configuración por pipeline en `lambda_function/pipelines.json`.
- La Lambda recibe `pipeline_name` y ejecuta la estrategia declarada:
  - `insert_only`
  - `upsert`
- Watermark por pipeline en `etl_watermarks.table_name = pipeline_name`.
- Auditoría por corrida:
  - S3: `s3://.../{pipeline_name}/{yyyy-mm-dd}/{timestamp}.json`
  - Supabase: tabla `etl_runs` (si existe).

### Ejemplo de invocación

```json
{
  "pipeline_name": "transportistas"
}
```
