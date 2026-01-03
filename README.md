# watchdog-dbt-models-main

Sanitized dbt project showing transformations for watchdog-style alerting/reporting. All identifiers are generic; add your own warehouse profile and staging sources to run end to end.

## What’s inside
- A collection of `*.sql` models for campaign, product, and KPI rollups.
- Materializations are left simple (views/ephemeral) so you can adapt to your warehouse.
- No seeds or snapshots are shipped to keep it lightweight.

## Prereqs
- dbt Core installed (`pip install dbt-core dbt-bigquery` or your adapter).
- Warehouse profile in `~/.dbt/profiles.yml` (not committed). Example:
```yaml
watchdog_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-gcp-project
      dataset: analytics
      keyfile: /path/to/service_account.json
      threads: 4
```

## Run locally
```bash
dbt deps        # if you add packages
dbt debug       # confirm profile connectivity
dbt run         # build models
dbt test        # add/execute tests as needed
```

## Notes
- Replace source table names/columns to match your warehouse schema.
- Keep credentials in your profile or env vars; nothing sensitive lives in this repo.
- Feel free to add docs (`dbt docs generate`) and tests to harden the project.***
