# CI/CD Deploy
- Use Jenkins/Azure DevOps to: 
  1) Validate ADF JSONs
  2) Deploy Functions/Databricks jobs
  3) Run smoke validations on AKS
  4) Create/refresh Synapse MVs
- Rollback: redeploy previous artifact; partitioned tables allow quick revert via time travel (Delta).