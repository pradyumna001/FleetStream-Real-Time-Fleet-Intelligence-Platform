# FleetStream infrastructure (Terraform)

The AWS deployment the local Docker Compose stack emulates. Each local component has
a managed counterpart here:

| Local (Compose)      | AWS (Terraform)                     |
|----------------------|-------------------------------------|
| Kafka (KRaft)        | MSK                                 |
| MinIO                | S3 + KMS                            |
| Iceberg REST fixture | Glue Data Catalog                   |
| Trino                | Athena                              |
| local credentials    | IAM roles, least privilege          |

## Status: validated, not deployed

This configuration is **written and validated, but has never been applied**. There is
no AWS account behind it, so no `terraform apply` has run and no resource here has
been created. Treat it as a reviewed starting point, not as proven infrastructure.

What *is* verified: `terraform fmt -check`, `terraform init -backend=false` and
`terraform validate` all pass, which catches syntax errors, bad references, type
mismatches and unknown arguments. What is not verified: anything only an apply would
reveal - IAM policy sufficiency, MSK subnet capacity, quota limits, and the exact
shape of resources at plan time.

Terraform is not installed on the development host, so it runs through Docker:

```bash
docker run --rm -v "$PWD:/w" -w /w/terraform/envs/dev hashicorp/terraform:1.10 init -backend=false
docker run --rm -v "$PWD:/w" -w /w/terraform/envs/dev hashicorp/terraform:1.10 validate
docker run --rm -v "$PWD:/w" -w /w/terraform/envs/dev hashicorp/terraform:1.10 fmt -check -recursive /w/terraform
```

CI runs exactly these on every pull request.

## Before an apply would be safe

1. Set a real remote backend (S3 + DynamoDB lock) in `envs/dev/versions.tf`; the
   committed config deliberately has no backend so `init -backend=false` works in CI.
2. Supply real VPC and subnet ids in `terraform.tfvars` - the defaults are placeholders.
3. Review `modules/iam` against your organisation's boundary policies.
4. Run `terraform plan` and read it. The S3 bucket has `prevent_destroy` set, which is
   intentional: the raw layer is the platform's replay source, and losing it makes
   every backfill impossible.
