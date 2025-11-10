set shell := ["bash", "-c"]

default:
    @just --list

bundle-validate target="dev":
    databricks bundle validate --target {{target}}

bundle-deploy target="dev":
    databricks bundle deploy --target {{target}}

bundle-destroy target="dev":
    databricks bundle destroy --target {{target}}

bundle-run resource target="dev":
    databricks bundle run --target {{target}} {{resource}}

bundle-run-bronze target="dev":
    just bundle-run bronze_pipeline {{target}}

bundle-run-silver target="dev":
    just bundle-run silver_pipeline {{target}}

bundle-run-gold target="dev":
    just bundle-run gold_pipeline {{target}}

bundle-run-train target="dev":
    just bundle-run train_opportunity_model {{target}}

bundle-run-batch target="dev":
    just bundle-run batch_scoring_job {{target}}

terraform-init:
    cd terraform && terraform init

terraform-plan tfvars="terraform.tfvars":
    cd terraform && terraform plan -var-file={{tfvars}}

terraform-apply tfvars="terraform.tfvars":
    cd terraform && terraform apply -var-file={{tfvars}}

terraform-destroy tfvars="terraform.tfvars":
    cd terraform && terraform destroy -var-file={{tfvars}}

