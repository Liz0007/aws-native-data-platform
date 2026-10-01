locals {
  name_prefix = "${var.project_name}-${var.environment}"

  # Glue catalog database names: lowercase and underscores only, no dashes.
  bronze_db = replace("${var.project_name}_${var.environment}_bronze", "-", "_")
  silver_db = replace("${var.project_name}_${var.environment}_silver", "-", "_")
  gold_db   = replace("${var.project_name}_${var.environment}_gold", "-", "_")

  # Topics exported from the ecommerce platform's data lake. Bronze stores
  # these as a single partitioned table rather than five, since the raw
  # envelope is identical across topics — only payload differs.
  topics = [
    "order-created",
    "order-confirmed",
    "order-cancelled",
    "payment-processed",
    "inventory-reserved",
  ]
}
