# Glue Data Catalog: the AWS counterpart to the local Iceberg REST fixture.
#
# One database per medallion layer rather than one shared database. The separation is
# what makes IAM grants meaningful - an analyst role can be given read on gold without
# also being able to read raw telemetry, which carries per-vehicle location history.

locals {
  layers = ["bronze", "silver", "gold", "platform"]
}

resource "aws_glue_catalog_database" "layer" {
  for_each = toset(local.layers)

  name        = "${var.database_prefix}_${each.value}"
  description = "FleetStream ${each.value} layer"

  location_uri = "${var.warehouse_location}/${each.value}"
}
