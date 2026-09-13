variable "database_prefix" {
  description = "Prefix for the medallion databases."
  type        = string
  default     = "fleetstream"
}

variable "warehouse_location" {
  description = "S3 location backing the catalog."
  type        = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
