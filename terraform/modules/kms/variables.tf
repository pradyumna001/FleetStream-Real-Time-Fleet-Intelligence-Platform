variable "name" {
  description = "Alias suffix for the key."
  type        = string
}

variable "deletion_window_days" {
  description = "Waiting period before key deletion completes."
  type        = number
  default     = 30
}

variable "tags" {
  type    = map(string)
  default = {}
}
