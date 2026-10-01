variable "aws_region" {
  description = "AWS region. eu-south-2 is opt-in — enable it in the account first."
  type        = string
  default     = "eu-south-2"
}

variable "environment" {
  type    = string
  default = "demo"
}

variable "project_name" {
  type    = string
  default = "ecom-analytics"
}
