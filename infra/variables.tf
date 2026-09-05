variable "aws_region" {
  description = "AWS region for the deployment"
  type        = string
  default     = "eu-central-1"
}

variable "instance_type" {
  description = "EC2 instance type for the app host"
  type        = string
  default     = "t3.medium"
}

variable "my_ip" {
  description = "Your public IP with /32 suffix, for SSH access (get with: curl -s https://checkip.amazonaws.com)"
  type        = string
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key that will be authorized on the instance"
  type        = string
  default     = "~/.ssh/aws-demo-key.pub"
}

variable "budget_email" {
  description = "Email address that receives AWS Budget alerts"
  type        = string
}

variable "budget_limit_usd" {
  description = "Monthly budget threshold in USD"
  type        = number
  default     = 25
}
