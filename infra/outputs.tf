output "public_ip" {
  description = "Elastic IP of the app instance"
  value       = aws_eip.app.public_ip
}

output "instance_id" {
  description = "EC2 instance ID (for aws ec2 stop-instances / start-instances)"
  value       = aws_instance.app.id
}

output "ssh_command" {
  description = "Ready-to-run SSH command"
  value       = "ssh -i ~/.ssh/aws-demo-key ubuntu@${aws_eip.app.public_ip}"
}

output "http_url" {
  description = "URL to open in the browser after deploy"
  value       = "http://${aws_eip.app.public_ip}"
}
