output "vast_vpc_id" {
  value = aws_vpc.vast.id
}

output "private_subnet_id" {
  value = aws_subnet.private.id
}

output "private_subnet_cidr" {
  value = local.private_subnet_cidr
}
