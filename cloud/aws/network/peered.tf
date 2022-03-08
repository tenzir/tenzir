### VPC

resource "aws_vpc_peering_connection_accepter" "peer" {
  provider = aws.monitored_vpc

  vpc_peering_connection_id = aws_vpc_peering_connection.peering.id
  auto_accept               = true

  tags = {
    Side = "Accepter"
  }
}

## ROUTING

resource "aws_route_table" "routes_on_peered_vpc" {
  provider = aws.monitored_vpc
  vpc_id   = var.peered_vpc_id
  route {
    cidr_block                = local.private_subnet_cidr
    vpc_peering_connection_id = aws_vpc_peering_connection.peering.id
  }
}
