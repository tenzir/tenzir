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

# get the main route table of the peered VPC to route all its
# subnets to the new VPC
data "aws_route_tables" "peered_vpc_main" {
  vpc_id = var.peered_vpc_id

  filter {
    name   = "association.main"
    values = ["true"]
  }
}

resource "aws_route" "peering_conn_route" {
  route_table_id            = tolist(data.aws_route_tables.peered_vpc_main.ids)[0]
  destination_cidr_block    = local.private_subnet_cidr
  vpc_peering_connection_id = aws_vpc_peering_connection.peering.id
}
