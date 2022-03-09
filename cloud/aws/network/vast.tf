### VPC
resource "aws_vpc" "vast" {
  cidr_block = var.new_vpc_cidr

  tags = {
    Name = "${module.env.module_name}-vpc-${module.env.stage}"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.vast.id

  tags = {
    Name = "${module.env.module_name}-igw-${module.env.stage}"
  }
}

resource "aws_subnet" "public" {
  vpc_id     = aws_vpc.vast.id
  cidr_block = local.public_subnet_cidr
}

resource "aws_subnet" "private" {
  vpc_id            = aws_vpc.vast.id
  cidr_block        = local.private_subnet_cidr
  availability_zone = aws_subnet.public.availability_zone
}

resource "aws_eip" "nat_gateway" {
  vpc = true
}

resource "aws_nat_gateway" "nat_gateway" {
  allocation_id = aws_eip.nat_gateway.id
  subnet_id     = aws_subnet.public.id
}

resource "aws_vpc_peering_connection" "peering" {
  peer_vpc_id   = var.peered_vpc_id
  vpc_id        = aws_vpc.vast.id
  peer_owner_id = data.aws_caller_identity.peer.account_id
  peer_region   = data.aws_region.peer.name
  auto_accept   = false

  tags = {
    Side = "Requester"
  }
}


## ROUTING

resource "aws_route_table" "routes_on_private_subnet" {
  vpc_id = aws_vpc.vast.id

  route {
    cidr_block                = data.aws_vpc.peer.cidr_block
    vpc_peering_connection_id = aws_vpc_peering_connection.peering.id
  }

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat_gateway.id
  }
}

resource "aws_route_table_association" "private" {
  subnet_id      = aws_subnet.private.id
  route_table_id = aws_route_table.routes_on_private_subnet.id
}

resource "aws_route_table" "routes_on_public_subnet" {
  vpc_id = aws_vpc.vast.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.routes_on_public_subnet.id
}
