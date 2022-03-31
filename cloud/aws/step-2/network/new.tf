### VPC
resource "aws_vpc" "new" {
  cidr_block           = var.new_vpc_cidr
  enable_dns_hostnames = true

  tags = {
    Name = "${module.env.module_name}-vpc-${module.env.stage}"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.new.id

  tags = {
    Name = "${module.env.module_name}-igw-${module.env.stage}"
  }
}

resource "aws_subnet" "public" {
  vpc_id     = aws_vpc.new.id
  cidr_block = local.public_subnet_cidr
  tags = {
    Name = "${module.env.module_name}-public-${module.env.stage}"
  }
}

resource "aws_subnet" "private" {
  vpc_id            = aws_vpc.new.id
  cidr_block        = local.private_subnet_cidr
  availability_zone = aws_subnet.public.availability_zone
  tags = {
    Name = "${module.env.module_name}-private-${module.env.stage}"
  }
}

resource "aws_eip" "nat_gateway" {
  vpc = true
}

resource "aws_nat_gateway" "nat_gateway" {
  allocation_id = aws_eip.nat_gateway.id
  subnet_id     = aws_subnet.public.id
  tags = {
    Name = "${module.env.module_name}-${module.env.stage}"
  }
}

resource "aws_vpc_peering_connection" "peering" {
  peer_vpc_id   = var.peered_vpc_id
  vpc_id        = aws_vpc.new.id
  peer_owner_id = data.aws_caller_identity.peer.account_id
  peer_region   = data.aws_region.peer.name
  auto_accept   = false

  tags = {
    Side = "Requester"
    Name = "${module.env.module_name}-${module.env.stage}"
  }
}


## ROUTING

resource "aws_route_table" "routes_on_private_subnet" {
  vpc_id = aws_vpc.new.id

  route {
    cidr_block                = data.aws_vpc.peer.cidr_block
    vpc_peering_connection_id = aws_vpc_peering_connection.peering.id
  }

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat_gateway.id
  }

  tags = {
    Name = "${module.env.module_name}-private-${module.env.stage}"
  }
}

resource "aws_route_table_association" "private" {
  subnet_id      = aws_subnet.private.id
  route_table_id = aws_route_table.routes_on_private_subnet.id
}

resource "aws_route_table" "routes_on_public_subnet" {
  vpc_id = aws_vpc.new.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${module.env.module_name}-public-${module.env.stage}"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.routes_on_public_subnet.id
}
