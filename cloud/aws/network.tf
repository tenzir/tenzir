resource "aws_subnet" "nat_gateway" {
  vpc_id     = var.vpc_id
  cidr_block = local.public_subnet_cidr
}

resource "aws_subnet" "ids_appliances" {
  vpc_id            = var.vpc_id
  cidr_block        = local.private_subnet_cidr
  availability_zone = aws_subnet.nat_gateway.availability_zone
}

resource "aws_eip" "nat_gateway" {
  vpc = true
}

resource "aws_nat_gateway" "nat_gateway" {
  allocation_id = aws_eip.nat_gateway.id
  subnet_id     = aws_subnet.nat_gateway.id
}

resource "aws_route_table" "route_to_nat_gateway" {
  vpc_id = var.vpc_id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat_gateway.id
  }
}

resource "aws_route_table_association" "nat_gateway" {
  subnet_id      = aws_subnet.ids_appliances.id
  route_table_id = aws_route_table.route_to_nat_gateway.id
}
