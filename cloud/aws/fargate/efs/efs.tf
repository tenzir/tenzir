resource "aws_security_group" "efs" {
  name        = "${module.env.module_name}_${var.name}_efs_${module.env.stage}"
  description = "allow inbound nfs access"
  vpc_id      = var.vpc_id

  ingress {
    protocol        = "tcp"
    from_port       = 2049
    to_port         = 2049
    security_groups = [var.ingress_security_group_id]
  }

}

resource "aws_efs_file_system" "efs_volume" {
  tags = {
    Name = "${module.env.module_name}-${var.name}-${module.env.stage}"
  }
}

resource "aws_efs_mount_target" "efs_target" {
  file_system_id  = aws_efs_file_system.efs_volume.id
  subnet_id       = var.subnet_id
  security_groups = [aws_security_group.efs.id]
}


resource "aws_efs_access_point" "access_point" {
  file_system_id = aws_efs_file_system.efs_volume.id

  root_directory {
    path = "/storage"
    creation_info {
      owner_gid   = 1000
      owner_uid   = 1000
      permissions = "755"
    }
  }

  posix_user {
    gid = 1000
    uid = 1000
  }
}
