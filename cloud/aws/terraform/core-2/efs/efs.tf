resource "aws_efs_file_system" "efs_volume" {
  tags = {
    Name = "${module.env.module_name}-${var.name}-${module.env.stage}"
  }
}

resource "aws_efs_mount_target" "efs_target" {
  file_system_id  = aws_efs_file_system.efs_volume.id
  subnet_id       = var.subnet_id
  security_groups = [var.security_group_id]
}


resource "aws_efs_access_point" "access_point" {
  file_system_id = aws_efs_file_system.efs_volume.id

  root_directory {
    path = "/vast"
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
