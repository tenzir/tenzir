output "access_point_id" {
  value = aws_efs_access_point.access_point.id
}

output "file_system_id" {
  value = aws_efs_file_system.efs_volume.id
}
