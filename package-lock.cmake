# CPM Package Lock
# This file should be committed to version control

CPMDeclarePackage(
  fmt
  NAME fmt
  VERSION 7.1.3
  GIT_TAG 7.1.3
  GITHUB_REPOSITORY fmtlib/fmt
  EXCLUDE_FROM_ALL ON)

CPMDeclarePackage(
  spdlog
  NAME spdlog
  VERSION 1.8.5
  GITHUB_REPOSITORY gabime/spdlog
  OPTIONS "SPDLOG_FMT_EXTERNAL ON"
  EXCLUDE_FROM_ALL ON)
