rule directory_include_beta {
  strings:
    $a = "BETA"
  condition:
    $a
}
