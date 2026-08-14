rule directory_include_alpha {
  strings:
    $a = "ALPHA"
  condition:
    $a
}
