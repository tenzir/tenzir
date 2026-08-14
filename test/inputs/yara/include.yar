rule included {
  strings:
    $a = "INCLUDED"
  condition:
    $a
}
