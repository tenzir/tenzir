rule contains_malware {
  strings:
    $a = "MALWARE"

  condition:
    $a
}
