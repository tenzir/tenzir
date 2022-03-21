terraform {
  backend "local" {
    path = ".terraform/state/terraform.tfstate"
  }
}
