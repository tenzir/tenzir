terraform {
  backend "local" {
    path = "../.terraform/state/step-1/terraform.tfstate"
  }
}
