terraform {
  backend "local" {
    path = "../.terraform/state/step-2/terraform.tfstate"
  }
}
