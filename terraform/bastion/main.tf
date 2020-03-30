/*
|--------------------------------------------------------------------------
| Project Configuration
|--------------------------------------------------------------------------
|
|
*/

// Variables
variable "project" {
    type = string
}

provider "google" {
  project = var.project
  region  = "us-west1"
  zone    = "us-west1-b"
}
