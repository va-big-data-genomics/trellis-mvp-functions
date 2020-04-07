variable "project" {
    type = string
}

variable "region" {
    type = string
    default = "us-west1"
}

variable "zone" {
    type = string
    default = "us-west1-b"
}

variable "data-group" {
    type = string
    default = "phase3"
}

variable "external-bastion-ip" {
    type = string
}

//variable "stable-cos-image" {
//    type = string
//}

variable "neo4j-image" {
    default = "neo4j:3.5.4"
}

variable "neo4j-pagecache-size" {
    type    = string
    default = "32G"
}

variable "neo4j-heap-size" {
    type    = string
    default = "32G"
}

variable "github-owner" {
    type = string
    description = "Owner of the Trellis GitHub repo."
}

variable "github-repo" {
    type = string
    description = "Name of the Trellis GitHub repo."
}

variable "github-branch-pattern" {
    type = string
}

variable "gatk-github-owner" {
    type = string
}

variable "gatk-github-repo" {
    type = string
}

variable "gatk-github-branch-pattern" {
    type = string
}