variable "project" {
    type = string
}

//variable "zone" {
//   type = string
//}

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
    type = string
}

variable "neo4j-heap-size" {
    type = string
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