# Multi-region deployment infrastructure for obsrvr-stellar-components
terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }

  backend "s3" {
    bucket = "obsrvr-terraform-state"
    key    = "stellar-components/multi-region/terraform.tfstate"
    region = "us-east-1"
    
    dynamodb_table = "obsrvr-terraform-locks"
    encrypt        = true
  }
}

# Variables
variable "environment" {
  description = "Environment name (production, staging)"
  type        = string
  default     = "production"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "obsrvr-stellar"
}

variable "regions" {
  description = "List of AWS regions for deployment"
  type        = list(string)
  default     = ["us-east-1", "us-west-2", "eu-west-1"]
}

variable "cluster_version" {
  description = "EKS cluster version"
  type        = string
  default     = "1.28"
}

variable "node_groups" {
  description = "Node group configurations"
  type = map(object({
    instance_types = list(string)
    min_size      = number
    max_size      = number
    desired_size  = number
    disk_size     = number
    labels        = map(string)
    taints = list(object({
      key    = string
      value  = string
      effect = string
    }))
  }))
  default = {
    "stellar-workload" = {
      instance_types = ["c5.2xlarge", "c5.4xlarge"]
      min_size      = 3
      max_size      = 20
      desired_size  = 6
      disk_size     = 100
      labels = {
        "workload-type" = "stellar-processing"
        "node-type"     = "compute-optimized"
      }
      taints = [{
        key    = "obsrvr.com/stellar-workload"
        value  = "true"
        effect = "NO_SCHEDULE"
      }]
    }
    "analytics-workload" = {
      instance_types = ["r5.2xlarge", "r5.4xlarge"]
      min_size      = 2
      max_size      = 10
      desired_size  = 4
      disk_size     = 200
      labels = {
        "workload-type" = "analytics"
        "node-type"     = "memory-optimized"
      }
      taints = [{
        key    = "obsrvr.com/analytics-workload"
        value  = "true"
        effect = "NO_SCHEDULE"
      }]
    }
    "storage-workload" = {
      instance_types = ["i3.2xlarge", "i3.4xlarge"]
      min_size      = 2
      max_size      = 8
      desired_size  = 3
      disk_size     = 500
      labels = {
        "workload-type" = "storage"
        "node-type"     = "storage-optimized"
      }
      taints = [{
        key    = "obsrvr.com/storage-workload"
        value  = "true"
        effect = "NO_SCHEDULE"
      }]
    }
  }
}

# Local values
locals {
  common_tags = {
    Environment = var.environment
    Project     = var.project_name
    ManagedBy   = "terraform"
    Component   = "stellar-components"
  }
  
  cluster_name = "${var.project_name}-${var.environment}"
}

# Data sources
data "aws_availability_zones" "available" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  state    = "available"
}

data "aws_caller_identity" "current" {}

# Provider configurations for each region
provider "aws" {
  alias  = "us-east-1"
  region = "us-east-1"
  
  default_tags {
    tags = local.common_tags
  }
}

provider "aws" {
  alias  = "us-west-2"
  region = "us-west-2"
  
  default_tags {
    tags = local.common_tags
  }
}

provider "aws" {
  alias  = "eu-west-1"
  region = "eu-west-1"
  
  default_tags {
    tags = local.common_tags
  }
}

# Dynamic provider configuration
provider "aws" {
  for_each = toset(var.regions)
  alias    = "region"
  region   = each.key
  
  default_tags {
    tags = local.common_tags
  }
}

# VPC and networking for each region
module "vpc" {
  for_each = toset(var.regions)
  source   = "terraform-aws-modules/vpc/aws"
  version  = "~> 5.0"
  
  providers = {
    aws = aws.region[each.key]
  }

  name = "${local.cluster_name}-vpc-${each.key}"
  cidr = "10.${index(var.regions, each.key)}.0.0/16"

  azs = data.aws_availability_zones.available[each.key].names
  private_subnets = [
    "10.${index(var.regions, each.key)}.1.0/24",
    "10.${index(var.regions, each.key)}.2.0/24",
    "10.${index(var.regions, each.key)}.3.0/24"
  ]
  public_subnets = [
    "10.${index(var.regions, each.key)}.101.0/24",
    "10.${index(var.regions, each.key)}.102.0/24",
    "10.${index(var.regions, each.key)}.103.0/24"
  ]
  database_subnets = [
    "10.${index(var.regions, each.key)}.21.0/24",
    "10.${index(var.regions, each.key)}.22.0/24",
    "10.${index(var.regions, each.key)}.23.0/24"
  ]

  enable_nat_gateway     = true
  single_nat_gateway     = false
  enable_vpn_gateway     = false
  enable_dns_hostnames   = true
  enable_dns_support     = true

  # VPC Flow Logs
  enable_flow_log                      = true
  create_flow_log_cloudwatch_iam_role  = true
  create_flow_log_cloudwatch_log_group = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
    "kubernetes.io/cluster/${local.cluster_name}-${each.key}" = "shared"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1
    "kubernetes.io/cluster/${local.cluster_name}-${each.key}" = "shared"
  }

  tags = merge(local.common_tags, {
    Region = each.key
  })
}

# EKS clusters for each region
module "eks" {
  for_each = toset(var.regions)
  source   = "terraform-aws-modules/eks/aws"
  version  = "~> 19.0"
  
  providers = {
    aws = aws.region[each.key]
  }

  cluster_name    = "${local.cluster_name}-${each.key}"
  cluster_version = var.cluster_version

  vpc_id                         = module.vpc[each.key].vpc_id
  subnet_ids                     = module.vpc[each.key].private_subnets
  cluster_endpoint_public_access = true
  cluster_endpoint_private_access = true

  # Cluster access entry
  enable_cluster_creator_admin_permissions = true

  # Cluster add-ons
  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent = true
    }
  }

  # Node groups
  eks_managed_node_groups = {
    for node_group_name, config in var.node_groups : node_group_name => {
      name           = "${node_group_name}-${each.key}"
      instance_types = config.instance_types
      
      min_size     = config.min_size
      max_size     = config.max_size
      desired_size = config.desired_size
      
      disk_size = config.disk_size
      disk_type = "gp3"
      
      labels = merge(config.labels, {
        region = each.key
      })
      
      taints = config.taints
      
      subnet_ids = module.vpc[each.key].private_subnets
      
      # Launch template configuration
      create_launch_template = true
      launch_template_name   = "${node_group_name}-${each.key}"
      
      # Security groups
      vpc_security_group_ids = [aws_security_group.stellar_nodes[each.key].id]
      
      # User data for node initialization
      pre_bootstrap_user_data = <<-EOT
        #!/bin/bash
        # Install additional monitoring agents
        curl -sSL https://get.datadoghq.com/docker-agent-install.sh | bash
        
        # Configure node for stellar workloads
        echo 'vm.max_map_count=262144' >> /etc/sysctl.conf
        echo 'fs.file-max=65536' >> /etc/sysctl.conf
        sysctl -p
      EOT
      
      tags = merge(local.common_tags, {
        Region = each.key
        NodeGroup = node_group_name
      })
    }
  }

  tags = merge(local.common_tags, {
    Region = each.key
  })
}

# Security groups for stellar nodes
resource "aws_security_group" "stellar_nodes" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  name_prefix = "${local.cluster_name}-stellar-nodes-${each.key}"
  vpc_id      = module.vpc[each.key].vpc_id

  # Arrow Flight ports
  ingress {
    description = "Arrow Flight - Stellar Source"
    from_port   = 8815
    to_port     = 8815
    protocol    = "tcp"
    cidr_blocks = [module.vpc[each.key].vpc_cidr_block]
  }

  ingress {
    description = "Arrow Flight - TTP Processor"
    from_port   = 8816
    to_port     = 8816
    protocol    = "tcp"
    cidr_blocks = [module.vpc[each.key].vpc_cidr_block]
  }

  ingress {
    description = "Arrow Flight - Analytics Sink"
    from_port   = 8817
    to_port     = 8817
    protocol    = "tcp"
    cidr_blocks = [module.vpc[each.key].vpc_cidr_block]
  }

  # WebSocket and REST API
  ingress {
    description = "WebSocket"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Public access for WebSocket
  }

  ingress {
    description = "REST API"
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Public access for REST API
  }

  # Health and metrics
  ingress {
    description = "Health checks"
    from_port   = 8088
    to_port     = 8088
    protocol    = "tcp"
    cidr_blocks = [module.vpc[each.key].vpc_cidr_block]
  }

  ingress {
    description = "Prometheus metrics"
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = [module.vpc[each.key].vpc_cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, {
    Name   = "${local.cluster_name}-stellar-nodes-${each.key}"
    Region = each.key
  })
}

# Global load balancer for multi-region traffic distribution
resource "aws_route53_zone" "main" {
  provider = aws.us-east-1
  
  name = "stellar.obsrvr.com"
  
  tags = local.common_tags
}

# Health checks for each region
resource "aws_route53_health_check" "region_health" {
  for_each = toset(var.regions)
  
  provider = aws.us-east-1
  
  fqdn                            = "stellar-${each.key}.obsrvr.com"
  port                            = 8088
  type                            = "HTTP"
  resource_path                   = "/health"
  failure_threshold               = 3
  request_interval                = 30
  cloudwatch_logs_region          = each.key
  cloudwatch_logs_group_name      = "/aws/route53/healthcheck"
  insufficient_data_health_status = "Failure"

  tags = merge(local.common_tags, {
    Name   = "stellar-health-${each.key}"
    Region = each.key
  })
}

# Route53 records with health check-based routing
resource "aws_route53_record" "stellar_regional" {
  for_each = toset(var.regions)
  
  provider = aws.us-east-1
  
  zone_id = aws_route53_zone.main.zone_id
  name    = "stellar-${each.key}.obsrvr.com"
  type    = "A"
  
  set_identifier = each.key
  
  # Weighted routing with health checks
  weighted_routing_policy {
    weight = 100
  }
  
  health_check_id = aws_route53_health_check.region_health[each.key].id
  
  alias {
    name                   = module.eks[each.key].cluster_endpoint
    zone_id               = module.eks[each.key].cluster_arn
    evaluate_target_health = true
  }
}

# Primary domain with failover routing
resource "aws_route53_record" "stellar_primary" {
  provider = aws.us-east-1
  
  zone_id = aws_route53_zone.main.zone_id
  name    = "stellar.obsrvr.com"
  type    = "A"
  
  set_identifier = "primary"
  
  failover_routing_policy {
    type = "PRIMARY"
  }
  
  health_check_id = aws_route53_health_check.region_health["us-east-1"].id
  
  alias {
    name                   = aws_route53_record.stellar_regional["us-east-1"].name
    zone_id               = aws_route53_zone.main.zone_id
    evaluate_target_health = true
  }
}

# Secondary failover record
resource "aws_route53_record" "stellar_secondary" {
  provider = aws.us-east-1
  
  zone_id = aws_route53_zone.main.zone_id
  name    = "stellar.obsrvr.com"
  type    = "A"
  
  set_identifier = "secondary"
  
  failover_routing_policy {
    type = "SECONDARY"
  }
  
  alias {
    name                   = aws_route53_record.stellar_regional["us-west-2"].name
    zone_id               = aws_route53_zone.main.zone_id
    evaluate_target_health = true
  }
}

# S3 buckets for data storage in each region
resource "aws_s3_bucket" "stellar_data" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  bucket = "obsrvr-stellar-data-${each.key}-${random_id.bucket_suffix.hex}"

  tags = merge(local.common_tags, {
    Region = each.key
    Purpose = "stellar-data-storage"
  })
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# S3 bucket configurations
resource "aws_s3_bucket_versioning" "stellar_data" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  bucket = aws_s3_bucket.stellar_data[each.key].id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_encryption" "stellar_data" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  bucket = aws_s3_bucket.stellar_data[each.key].id

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
}

# Cross-region replication
resource "aws_s3_bucket_replication_configuration" "stellar_data" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  role   = aws_iam_role.replication[each.key].arn
  bucket = aws_s3_bucket.stellar_data[each.key].id

  dynamic "rule" {
    for_each = [for region in var.regions : region if region != each.key]
    content {
      id     = "replicate-to-${rule.value}"
      status = "Enabled"

      destination {
        bucket        = aws_s3_bucket.stellar_data[rule.value].arn
        storage_class = "STANDARD_IA"
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.stellar_data]
}

# IAM role for S3 replication
resource "aws_iam_role" "replication" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  name = "stellar-s3-replication-${each.key}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_policy" "replication" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  name = "stellar-s3-replication-policy-${each.key}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObjectVersionForReplication",
          "s3:GetObjectVersionAcl"
        ]
        Resource = "${aws_s3_bucket.stellar_data[each.key].arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.stellar_data[each.key].arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ReplicateObject",
          "s3:ReplicateDelete"
        ]
        Resource = [for region in var.regions : "${aws_s3_bucket.stellar_data[region].arn}/*" if region != each.key]
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "replication" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  role       = aws_iam_role.replication[each.key].name
  policy_arn = aws_iam_policy.replication[each.key].arn
}

# RDS Aurora Global Database for metadata
resource "aws_rds_global_cluster" "stellar_metadata" {
  provider = aws.us-east-1
  
  global_cluster_identifier = "stellar-metadata-global"
  engine                   = "aurora-postgresql"
  engine_version           = "15.4"
  database_name            = "stellar_metadata"
  master_username          = "stellar_admin"
  manage_master_user_password = true
  
  deletion_protection = true
  
  tags = local.common_tags
}

# Aurora clusters in each region
resource "aws_rds_cluster" "stellar_metadata" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  cluster_identifier = "stellar-metadata-${each.key}"
  
  global_cluster_identifier = aws_rds_global_cluster.stellar_metadata.id
  engine                   = aws_rds_global_cluster.stellar_metadata.engine
  engine_version           = aws_rds_global_cluster.stellar_metadata.engine_version
  
  db_subnet_group_name   = aws_db_subnet_group.stellar[each.key].name
  vpc_security_group_ids = [aws_security_group.rds[each.key].id]
  
  backup_retention_period = 35
  preferred_backup_window = "03:00-04:00"
  preferred_maintenance_window = "sun:04:00-sun:05:00"
  
  skip_final_snapshot = false
  final_snapshot_identifier = "stellar-metadata-${each.key}-final-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"
  
  enabled_cloudwatch_logs_exports = ["postgresql"]
  
  tags = merge(local.common_tags, {
    Region = each.key
  })
  
  lifecycle {
    ignore_changes = [final_snapshot_identifier]
  }
}

# DB subnet groups
resource "aws_db_subnet_group" "stellar" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  name       = "stellar-db-subnet-${each.key}"
  subnet_ids = module.vpc[each.key].database_subnets

  tags = merge(local.common_tags, {
    Name   = "stellar-db-subnet-${each.key}"
    Region = each.key
  })
}

# Security group for RDS
resource "aws_security_group" "rds" {
  for_each = toset(var.regions)
  
  provider = aws.region[each.key]
  
  name_prefix = "stellar-rds-${each.key}"
  vpc_id      = module.vpc[each.key].vpc_id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.stellar_nodes[each.key].id]
  }

  tags = merge(local.common_tags, {
    Name   = "stellar-rds-${each.key}"
    Region = each.key
  })
}

# Outputs
output "cluster_endpoints" {
  description = "EKS cluster endpoints by region"
  value = {
    for region in var.regions : region => module.eks[region].cluster_endpoint
  }
}

output "cluster_names" {
  description = "EKS cluster names by region"
  value = {
    for region in var.regions : region => module.eks[region].cluster_name
  }
}

output "s3_buckets" {
  description = "S3 bucket names by region"
  value = {
    for region in var.regions : region => aws_s3_bucket.stellar_data[region].bucket
  }
}

output "route53_zone_id" {
  description = "Route53 hosted zone ID"
  value = aws_route53_zone.main.zone_id
}

output "dns_endpoints" {
  description = "DNS endpoints for the service"
  value = {
    primary   = aws_route53_record.stellar_primary.fqdn
    secondary = aws_route53_record.stellar_secondary.fqdn
    regional = {
      for region in var.regions : region => aws_route53_record.stellar_regional[region].fqdn
    }
  }
}

output "rds_cluster_endpoints" {
  description = "RDS Aurora cluster endpoints by region"
  value = {
    for region in var.regions : region => {
      writer = aws_rds_cluster.stellar_metadata[region].endpoint
      reader = aws_rds_cluster.stellar_metadata[region].reader_endpoint
    }
  }
  sensitive = true
}