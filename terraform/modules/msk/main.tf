# Managed Kafka.
#
# Three brokers across three AZs with replication factor 3 and min.insync.replicas 2.
# That combination is what makes acks=all meaningful: the write is acknowledged only
# once two replicas hold it, so a single broker or AZ failure loses nothing. With
# min.insync.replicas=1, acks=all degrades to acks=1 the moment a replica falls
# behind, which is precisely when durability matters most.

resource "aws_msk_configuration" "fleetstream" {
  name           = "${var.cluster_name}-config"
  kafka_versions = [var.kafka_version]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=12
    log.retention.hours=168
    compression.type=zstd
    unclean.leader.election.enable=false
  PROPERTIES
}

resource "aws_msk_cluster" "fleetstream" {
  cluster_name           = var.cluster_name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count
  tags                   = var.tags

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = var.security_group_ids

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_volume_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.fleetstream.arn
    revision = aws_msk_configuration.fleetstream.latest_revision
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = var.kms_key_arn
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${var.cluster_name}"
  retention_in_days = 30
  tags              = var.tags
}
