locals {
  metric_namespace = "${module.env.module_name}-${module.env.stage}"
}

resource "aws_cloudwatch_log_metric_filter" "vast_server_rates" {
  name           = "${module.env.module_name}_server_rates_${module.env.stage}"
  pattern        = "{ $.key= \"*.rate\" }"
  log_group_name = module.vast_server.log_group_name

  metric_transformation {
    name      = "rates"
    namespace = local.metric_namespace
    value     = "$.value"
    unit      = "Count/Second"
    dimensions = {
      "key" = "$.key"
    }
  }
}

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${module.env.module_name}-preset-${module.env.stage}-${var.region_name}"

  dashboard_body = <<EOF
{
    "widgets": [
        {
            "type": "metric",
            "x": 0,
            "y": 0,
            "width": 24,
            "height": 6,
            "properties": {
                "metrics": [
                    [ "${local.metric_namespace}", "rates", "key", "node_throughput.rate" ],
                    [ "...", { "stat": "Minimum" } ],
                    [ "...", { "stat": "Maximum" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${var.region_name}",
                "stat": "Average",
                "period": 1,
                "title": "VAST Server - Node throughput Rate"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 6,
            "width": 8,
            "height": 6,
            "properties": {
                "metrics": [
                    [ "ECS/ContainerInsights", "NetworkTxBytes", "TaskDefinitionFamily", "vast-cloud-vast-server-default", "ClusterName", "vast-cloud-cluster-default" ],
                    [ ".", "NetworkRxBytes", ".", ".", ".", "." ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${var.region_name}",
                "stat": "Average",
                "period": 60,
                "title": "VAST Server - Network"
            }
        },
        {
            "type": "metric",
            "x": 8,
            "y": 6,
            "width": 8,
            "height": 6,
            "properties": {
                "metrics": [
                    [ "ECS/ContainerInsights", "CpuUtilized", "TaskDefinitionFamily", "vast-cloud-vast-server-default", "ClusterName", "vast-cloud-cluster-default" ],
                    [ ".", "CpuReserved", ".", ".", ".", "." ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${var.region_name}",
                "stat": "Maximum",
                "period": 60,
                "title": "VAST Server - CPU"
            }
        },
        {
            "type": "metric",
            "x": 16,
            "y": 6,
            "width": 8,
            "height": 6,
            "properties": {
                "metrics": [
                    [ "ECS/ContainerInsights", "MemoryUtilized", "TaskDefinitionFamily", "vast-cloud-vast-server-default", "ClusterName", "vast-cloud-cluster-default", { "region": "${var.region_name}" } ],
                    [ "ECS/ContainerInsights", "MemoryReserved", "TaskDefinitionFamily", "vast-cloud-vast-server-default", "ClusterName", "vast-cloud-cluster-default", { "region": "${var.region_name}" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "${var.region_name}",
                "stat": "Maximum",
                "period": 60,
                "title": "VAST Server - Memory"
            }
        },
        {
            "type": "log",
            "x": 0,
            "y": 12,
            "width": 12,
            "height": 6,
            "properties": {
                "query": "SOURCE '${module.vast_server.log_group_name}' | fields ts, level, message\n| filter key = \"vast_log\"\n| sort @timestamp desc\n| limit 20",
                "region": "${var.region_name}",
                "stacked": false,
                "view": "table",
                "title": "Server logs"
            }
        },
        {
            "type": "log",
            "x": 12,
            "y": 12,
            "width": 12,
            "height": 6,
            "properties": {
                "query": "SOURCE '${module.vast_client.log_group_name}' | fields @timestamp, @message\n| sort @timestamp desc\n| limit 20",
                "region": "${var.region_name}",
                "stacked": false,
                "view": "table",
                "title": "Lambda logs"
            }
        }
    ]
}
EOF
}
