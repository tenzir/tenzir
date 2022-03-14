resource "aws_cloudwatch_log_metric_filter" "vast_server_rates" {
  name           = "${module.env.module_name}_server_rates_${module.env.stage}"
  pattern        = "{ $.key= \"*.rate\" }"
  log_group_name = module.vast_server.log_group_name

  metric_transformation {
    name      = "rates"
    namespace = "${module.env.module_name}-${module.env.stage}"
    value     = "$.value"
    unit      = "Count/Second"
    dimensions = {
      "key" = "$.key"
    }
  }
}
