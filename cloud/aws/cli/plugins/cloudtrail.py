"""Stream data from AWS Cloudtrail to VAST"""
import dynaconf


VALIDATORS = [
    dynaconf.Validator("TENZIR_CLOUDTRAIL_BUCKET_NAME", must_exist=True, ne=""),
    dynaconf.Validator("TENZIR_CLOUDTRAIL_BUCKET_REGION", must_exist=True, ne=""),
]
