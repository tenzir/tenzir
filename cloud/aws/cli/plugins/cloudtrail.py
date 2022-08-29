"""Stream data from AWS Cloudtrail to VAST"""
import dynaconf


VALIDATORS = [
    dynaconf.Validator("VAST_CLOUDTRAIL_BUCKET_NAME", must_exist=True, ne=""),
    dynaconf.Validator("VAST_CLOUDTRAIL_BUCKET_REGION", must_exist=True, ne=""),
]
