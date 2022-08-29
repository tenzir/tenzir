"""Stream data from VPC Flowlogs to VAST"""
import dynaconf


VALIDATORS = [
    dynaconf.Validator("VAST_FLOWLOGS_BUCKET_NAME", must_exist=True, ne=""),
    dynaconf.Validator("VAST_FLOWLOGS_BUCKET_REGION", must_exist=True, ne=""),
]
