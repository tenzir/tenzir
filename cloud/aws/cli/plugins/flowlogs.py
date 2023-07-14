"""Stream data from VPC Flowlogs to Tenzir"""
import dynaconf


VALIDATORS = [
    dynaconf.Validator("TENZIR_FLOWLOGS_BUCKET_NAME", must_exist=True, ne=""),
    dynaconf.Validator("TENZIR_FLOWLOGS_BUCKET_REGION", must_exist=True, ne=""),
]
