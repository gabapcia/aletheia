from typing import Union, Optional, List, Dict
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from spark_plugin.utils.lookup import ConfFromConnection


class SparkSubmitWithCredentialsOperator(SparkSubmitOperator):
    def __init__(
        self,
        packages: Optional[Union[List[str], str]] = None,
        conf: Optional[Dict[str, Union[str, ConfFromConnection]]] = None,
        *args,
        **kwargs,
    ) -> None:
        if isinstance(packages, list):
            packages = ','.join(packages)

        if isinstance(conf, dict):
            nconf = dict()
            for k, v in conf.items():
                if isinstance(v, ConfFromConnection):
                    v = v.get()

                nconf[k] = v

            conf = nconf

        super().__init__(packages=packages, conf=conf, *args, **kwargs)
