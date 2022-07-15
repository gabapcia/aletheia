from typing import Union, Optional, List, Dict
from airflow.utils.context import Context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from spark_plugin.utils.lookup import ConfFromConnection, ConfFromXCom


class SparkSubmitWithCredentialsOperator(SparkSubmitOperator):
    def __init__(
        self,
        packages: Optional[Union[List[str], str]] = None,
        conf: Optional[Dict[str, Union[str, ConfFromConnection, ConfFromXCom]]] = None,
        *args,
        **kwargs,
    ) -> None:
        if isinstance(packages, list):
            packages = ','.join(packages)

        if isinstance(conf, dict):
            nconf = dict()
            on_fly = list()
            for k, v in conf.items():
                if isinstance(v, ConfFromConnection):
                    v = v.get()
                elif isinstance(v, ConfFromXCom):
                    on_fly.append(k)

                nconf[k] = v

            conf = nconf

        self._on_fly = on_fly
        super().__init__(packages=packages, conf=conf, *args, **kwargs)

    def execute(self, context: Context) -> None:
        for key in self._on_fly:
            self._conf[key] = self._conf[key].resolve(context)

        return super().execute(context)
