from typing import Union, Optional, List, Dict
from airflow.utils.context import Context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from spark_plugin.utils.lookup import ConfFromConnection, ConfFromXCom


class SparkSubmitWithCredentialsOperator(SparkSubmitOperator):
    def __init__(
        self,
        packages: Optional[Union[List[str], str]] = None,
        conf: Optional[Dict[str, str]] = None,
        lazy_conf: Optional[Dict[str, Union[ConfFromConnection, ConfFromXCom]]] = None,
        *args,
        **kwargs,
    ) -> None:
        conf = conf or dict()
        lazy_conf = lazy_conf or dict()

        if isinstance(packages, list):
            packages = ','.join(packages)

        self._on_fly: List[ConfFromXCom] = list()

        conf = self._load_conf(conf=conf)
        conf.update(self._load_conf(conf=lazy_conf))

        super().__init__(packages=packages, conf=conf, *args, **kwargs)

    def _load_conf(self, conf: Dict[str, Union[ConfFromConnection, ConfFromXCom]]) -> Dict[str, str]:
        nconf = dict()

        for k, v in conf.items():
            if isinstance(v, ConfFromConnection):
                v = v.get()
            elif isinstance(v, ConfFromXCom):
                self._on_fly.append(k)

            nconf[k] = v

        return nconf

    def execute(self, context: Context) -> None:
        for key in self._on_fly:
            self._conf[key] = self._conf[key].resolve(context)

        return super().execute(context)
