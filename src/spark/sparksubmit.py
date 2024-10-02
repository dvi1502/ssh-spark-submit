import os

from pyhocon import ConfigFactory

from src.spark.deploymode import DeployMode
from src.spark.master import Master


# from deploymode import DeployMode
# from master import Master



class SparkSubmit:

    def __init__(self, conf: list, app_file: str):
        self.workdir = conf['ssh.workdir']
        self.conf = conf
        self.app_file = app_file

    def __str__(self):

        deploy_mode = DeployMode(self.conf)

        master = Master(self.conf)

        spark_configs = " ".join([f"--conf {c}" for c in self.conf["spark.configs"]]) if len(
            self.conf["spark.configs"]) != 0 else ""

        spark_app_args = " ".join([f"{arg}" for arg in self.conf["application.args"]]) if len(
            self.conf["application.args"]) != 0 else ""

        spark_files = f"""{"--files '" + ",".join([f"{self.workdir}/{os.path.basename(f)}" for f in self.conf["spark.files"]]) + "'" if len(self.conf["spark.files"]) != 0 else ""} """

        spark_jars = f"""{"--jars '" + ",".join([f"{self.workdir}/{os.path.basename(f)}" for f in self.conf["spark.jars"]]) + "'" if len(self.conf["spark.jars"]) != 0 else ""} """

        spark_packages = f"""{"--packages '" + ",".join([f"{os.path.basename(f)}" for f in self.conf["spark.packages"]]) + "'" if len(self.conf["spark.packages"]) != 0 else ""} """

        spark_repositories = f"""{"--repositories '" + ",".join([f"{os.path.basename(f)}" for f in self.conf["spark.repositories"]]) + "'" if len(self.conf["spark.repositories"]) != 0 else ""} """

        spark_py_files = f"""{"--py-files '" + ",".join([f"{os.path.basename(f)}" for f in self.conf["spark.py-files"]]) + "'" if len(self.conf["spark.py-files"]) != 0 else ""} """

        spark_submit_command = " ".join([
            f"spark-submit",
            f"--name '{self.conf['application.name']}'",
            f"--class {self.conf['application.class']} ",
            f"{deploy_mode}",
            f"{master}",
            f"--driver-memory {self.conf['spark.driverMemory']}",
            f"--executor-memory {self.conf['spark.executorMemory']}",
            f"{spark_repositories}",
            f"{spark_packages}",
            f"{spark_files}",
            f"{spark_py_files}",
            f"{spark_jars}",
            f"{spark_configs}",
            f"{self.workdir}/{os.path.basename(self.app_file)}",
            f"{spark_app_args}"
        ])

        return spark_submit_command


if __name__ == '__main__':
    conf = ConfigFactory.parse_file(
        """/tests/configs/spark-submit.conf""")

    print(SparkSubmit(conf, "app_file"))
