import os
import re

from pyhocon import ConfigFactory
from spark.deploymode import DeployMode
from spark.master import Master


class SparkSubmit:

    def __init__(self, conf: list, app_file: str):
        self.workdir = conf['ssh.workdir']
        self.conf = conf
        self.app_file = app_file

    def files(self):
        if self.conf.get("spark.files", ""):
            spark_file = []
            for file in self.conf.get("spark.files"):
                filename = os.path.basename(file).replace("upload://", "").replace("server://", "")
                if "#" in filename:
                    src_filename = filename.split("#")[0]
                    dest_filename = filename.split("#")[1]
                else:
                    src_filename = filename
                    dest_filename = filename

                spark_file.append(f"file://{self.workdir}/{os.path.basename(dest_filename)}")

            return f"""--files {",".join([f"{f}" for f in spark_file])} """

        else:
            return ""

    def __str__(self):
        deploy_mode = DeployMode(self.conf)

        master = Master(self.conf)

        principal = ""
        if self.conf.get("spark.principal", ""):
            principal = f"--principal {self.conf['spark.principal']}"

        keytab = ""
        if self.conf.get("spark.keytab", ""):
            keytab = f"--keytab {self.conf['spark.keytab']}"

        spark_configs = ""
        if self.conf.get("spark.configs", ""):
            spark_configs = " ".join([f"--conf {c}" for c in self.conf["spark.configs"]])

        spark_app_args = ""
        if self.conf.get("application.args", ""):
            spark_app_args = " ".join([f"{arg}" for arg in self.conf["application.args"]])

        spark_files = self.files()

        spark_jars = f"""{"--jars " + ",".join([f"file://{self.workdir}/{os.path.basename(f)}" for f in self.conf["spark.jars"]]) + " " if len(self.conf["spark.jars"]) != 0 else ""} """

        spark_packages = f"""{"--packages " + ",".join([f"{os.path.basename(f)}" for f in self.conf["spark.packages"]]) + " " if len(self.conf["spark.packages"]) != 0 else ""} """

        spark_repositories = f"""{"--repositories " + ",".join([f"{os.path.basename(f)}" for f in self.conf["spark.repositories"]]) + " " if len(self.conf["spark.repositories"]) != 0 else ""} """

        spark_py_files = f"""{"--py-files " + ",".join([f"{os.path.basename(f)}" for f in self.conf["spark.py-files"]]) + " " if len(self.conf["spark.py-files"]) != 0 else ""} """

        spark_submit_command = " ".join([
            f"cd {self.workdir}; ",
            f"/bin/spark-submit",
            f"{master}",
            f"{deploy_mode}",
            f"--class {self.conf['application.class']} ",
            f"--name '{self.conf['application.name']}'",
            f"--driver-memory {self.conf['spark.driverMemory']}",
            f"--executor-memory {self.conf['spark.executorMemory']}",
            f"{principal}",
            f"{keytab}",
            f"{spark_repositories}",
            f"{spark_packages}",
            f"{spark_files}",
            f"{spark_py_files}",
            f"{spark_jars}",
            f"{spark_configs}",
            f"{self.workdir}/{os.path.basename(self.app_file)}",
            f"{spark_app_args}"
        ])

        return " ".join(spark_submit_command.split())


if __name__ == '__main__':
    conf = ConfigFactory.parse_file(
        """/tests/configs/utils.conf""")

    print(SparkSubmit(conf, "app_file"))
