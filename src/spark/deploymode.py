from pyhocon import ConfigFactory

class DeployMode:
    def __init__(self, conf: list):
        if conf.get("spark.deployMode", ""):
            self.deployMode = f"""--deploy-mode {conf["spark.deployMode"]}"""
        else:
            self.deployMode = "--deploy-mode client"

        if conf.get("spark.driverCores", ""):
            self.driverCores = f"""--driver-cores {conf["spark.driverCores"]}"""
        else:
            self.driverCores = ""

    def __str__(self):
        return " ".join([self.deployMode, self.driverCores])


if __name__ == '__main__':
    conf = ConfigFactory.parse_file(
        """/tests/configs/spark-submit.conf""")
    print(DeployMode(conf))
