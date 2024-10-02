from pyhocon import ConfigFactory, ConfigTree


class Master:
    def __init__(self, conf: ConfigTree):

        if conf.get("spark.master", "") == "local":
            self.master = Local(conf)
        elif conf.get("spark.master", "") == "spark":
            self.master = Spark(conf)
        elif conf.get("spark.master", "") == "yarn":
            self.master = Yarn(conf)
        elif conf.get("spark.master", "") == "mesos":
            self.master = Mesos(conf)
        elif conf.get("spark.master", "") == "k8s":
            self.master = K8s(conf)
        else:
            self.master = Local(conf)

    def __str__(self):
        return self.master.__str__()


class Spark(Master):
    def __init__(self, conf: ConfigTree):
        if conf.get("spark.urls", ""):
            self.urls = "--master " + ",".join([f"spark://{c}" for c in conf.get("spark.urls")])
        else:
            self.urls = ""

        if conf.get("spark.totalExecutorCores", ""):
            self.totalExecutorCores = "--total-executor-cores " + str(conf.get("spark.totalExecutorCores"))
        else:
            self.totalExecutorCores = ""

        self.supervise = conf.get("spark.supervise", False)

    def __str__(self):
        base = [
            self.urls,
            self.totalExecutorCores,
            "--supervise" if self.supervise else ""
        ]
        return " ".join(base)


class Mesos(Master):
    def __init__(self, conf: ConfigTree):

        if conf.get("spark.urls", ""):
            self.urls = "--master " + ",".join([f"mesos://{c}" for c in conf.get("spark.urls")])
        else:
            self.urls = ""

        if conf.get("spark.totalExecutorCores", ""):
            self.totalExecutorCores = "--total-executor-cores " + str(conf.get("spark.totalExecutorCores"))
        else:
            self.totalExecutorCores = ""

        self.zooKeeper = conf.get("spark.zooKeeper", False)
        self.supervise = conf.get("spark.supervise", False)

    def __str__(self):
        base = [
            self.urls,
            self.totalExecutorCores,
            "--supervise" if self.supervise else ""
        ]
        return " ".join(base)


class Yarn(Master):
    def __init__(self, conf: ConfigTree):

        if conf.get("spark.numExecutors", ""):
            self.numExecutors = "--num-executors " + str(conf.get("spark.numExecutors"))
        else:
            self.numExecutors = ""

        if conf.get("spark.executorCores", ""):
            self.executorCores = "--executor-cores " + str(conf.get("spark.executorCores"))
        else:
            self.executorCores = ""

        if conf.get("spark.queue", ""):
            self.queue = "--queue " + str(conf.get("spark.queue"))
        else:
            self.queue = ""

        if conf.get("spark.principal", ""):
            self.principal = "--principal " + str(conf.get("spark.principal"))
        else:
            self.principal = ""

        if conf.get("spark.keytab", ""):
            self.keytab = "--keytab " + str(conf.get("spark.keytab"))
        else:
            self.keytab = ""

        if conf.get("spark.archives", ""):
            self.archives = "--archives " + ",".join([f"{c}" for c in conf.get("spark.archives")])
        else:
            self.archives = ""

    def __str__(self):
        cmd = [
            "--master yarn",
            self.numExecutors,
            self.executorCores,
            self.queue,
            self.principal,
            self.keytab,
            self.archives
        ]

        return " ".join(cmd)


#
class K8s(Master):
    def __init__(self, conf: ConfigTree):
        if conf.get("spark.urls", ""):
            self.urls = "--master " + ",".join([f"k8s://{c}" for c in conf.get("spark.urls")])
        else:
            self.urls = ""

    def __str__(self):
        base = [
            self.urls,
        ]
        return " ".join(base)


class Local(Master):
    def __init__(self, conf: ConfigTree):
        if conf.get("spark.numWorkers", ""):
            self.numWorkers = conf.get("spark.numWorkers")
        else:
            self.numWorkers = None

        if conf.get("spark.maxFailures", ""):
            self.maxFailures = conf.get("spark.maxFailures")
        else:
            self.maxFailures = None

    def __str__(self):
        if self.numWorkers is None and self.maxFailures is None:
            master = "local[*]"

        elif self.numWorkers is not None and self.maxFailures is not None:
            master = f"local[{self.numWorkers}:{self.maxFailures}]"

        elif self.numWorkers is not None and self.maxFailures is None:
            master = f"local[{self.numWorkers}]"

        elif self.numWorkers is None and self.maxFailures is not None:
            master = f"local[*:{self.maxFailures}]"

        return f"--master {master}"


if __name__ == '__main__':
    conf = ConfigFactory.parse_file(
        """/tests/configs/spark-submit.conf""")

    print(Master(conf))
