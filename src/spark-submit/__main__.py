import glob
import os
import sys
from argsparser import parser
from colors import colors
from pyhocon import ConfigFactory
from ssh import SSH

home_dir = os.path.expanduser("~")

def get_app_verson(prjdir: str):
    import re
    with open(f"{prjdir}/build.sbt") as f:
        lines = f.readlines()
        for line in lines:
            title_search = re.search(r'ThisBuild \/ version := "([0-9\.]+)"', line, re.IGNORECASE)
            if title_search:
                title = title_search.group(1)
                return title


def main():
    # print("Аргументы : ", " ".join(str(e) for e in sys.argv[1:]))

    args = parser.parse_args()
    conf = ConfigFactory.parse_file(args.conf)

    prjdir = conf['projectdir']
    workdir = conf['ssh.workdir']
    app_version = get_app_verson(prjdir)

    print(conf["ssh.host"], conf["ssh.user"], conf["ssh.key"])
    ssh = SSH(conf["ssh.host"], conf["ssh.user"], conf["ssh.key"])

    # создать рабочую папку
    print(ssh.command(f"mkdir -p {workdir}"))

    # скопировать файлы из локальной папки
    files = conf["spark.files"]
    for file in files:
        print(ssh.command(f"rm -f {workdir}/{os.path.basename(file)}"))
        ssh.transfer(f"{prjdir}/{file}", f"{workdir}/{os.path.basename(file)}")

    # скопировать jars из локальной папки
    files = conf["spark.jars"]
    for file in files:
        print(ssh.command(f"rm -f {workdir}/{os.path.basename(file)}"))
        ssh.transfer(f"{prjdir}/{file}", f"{workdir}/{os.path.basename(file)}")


    # скопировать сборку
    files = glob.glob(f'{prjdir}/target/**/*{app_version}.jar', recursive=True)
    app_file = files[0]
    print(ssh.command(f"rm -f {workdir}/{os.path.basename(app_file)}"))
    ssh.transfer(f"{app_file}", f"{workdir}/{os.path.basename(app_file)}")

    # установить переменные окружения
    envs = conf["ssh.env"]
    for env in envs:
        print(ssh.command(f"export {env}"))

    # выполнить beforeScript
    scripts = conf["ssh.beforeScript"]
    for script in scripts:
        print(ssh.command(f"{script}"))


    # spark-submit
    # --------------------------------------------------------------------------------------------------------------
    spark_configs = " ".join([f"--conf {c}" for c in conf["spark.configs"]]) if len(conf["spark.configs"]) != 0 else ""

    spark_app_args = " ".join([f"{arg}" for arg in conf["application.args"]]) if len(
        conf["application.args"]) != 0 else ""

    spark_files = f"""{"--files '" + ",".join([f"{workdir}/{os.path.basename(f)}" for f in conf["spark.files"]]) + "'" if len(conf["spark.files"]) != 0 else ""} """

    spark_jars = f"""{"--jars '" + ",".join([f"{workdir}/{os.path.basename(f)}" for f in conf["spark.jars"]]) + "'" if len(conf["spark.jars"]) != 0 else ""} """

    spark_packages = f"""{"--packages '" + ",".join([f"{os.path.basename(f)}" for f in conf["spark.packages"]]) + "'" if len(conf["spark.packages"]) != 0 else ""} """

    spark_repositories = f"""{"--repositories '" + ",".join([f"{os.path.basename(f)}" for f in conf["spark.repositories"]]) + "'" if len(conf["spark.repositories"]) != 0 else ""} """

    spark_py_files = f"""{"--py-files '" + ",".join([f"{os.path.basename(f)}" for f in conf["spark.py-files"]]) + "'" if len(conf["spark.py-files"]) != 0 else ""} """

    spark_submit_command = f"""spark-submit \
--name '{conf["application.name"]}' \
--class {conf["application.class"]} \
--master {conf["spark.master"]} \
--deploy-mode {conf["spark.deployMode"]} \
--driver-cores {conf["spark.driverCores"]} \
--driver-memory {conf["spark.driverMemory"]} \
--executor-cores {conf["spark.executorCores"]} \
--executor-memory {conf["spark.executorMemory"]} \
{spark_repositories} \
{spark_packages} \
{spark_files} \
{spark_py_files} \
{spark_jars} \
--num-executors {conf["spark.numExecutors"]} \
{spark_configs} \
{workdir}/{os.path.basename(app_file)} \
{spark_app_args} \
    """

    app_id = ssh.command2(f"{spark_submit_command}")
    # --------------------------------------------------------------------------------------------------------------

    # выполнить afterScript
    scripts = conf["ssh.afterScript"]
    for script in scripts:
        print(ssh.command(f"{script.replace('$APPID', f'{app_id}')}"))

    ssh.disconnect()


if __name__ == '__main__':
    sys.exit(main())
