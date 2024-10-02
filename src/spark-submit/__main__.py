import glob
import os
import sys
from argsparser import parser
from pyhocon import ConfigFactory

from src.spark.sparksubmit import SparkSubmit
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
        filename = os.path.basename(file)
        curdir = os.path.dirname(file)
        if "#" in filename :
            src_filename = filename.split("#")[0]
            dest_filename = filename.split("#")[1]
        else:
            src_filename = filename
            dest_filename = filename

        print(ssh.command(f"rm -f {workdir}/{dest_filename}"))
        ssh.transfer(f"{prjdir}/{curdir}/{src_filename}", f"{workdir}/{dest_filename}")

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
    app_id = ssh.command2(f"{SparkSubmit(conf, app_file)}")
    # --------------------------------------------------------------------------------------------------------------

    # выполнить afterScript
    scripts = conf["ssh.afterScript"]
    for script in scripts:
        print(ssh.command(f"{script.replace('$APPID', f'{app_id}')}"))

    ssh.disconnect()


if __name__ == '__main__':
    sys.exit(main())
