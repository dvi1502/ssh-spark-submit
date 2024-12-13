import glob
import os
import re
import sys

from config import Config

# sys.path.append('src/spark')
# sys.path.append('src/utils')

from utils.argsparser import parser
from pyhocon import ConfigFactory
from spark.sparksubmit import SparkSubmit
from utils.colors import colors
from utils.ssh import SSH

home_dir = os.path.expanduser("~")


def get_app_verson(prjdir: str):
    import re
    with open(f"{prjdir}/build.sbt", mode="r", encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            title_search = re.search(r'ThisBuild \/ version := "([0-9\.]+.*)"', line, re.IGNORECASE)
            if title_search:
                title = title_search.group(1)
                print(f"app version = {colors.fg.lightgreen}{title}{colors.endc}")
                return title


def move_files(files, prjdir, workdir, ssh):
    # скопировать файлы из локальной папки
    # upload: //
    # server: //

    for file in files:
        match = re.search(r"""^(.*):\/\/(.*)""", file)
        driver = match.group(1) + "://"
        filename = os.path.basename(match.group(2))
        curdir = os.path.dirname(match.group(2))

        # print(f"driver = {driver}")
        # print(f"filename = {filename}")
        # print(f"curdir = {curdir}")

        if "#" in filename:
            src_filename = filename.split("#")[0]
            dest_filename = filename.split("#")[1]
        else:
            src_filename = filename
            dest_filename = filename

        if driver == "upload://":
            print(ssh.command(f"rm -f {workdir}/{dest_filename}"))
            ssh.transfer(f"{prjdir}/{curdir}/{src_filename}", f"{workdir}/{dest_filename}")

        elif driver == "server://":
            print(ssh.command(f"rm -f {workdir}/{dest_filename}"))
            print(ssh.command(f"cp {curdir}/{src_filename} {workdir}/{dest_filename}"))


def move_jars(files, prjdir, workdir, ssh):
    for file in files:
        match = re.search(r"""^(.*):\/\/(.*)""", file)
        driver = match.group(1) + "://"
        filename = os.path.basename(match.group(2))
        curdir = os.path.dirname(match.group(2))

        # print(f"driver = {driver}")
        # print(f"filename = {filename}")
        # print(f"curdir = {curdir}")

        if driver == "upload://":
            print(ssh.command(f"rm -f {workdir}/{filename}"))
            ssh.transfer(f"{prjdir}/{curdir}/{filename}", f"{workdir}/{filename}")

        elif driver == "server://":
            print(ssh.command(f"rm -f {workdir}/{filename}"))
            print(ssh.command(f"cp {curdir}/{filename} {workdir}/{filename}"))

def move_app(app_version, prjdir, workdir, ssh):
    files = glob.glob(f'{prjdir}/target/**/*{app_version}*.jar', recursive=True)
    app_file = files[0]
    print(ssh.command(f"rm -f {workdir}/{os.path.basename(app_file)}"))
    ssh.transfer(f"{app_file}", f"{workdir}/{os.path.basename(app_file)}")
    return os.path.basename(app_file)

def move_to_hdfs(workdir, filename, hdfs, ssh):
    print(ssh.command(f"if $(hdfs dfs -test -d {hdfs}) ; then echo 'EXISTS'; else echo 'NOT EXISTS'; hdfs dfs -mkdir -p {hdfs}; hdfs dfs -chmod 755 {hdfs} ; fi;"))
    print(ssh.command(f"hdfs dfs -put -f {workdir}/{filename} {hdfs}"))
    print(ssh.command(f"hdfs dfs -chmod 644 {hdfs}/{filename} ; hdfs dfs -ls {hdfs}"))

def run(conf: Config):
    # print("Аргументы : ", " ".join(str(e) for e in sys.argv[1:]))

    prjdir = conf['projectdir']
    workdir = conf['ssh.workdir']
    app_version = get_app_verson(prjdir)

    print("prjdir   =\t", f"{colors.fg.lightgreen}", prjdir, f"{colors.endc}")
    print("workdir  =\t", f"{colors.fg.lightgreen}", workdir, f"{colors.endc}")
    print("ssh.host =\t", f"{colors.fg.lightgreen}", conf["ssh.host"], f"{colors.endc}")
    print("ssh.user =\t", f"{colors.fg.lightgreen}", conf["ssh.user"], f"{colors.endc}")
    print("ssh.key  =\t", f"{colors.fg.lightgreen}", conf["ssh.key"], f"{colors.endc}")
    print(f"{colors.fg.green}---------------------------------------{colors.endc}")

    ssh = SSH(conf["ssh.host"], conf["ssh.user"], conf["ssh.key"])

    # создать рабочую папку
    print(ssh.command(f"mkdir -p {workdir}"))

    # скопировать файлы из локальной папки
    move_files(conf["spark.files"], prjdir, workdir, ssh)

    # скопировать jars из локальной папки
    move_jars(conf["spark.jars"], prjdir, workdir, ssh)

    # скопировать сборку
    move_app(app_version, prjdir, workdir, ssh)

    # установить переменные окружения
    envs = conf["ssh.env"]
    for env in envs:
        print(ssh.command(f"export {env}"))

    # выполнить beforeScript
    scripts = conf["ssh.beforeScript"]
    for script in scripts:
        print(ssh.command(f"{script}"))

    # utils
    # --------------------------------------------------------------------------------------------------------------
    app_id = ssh.command2(f"{SparkSubmit(conf, app_file)}")
    # --------------------------------------------------------------------------------------------------------------

    # выполнить afterScript
    scripts = conf["ssh.afterScript"]
    for script in scripts:
        print(ssh.command(f"{script.replace('$APPID', f'{app_id}')}"))

    ssh.disconnect()


def show(conf: Config):
    from pyhocon import HOCONConverter
    return HOCONConverter().to_hocon(conf)

def deploy(conf: Config):
    prjdir = conf['projectdir']
    workdir = conf['ssh.workdir']
    app_version = get_app_verson(prjdir)

    print("prjdir   =\t", f"{colors.fg.lightgreen}", prjdir, f"{colors.endc}")
    print("workdir  =\t", f"{colors.fg.lightgreen}", workdir, f"{colors.endc}")
    print("ssh.host =\t", f"{colors.fg.lightgreen}", conf["ssh.host"], f"{colors.endc}")
    print("ssh.user =\t", f"{colors.fg.lightgreen}", conf["ssh.user"], f"{colors.endc}")
    print("ssh.key  =\t", f"{colors.fg.lightgreen}", conf["ssh.key"], f"{colors.endc}")
    print(f"{colors.fg.green}---------------------------------------{colors.endc}")
    print("deploy.hdfs  =\t", f"{colors.fg.lightgreen}", conf["deploy.hdfs"], f"{colors.endc}")
    print(f"{colors.fg.green}---------------------------------------{colors.endc}")

    ssh = SSH(conf["ssh.host"], conf["ssh.user"], conf["ssh.key"])

    # создать рабочую папку
    print(ssh.command(f"mkdir -p {workdir}"))

    # скопировать файлы из локальной папки
    move_files(conf["spark.files"], prjdir, workdir, ssh)

    # скопировать jars из локальной папки
    move_jars(conf["spark.jars"], prjdir, workdir, ssh)

    # скопировать сборку
    filename = move_app(app_version, prjdir, workdir, ssh)

    # отправить на HDFS
    move_to_hdfs(workdir, filename,  conf["deploy.hdfs"], ssh)

def new(spark_project_path: str):
    from datetime import date
    today = date.today()

    current_path = spark_project_path if spark_project_path else os.path.dirname(os.path.abspath(__file__))
    hostname = "hdp3-client.dmp.vimpelcom.ru"
    user = os.getlogin().lower()
    home_dir_local = os.path.expanduser("~")
    ssh_key = os.path.join(home_dir_local, ".ssh", "id_dmp")
    app_name = os.path.basename(os.path.normpath(current_path))

    run_path = os.path.join(current_path, ".run")
    if not os.path.exists(run_path):
        os.mkdir(run_path, mode=0o777, dir_fd=None)
        files = ["app.conf"]
        files = ',\n'.join(
            ["\"\"\"" + os.path.join(run_path, file).replace(current_path, './') + "\"\"\"" for file in files if
             not file.endswith(('.jar', '.py', 'spark-submit.conf'))])

    else:
        files = os.listdir(run_path)
        jars = ',\n'.join(
            ["\"\"\"upload://" + os.path.join(run_path, file).replace(current_path, './') + "\"\"\"" for file in files
             if file.endswith(('.jar'))])
        py_files = ',\n'.join(
            ["\"\"\"upload://" + os.path.join(run_path, file).replace(current_path, './') + "\"\"\"" for file in files
             if file.endswith(('.py'))])
        files = ',\n'.join(
            ["\"\"\"upload://" + os.path.join(run_path, file).replace(current_path, './') + "\"\"\"" for file in files
             if not file.endswith(('.jar', '.py', 'spark-submit.conf'))])

    text = f"""projectdir = \"\"\"{current_path}\"\"\"

ssh {{
  host = "{hostname}"
  user = "{user}"
  key = "{ssh_key}"
  workdir = "/home/{user}/{app_name}"
  env = [
    "SPARK_MAJOR_VERSION=3"
  ]
  beforeScript = [
    "echo $SPARK_MAJOR_VERSION"
  ]
  afterScript = [
    "echo $SPARK_MAJOR_VERSION"
    "echo $APPID"
    "rm -r -f /home/{user}/{app_name}/logs.txt"
    "yarn logs -applicationId $APPID -out /home/{user}/{app_name}/logs.txt"
  ]
}}

application {{
  class = "ru.beeline.dmp.<package>.<name>.Main"
  name = "SPARK:APP:NAME, simple {app_name}"
  args = [
    "--event-date"
    "{today}T00:00:00+0300"
  ]
}}

spark {{
  master = "yarn"
  deployMode = "cluster"
  driverCores = 2
  driverMemory = "512m"
  executorMemory = "512m"
  numExecutors = 2
  executorCores = 2
  verbose = false
  principal = "dmvivakin@BAA.RUCOM.RU"
  keytab = "dmvivakin.keytab"
  
  configs = [
    "spark.yarn.report.interval=3000"
    "spark.executor.extraJavaOptions=\\\"-Dconfig.file=./application.conf -Dfile.encoding=utf-8\\\""
    "spark.driver.extraJavaOptions=\\\"-Dconfig.file=./application.conf -Dfile.encoding=utf-8\\\""
  ]
  
  # Use 'upload://' and 'server://' prefix to specify file location 
  files = [{files}]
  jars = [{jars}]
  packages = []
  repositories = []
  py-files = [{py_files}]
}}

deploy {{
  hdfs: hdfs://ns-etl/tmp/share/smsr
}}
"""

    with open(os.path.join(current_path, ".run", "spark-submit.conf"), 'w') as the_file:
        for l in text:
            the_file.writelines(l)

    return text


def main():
    args = parser.parse_args()
    if args.run:
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
        print(f"{colors.fg.lightgreen}run{colors.fg.yellow}\tconf: {args.conf}{colors.endc}")
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
        conf = ConfigFactory.parse_file(args.conf)
        run(conf)
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
    elif args.new:
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
        print(f"{colors.fg.lightgreen}new{colors.fg.yellow}\tproject: {args.project}{colors.endc}")
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
        print(new(args.project))
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
    elif args.show:
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
        print(f"{colors.fg.lightgreen}show{colors.fg.yellow}\tconf: {args.conf}{colors.endc}")
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
        conf = ConfigFactory.parse_file(args.conf)
        print(show(conf))
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
    elif args.deploy:
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
        print(f"{colors.fg.lightgreen}deploy{colors.fg.yellow}\tconf: {args.conf}{colors.endc}")
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")
        conf = ConfigFactory.parse_file(args.conf)
        print(deploy(conf))
        print(f"{colors.fg.green}---------------------------------------{colors.endc}")


if __name__ == '__main__':
    sys.exit(main())
