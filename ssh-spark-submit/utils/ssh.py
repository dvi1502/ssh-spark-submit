
from sys import stderr
from colors import colors
import paramiko


class SSH:

    def __init__(self, host: str, user: str, key: str):
        self.host = host
        self.user = user
        self.key = key
        self.sshcon = self.connect()

    def connect(self):
        sshcon = paramiko.SSHClient()
        sshcon.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # no known_hosts error
        sshcon.connect(self.host, username=self.user, key_filename=self.key)  # no passwd needed
        return sshcon

    def disconnect(self):
        self.sshcon.close()

    def command(self, command):
        try:
            print(f"-bash$ {colors.fg.lightgreen}{command}{colors.endc}")
            stdin, stdout, stderr = self.sshcon.exec_command(command)
            stdin.close()
            stdout.channel.set_combine_stderr(True)
            output = ''
            for line in stdout.readlines():
                output += line

            return output
        except:
            for line in stderr.readlines():
                print(line)
            return ''

    def command2(self, command):
        import re
        AppId = ""
        print(f"-bash$ {colors.fg.lightgreen}{command}{colors.endc}")
        self.sshcon.load_system_host_keys()
        stdin, stdout, stderr = self.sshcon.exec_command(command, get_pty=True)
        stdout.channel.set_combine_stderr(True)
        for line in iter(stdout.readline, ""):
            if not AppId:
                k1 = re.search(r".*(application_[0-9]+_[0-9]+).*", line)
                if k1:
                    AppId = k1.group(1)
                    print(f"{colors.fg.lightgreen}$APPID={AppId}{colors.endc}")

            print(line, end="")
        return AppId

    def transfer(self, localpath, remotepath):
        try:
            print(f"-move$ {colors.fg.cyan}{localpath}\t->\t{remotepath}{colors.endc}")
            sftp = self.sshcon.open_sftp()
            sftp.put(localpath, remotepath)
            # time.sleep(1)
            sftp.close()
            # print(f"   >>> move from {localpath} to {remotepath} ")
            # print("--------------------")
        except:
            for line in stderr.readlines():
                print(line)
            return ''
