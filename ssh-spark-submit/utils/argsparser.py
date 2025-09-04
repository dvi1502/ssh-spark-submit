import argparse

parser = argparse.ArgumentParser(
    prog='Run spark app on server',
    description='Process some integers.',
    epilog="And that's how you'd foo a bar."
)

parser.add_argument('-c', '--conf', help='File name ini-file with run options.', type=str, required=False,
                    default="./.run/spark-submit.conf")
# parser.add_argument('-a', '--app', help='run, new, show', type=str, required=False, default="run")
parser.add_argument('-p', '--project', help='Spark-project path', type=str, required=False)

parser.add_argument("-r", "--run", help="run", action="store_true")
parser.add_argument("-n", "--new", help="new", action="store_true")
parser.add_argument("-s", "--show", help="show", action="store_true")
parser.add_argument("-d", "--deploy", help="show", action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()
    print(args)
