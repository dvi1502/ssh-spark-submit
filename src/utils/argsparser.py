import argparse

parser = argparse.ArgumentParser(
    prog='Run spark app on server',
    description='Process some integers.',
    epilog="And that's how you'd foo a bar."
)

parser.add_argument('--conf', '-c', help='File name ini-file with run options.', type=str, required=True)
parser.add_argument('--app', '-a', help='run, new, show', type=str, required=True, default="run")
parser.add_argument('--project', '-p', help='Spark-project path', type=str, required=False)

if __name__ == "__main__":
    args = parser.parse_args()
    print(args)
