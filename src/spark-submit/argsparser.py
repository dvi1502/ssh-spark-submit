import argparse

parser = argparse.ArgumentParser(
    prog='Run spark app on server',
    description='Process some integers.',
    epilog="And that's how you'd foo a bar."
)

parser.add_argument('--conf', '--c', help='File name ini-file with run options.', type=str, required=True)

if __name__ == "__main__":
    args = parser.parse_args()
    print(args)
