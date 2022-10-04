import argparse

# TODO: use this to load installation file from testers https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action')

    install_parser = subparsers.add_parser('install-tester', help='a git url of a tester to install')
    install_parser.add_argument('name', help='the name of the tester to install')
    install_parser.add_argument('-u', '--url', help='url to the git repo of the tester to install')
    install_parser.add_argument('-l', '--local', help='a path to a locally installed tester instance')

    remove_parser = subparsers.add_parser('remove-tester', help='the name of the tester to remove')
    remove_parser.add_argument('name', help='the name of the tester to remove')

    update_parser = subparsers.add_parser('update-tester', help='update a tester to a specified version')
    update_parser.add_argument('name', help='the name of the tester to update')
    install_parser.add_argument('-u', '--url', help='url to the git repo of the tester to update')
    install_parser.add_argument('-l', '--local', help='a path to a locally installed tester instance')

    args = parser.parse_args()

    print(args)
