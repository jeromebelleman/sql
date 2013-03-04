#! /usr/bin/env python
import argparse
import sys, os.path
import cmd
import readline
import getpass
import cx_Oracle
import time

HISTFILE = '~/.sql/history'

def duration(d):
    if d > 60:
        seconds = d % 60
    if d > 60 * 60:
        minutes = d % 60 * 60

class Cli(cmd.Cmd):
    def __init__(self, username, password, tns, dryrun):
        cmd.Cmd.__init__(self)
        readline.read_history_file(os.path.expanduser(HISTFILE))
        self.prompt = '%s@%s%% ' % (username, tns)
        self.dryrun = dryrun

        if not self.dryrun:
            try:
                self.connection = cx_Oracle.connect(username, password, tns)
                self.cursor = self.connection.cursor()
            except cx_Oracle.DatabaseError, e:
                print >>sys.stderr, e.message,
                sys.exit(1)

    def do_edit(self, line):
        pass

    def do_page(self, line):
        pass

    def default(self, line):
        t = time.time()
        sql = line if line[-1] != ';' else line[:-1]
        print sql
        if not self.dryrun:
            self.cursor.execute(sql)
            for row in self.cursor:
                print row
        print time.time() - t


    def completedefault(self, text, line, begidx, endidx):
        matches = []
        for e in ('foo', 'bar', 'baz', 'boo'):
            matches.append(text + e)
        return matches

    def do_EOF(self, arg):
        print
        readline.write_history_file(os.path.expanduser(HISTFILE))
        sys.exit(0)

def main():
    p = argparse.ArgumentParser()
    p.add_argument('tns')
    p.add_argument('-u', '--user')
    p.add_argument('-z', '--dryrun', action='store_true')
    args = p.parse_args()

    try:
        if args.user:
            username = args.user
        else:
            username = raw_input('Username: ')
        password = getpass.getpass()
    except (EOFError, KeyboardInterrupt):
        print
        sys.exit(0)

    cli = Cli(username, password, args.tns, args.dryrun)
    while True:
        try:
            cli.cmdloop()
        except KeyboardInterrupt:
            print

if __name__ == '__main__':
    sys.exit(main())
