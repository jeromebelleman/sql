#! /usr/bin/env python
import argparse
import sys, os, os.path
import cmd
import readline
import getpass
import cx_Oracle
import time

RCDIR = '~/.sql'
HISTFILE = RCDIR + '/history'

def duration(d):
    d = int(d)
    seconds = "%02d s" %  (d % 60) if d % 60 != 0 else ''
    minutes = "%02d min " % (d / 60 % 60) if d >= 60 else ''
    hours = " %d h " % (d / 60 / 60 % 60) if d >= 60 * 60 else ''

    return (hours + minutes + seconds).strip()

class Cli(cmd.Cmd):
    def __init__(self, username, password, tns, dryrun):
        cmd.Cmd.__init__(self)
        if os.path.isfile(os.path.expanduser(HISTFILE)):
            readline.read_history_file(os.path.expanduser(HISTFILE))
        self.prompt = '%s@%s%% ' % (username, tns)
        self.dryrun = dryrun

        if not self.dryrun:
            try:
                self.connection = cx_Oracle.connect(username, password, tns)
                self.cursor = self.connection.cursor()
            except cx_Oracle.DatabaseError, e:
                print >>sys.stderr, e,
                sys.exit(1)

    def do_edit(self, line):
        pass

    def do_page(self, line):
        pass

    def default(self, line):
        sql = line if line[-1] != ';' else line[:-1]
        if not self.dryrun:
            try:
                t = time.time()
                self.cursor.execute(sql)
                for row in self.cursor:
                    print row
                d = duration(time.time() - t)
                if d:
                    print d
            except cx_Oracle.DatabaseError, e:
                print >>sys.stderr, e,

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

    if not os.path.isdir(os.path.expanduser(RCDIR)):
        os.mkdir(os.path.expanduser(RCDIR))

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
