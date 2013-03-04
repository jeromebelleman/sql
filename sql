#! /usr/bin/env python
import argparse
import sys, os, os.path
import cmd
import readline
import getpass
import cx_Oracle
import time
import subprocess

RCDIR = '~/.sql'
HISTFILE = RCDIR + '/history'

def duration(d):
    d = int(d)
    seconds = "%02d s" %  (d % 60) if d % 60 != 0 else ''
    minutes = "%02d min " % (d / 60 % 60) if d >= 60 else ''
    hours = " %d h " % (d / 60 / 60 % 60) if d >= 60 * 60 else ''

    return (hours + minutes + seconds).strip()

def termw():
    out, _ = subprocess.Popen(['stty', 'size'],
                              stdout=subprocess.PIPE).communicate()
    _, w = out.split()
    return int(w)

class Cli(cmd.Cmd):
    def __init__(self, username, password, tns):
        cmd.Cmd.__init__(self)
        if os.path.isfile(os.path.expanduser(HISTFILE)):
            readline.read_history_file(os.path.expanduser(HISTFILE))
        self.prompt = '%s@%s%% ' % (username, tns)

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

    def do_describe(self, line):
        cols = "column_name", "nullable", "data_type", "data_precision"
        select = "SELECT " + ', '.join(cols) + " FROM user_tab_cols"
        where = " WHERE table_name = :t"
        print "Name          Null?         Type          "
        print "------------- ------------- --------------"
        self.cursor.execute(select + where, [line.upper()])
        for name, null, type, precision in self.cursor:
            null = 'NOT NULL' if null == 'N' else ''
            precision = '(' + str(precision) + ')' if precision != None else ''
            print "%13s %13s %13s" % (name, null, (type + precision))
    do_desc = do_describe

    def default(self, line):
        sql = line if line[-1] != ';' else line[:-1]
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

    cli = Cli(username, password, args.tns)
    while True:
        try:
            cli.cmdloop()
        except KeyboardInterrupt:
            print

if __name__ == '__main__':
    sys.exit(main())
