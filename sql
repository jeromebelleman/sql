#! /usr/bin/env python
import argparse
import sys, os, os.path
import cmd
import readline
from getpass import getpass
import cx_Oracle
from time import time
from subprocess import Popen, PIPE
from re import compile, IGNORECASE

RCDIR = '~/.sql'
HISTFILE = RCDIR + '/history'
# FIXME Doesn't handle joins yet
RE = compile('.*FROM (?P<table>\S+).*', IGNORECASE)
SIZETIME = .2
MAXWIDTH = 20

def duration(d):
    d = int(d)
    seconds = "%02d s" %  (d % 60) if d % 60 != 0 else ''
    minutes = "%02d min " % (d / 60 % 60) if d >= 60 else ''
    hours = " %d h " % (d / 60 / 60 % 60) if d >= 60 * 60 else ''

    return (hours + minutes + seconds).strip()

def termw():
    out, _ = Popen(['stty', 'size'], stdout=PIPE).communicate()
    _, w = out.split()
    return int(w)

def table(cursor):

    # Gather data for a bit and guess likely columns widths
    sample = []
    lens = [0] * len(cursor.description)
    t = time()
    for row in cursor:
        if time() - t > SIZETIME:
            break
        for i, col in enumerate(row):
            lens[i] = len(str(col)) if len(str(col)) > lens[i] else lens[i]
        sample.append(row)

    # The header names lengths have precedence of the data length
    lens = [l if l > len(h) else len(h) \
            for l, (h, _, _, _, _, _, _) in zip(lens, cursor.description)]
    
    # Lay out format
    fmt = ' '.join(('{:%d}' % (l if l < MAXWIDTH else MAXWIDTH) for l in lens))

    # Headings
    print fmt.format(*[name for name, _, _, _, _, _, _ in cursor.description])
    print ' '.join('-' * (l if l < MAXWIDTH else MAXWIDTH) for l in lens)

    # Display data
    rowc = 0
    for row in sample:
        print fmt.format(*[str(r)[:MAXWIDTH] for r in row])
        rowc += 1
    for row in cursor:
        print fmt.format(*[str(r)[:MAXWIDTH] for r in row])
        rowc += 1

    print '\a'

    return rowc

class Cli(cmd.Cmd):
    def __init__(self, username, password, tns):
        cmd.Cmd.__init__(self)

        # Load history file
        if os.path.isfile(os.path.expanduser(HISTFILE)):
            readline.read_history_file(os.path.expanduser(HISTFILE))
        self.prompt = '%s@%s%% ' % (username, tns)

        # Connect
        try:
            self.connection = cx_Oracle.connect(username, password, tns)
            self.cursor = self.connection.cursor()
        except cx_Oracle.DatabaseError, e:
            print >>sys.stderr, e,
            sys.exit(1)

        # Gather table information
        self.tables = {}
        sql = "SELECT table_name, column_name FROM user_tab_cols"
        for table, column in self.cursor.execute(sql):
            if table.lower() in self.tables:
                self.tables[table.lower()].append(column.lower())
            else:
                self.tables[table.lower()] = [column.lower()]

    def do_edit(self, line):
        # TODO
        pass

    def do_page(self, line):
        # TODO
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
            t = time()

            # TODO Interrupt queries. Threads?

            # Query and display results
            self.cursor.execute(sql)
            rowc = table(self.cursor)

            # Time query and retrieval
            print "%d rows" % rowc,
            d = duration(time() - t)
            if d:
                print " in %s" % d,
            print

        except cx_Oracle.DatabaseError, e:
            print >>sys.stderr, e,

    def completedefault(self, text, line, begidx, endidx):
        m = RE.match(line)
        if m: # Only consider columns of specified table
            return [c.lower() for c in self.tables[m.group('table').lower()]
                    if c[:len(text)] == text.lower()]
        else: # Consider all columns of all tables 
            # FIXME List comprehension?
            columns = set()
            for t in self.tables:
                for c in self.tables[t]:
                    columns.add(c)

            return [c.lower() for c in columns if c[:len(text)] == text.lower()]

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
        password = getpass()
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
