#! /usr/bin/env python
import argparse
import sys, os, os.path
import cmd
import readline
from getpass import getpass
import cx_Oracle
from time import time
from subprocess import Popen, PIPE, call
from re import compile, IGNORECASE
from datetime import datetime, date, timedelta
from tempfile import NamedTemporaryFile

RCDIR = '~/.sql'
HISTFILE = RCDIR + '/history'
# FIXME Doesn't handle joins yet
RETABLE = compile('.*FROM (?P<table>\S+).*', IGNORECASE)
REPARAM = compile(':(?P<param>\w+)')
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

def table(cursor, f, maxw):

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
    if maxw:
        fmt = ' '.join(('{:%d}' % (l if l < maxw else maxw) for l in lens))
    else:
        fmt = ' '.join(('{:%d}' % l for l in lens))

    # Header and footer
    fields = fmt.format(*[n for n, _, _, _, _, _, _ in cursor.description])
    if maxw:
        lines = ' '.join('-' * (l if l < maxw else maxw) for l in lens)
    else:
        lines = ' '.join('-' * l for l in lens)
    print >>f, fields
    print >>f, lines

    # Display data
    rowc = 0
    for result in sample, cursor:
        for row in result:
            # FIXME MAXWIDTH isn't the limit here, it's should be more dynamic
            if maxw:
                print >>f, fmt.format(*[str(r)[:MAXWIDTH] for r in row])
            else:
                print >>f, fmt.format(*[str(r) for r in row])
            rowc += 1

    # Footers
    print >>f, lines
    print >>f, fields

    print '\a\r',

    return rowc

def execute(line, cursor, params, f):
    sql = line if line[-1] != ';' else line[:-1]
    try:
        t = time()

        # Query and display results
        ps = dict((p, params[p]) for p in params if p in REPARAM.findall(sql))
        cursor.execute(sql, ps)
        if f == sys.stdout:
            rowc = table(cursor, f, MAXWIDTH)
        else:
            rowc = table(cursor, f, None)

        # Time query and retrieval
        print >>f, "%d row%s" % (rowc, 's' if rowc > 1 else ''),
        d = duration(time() - t)
        if d:
            print >>f, "in %s" % d,
        print >>f

        # Display in pager if not stdout
        if f != sys.stdout:
            f.flush()
            call(['vim', f.name])
            f.close()

    except cx_Oracle.DatabaseError, e:
        print e,

class Cli(cmd.Cmd):
    def __init__(self, username, password, tns):
        cmd.Cmd.__init__(self)

        self.params = {}

        # Load history file
        if os.path.isfile(os.path.expanduser(HISTFILE)):
            readline.read_history_file(os.path.expanduser(HISTFILE))
        self.prompt = '%s@%s%% ' % (username, tns)

        # Connect
        try:
            self.connection = cx_Oracle.connect(username, password, tns)
            self.cursor = self.connection.cursor()
        except cx_Oracle.DatabaseError, e:
            print e,
            sys.exit(1)

        # Gather table information
        self.tables = {}
        sql = "SELECT table_name, column_name FROM user_tab_cols"
        for table, column in self.cursor.execute(sql):
            if table.lower() in self.tables:
                self.tables[table.lower()].append(column.lower())
            else:
                self.tables[table.lower()] = [column.lower()]

    # # TODO
    # def do_edit(self, line):
    #     pass

    def do_page(self, line):
        # FIXME Should be in user directory in /tmp
        f = NamedTemporaryFile(suffix='-sql')
        # XXX Check snowplough if problem with Unicode
        execute(line, self.cursor, self.params, f)

    def default(self, line):
        execute(line, self.cursor, self.params, sys.stdout)

    def do_params(self, _):
        print self.params

    def do_describe(self, line):
        # TODO Use table()?
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

    def do_param(self, line):
        try:
            key, val = line.split('=')
            self.params[key.strip()] = eval(val)
        except BaseException, e:
            print e

    def emptyline(self):
        pass

    def completedefault(self, text, line, begidx, endidx):
        m = RETABLE.match(line)
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
