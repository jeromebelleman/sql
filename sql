#! /usr/bin/env python
import argparse
import sys, os, os.path
import cmd
import readline
from getpass import getpass
import cx_Oracle
from time import time, sleep
from subprocess import Popen, PIPE, call
from re import compile, IGNORECASE
from datetime import datetime, date, timedelta
from tempfile import NamedTemporaryFile

RCDIR = '~/.sql'
HISTFILE = RCDIR + '/history'
TMPDIR = RCDIR + '/tmp'
# FIXME Doesn't handle joins yet
RETABLE = compile('.*FROM (?P<table>\S+).*', IGNORECASE)
REPARAM = compile(':(?P<param>\w+)')
SIZETIME = .2
MAXWIDTH = 50
OBJECTS = 'tables', 'indices' # Don't encourage indexes

VIMCMDS = '+set %s titlestring=%s\\ -\\ sql"'

# TODO Display progress?

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
        fmt = ' '.join(('{%d:%d}' % (i, (l if l < maxw else maxw)) \
            for i, l in enumerate(lens)))
    else:
        fmt = ' '.join(('{%d:%d}' % (i, l) for i, l in enumerate(lens)))

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
            if maxw:
                print >>f, fmt.format(*[str(r)[:maxw] for r in row])
            else:
                print >>f, fmt.format(*[str(r) for r in row])
            rowc += 1

    # Footers
    print >>f, lines
    print >>f, fields

    print '\a\r',
    sys.stdout.flush()

    return rowc

def vim(f, title, wrap):
    f.flush()
    call(['vim', VIMCMDS % (wrap, title), f.name])
    wintitle(title)

def execute(line, cursor, params, f, title):
    try:
        # Query and parameters
        sql = line.rstrip(';')
        ps = dict((p, params[p]) for p in params if p in REPARAM.findall(sql))

        # Display query and parameters in Vim
        if f != sys.stdout:
            print >>f, sql
            print >>f, ps

        # Query and display results
        t = time()
        cursor.execute(sql, ps)
        if f == sys.stdout:
            rowc = table(cursor, f, MAXWIDTH)
        else:
            rowc = table(cursor, f, None)

        # Time query and retrieval
        if f == sys.stdout:
            filehandles = [f]
        else:
            filehandles = [f, sys.stdout]

        for fh in filehandles:
            print >>fh, "%d row%s" % (rowc, 's' if rowc > 1 else ''),
            d = duration(time() - t)
            if d:
                print >>fh, "in %s" % d,
            print >>fh

        # Display in pager if not stdout
        if f != sys.stdout:
            vim(f, title, 'wrap')
            f.close()

    except cx_Oracle.DatabaseError, e:
        print str(e)[:-1]

def wintitle(title):
    print "\033]0;%s - sql\007\r" % title,

class Cli(cmd.Cmd):
    def __init__(self, username, password, tns):
        cmd.Cmd.__init__(self)

        self.params = {}

        # Set prompt and window title
        self.title = "%s@%s" % (username, tns)
        self.prompt = self.title + '% '
        wintitle(self.title)

        # Load history file
        if os.path.isfile(os.path.expanduser(HISTFILE)):
            readline.read_history_file(os.path.expanduser(HISTFILE))

        # Connect
        try:
            self.connection = cx_Oracle.connect(username, password, tns)
            self.cursor = self.connection.cursor()
        except cx_Oracle.DatabaseError, e:
            print str(e)[:-1]
            sys.exit(1)

        # Gather table information
        self.tables = {}
        sql = "SELECT table_name, column_name FROM user_tab_cols"
        for table, column in self.cursor.execute(sql):
            if table.lower() in self.tables:
                self.tables[table.lower()].append(column.lower())
            else:
                self.tables[table.lower()] = [column.lower()]

    def precmd(self, line):
        readline.write_history_file(os.path.expanduser(HISTFILE))
        return line

    def do_edit(self, line):
        f = NamedTemporaryFile(dir=os.path.expanduser(TMPDIR))
        print >>f, line
        vim(f, self.title, 'nowrap')
        g = open(f.name)
        line = g.read()
        readline.add_history(line)
        execute(line, self.cursor, self.params, sys.stdout, self.title)
        g.close()
        f.close()

    # TODO
    # def do_vim(self, line):
    #     pass

    def do_page(self, line):
        f = NamedTemporaryFile(dir=os.path.expanduser(TMPDIR))
        # XXX Check snowplough if problem with Unicode
        execute(line, self.cursor, self.params, f, self.title)

    def help_page(self):
        print "Display results in Vim instead of stdout"

    def default(self, line):
        execute(line, self.cursor, self.params, sys.stdout, self.title)

    def do_params(self, _):
        print self.params

    def help_params(self):
        print "Display set parameters and their value"

    def do_describe(self, line):
        cols = 'column_name', 'nullable', 'data_type', \
               'data_length', 'data_precision', 'data_scale'
        select = "SELECT " + ', '.join(cols) + " FROM user_tab_cols"
        where = " WHERE table_name = :t"
        sql = select + where

        table = line.rstrip(';').upper()
        execute(sql, self.cursor, {'t': table}, sys.stdout, self.title)
    do_desc = do_describe

    def help_describe(self):
        print "Describe table"
    help_desc = help_describe

    def do_param(self, line):
        try:
            key, val = line.split('=')
            self.params[key.strip()] = eval(val)
        except BaseException, e:
            print e

    def help_param(self):
        print '''\
Assign value to parameter. E.g.:
% param t = date(1984,4,6)
% SELECT * FROM loc where eventTime = :t'''

    def emptyline(self):
        pass

    def completedefault(self, text, line, begidx, endidx):
        m = RETABLE.match(line.rstrip(';'))
        if m: # Only consider columns of specified table
            return [c for c in self.tables[m.group('table').lower()]
                    if c.startswith(text.lower())]
        else: # Consider all columns of all tables 
            # FIXME List comprehension?
            columns = set()
            for t in self.tables:
                for c in self.tables[t]:
                    columns.add(c)

            return [c.lower() for c in columns if c.startswith(text.lower())]

    def do_show(self, line):
        obj = line.rstrip(';').lower()
        if obj == 'tables':
            sql = "SELECT table_name, tablespace_name FROM user_tables"
        elif obj in ('indices', 'indexes'):
            cols = 'index_name', 'tablespace_name', 'table_name'
            sql = "SELECT " + ', '.join(cols) + " FROM user_indexes"
        elif not obj:
            self.help_show()
            return
        else:
            article = 'an' if obj[0] in 'aeiou' else 'a'
            print "Dunno what %s %s is" % (article, obj)
            return

        execute(sql, self.cursor, {}, sys.stdout, self.title)

    def complete_show(self, text, line, begidx, endidx):
        return [t for t in OBJECTS if t.startswith(text.lower())]

    def help_show(self):
        print "Show objects: %s" % ', '.join(OBJECTS)

    def do_EOF(self, _):
        print
        sys.exit(0)

    def help_EOF(self):
        print "Exit. You could also use Ctrl-D."

    def help_help(self):
        do_help()

def main():
    p = argparse.ArgumentParser()
    p.add_argument('tns')
    p.add_argument('-u', '--user')
    args = p.parse_args()

    if not os.path.isdir(os.path.expanduser(RCDIR)):
        os.mkdir(os.path.expanduser(RCDIR))
    if not os.path.isdir(os.path.expanduser(TMPDIR)):
        os.mkdir(os.path.expanduser(TMPDIR))

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
