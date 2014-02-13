#! /usr/bin/env python
import argparse
import sys, os, os.path
import cmd
import readline
from getpass import getpass
import cx_Oracle
from time import time, sleep, mktime
from subprocess import Popen, PIPE, call
from re import compile, IGNORECASE, DOTALL
from datetime import datetime, date, timedelta
from tempfile import NamedTemporaryFile
import ConfigParser

RCDIR = '~/.sql'
HISTFILE = RCDIR + '/history'
TMPDIR = RCDIR + '/tmp'
REPARAM = compile(':(?P<param>\w+)')
REPLSQL = compile('.*END\s*;\s*$', DOTALL)
WIDTHCOUNT = 10 # Number of rows to guess width from before breaking
WIDTHTIME = .2 # Amount of time to guess width from before breaking
MAXWIDTH = 50
OBJECTS = 'usage', 'quotas', 'systables', 'tables', \
          'indices' # Don't encourage 'indexes'
USEFUL = 'all_objects',  'all_tables',       'all_tab_cols', \
         'all_indexes',  'all_ind_columns', \
         'user_objects', 'user_tables',      'user_tab_cols', \
         'user_indexes', 'user_ind_columns', \
         'user_segments', 'user_extents', 'user_free_space', 'user_ts_quotas', \
         'plan_table'
VIMCMDS = '+set %s titlestring=%s\\ -\\ sql"'

# TODO Display progress?
# TODO Redisplay to handle window resizes

def mkcomplete(cursor, tables):
    tables.clear()
    sql = "SELECT table_name, column_name FROM user_tab_cols"
    for table, column in cursor.execute(sql):
        if table.lower() in tables:
            tables[table.lower()].append(column.lower())
        else:
            tables[table.lower()] = [column.lower()]

    # Add useful tables to e.g. completion (let's not do columns, though)
    for t in USEFUL:
        tables[t] = {}

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

def table(cursor, f, maxw, config):

    # Some statements have no result (e.g. EXPLAIN)
    if not cursor.description:
        return 0

    # Gather data for a bit and guess likely columns widths
    sample = []
    lens = [0] * len(cursor.description)
    t = time()
    for j, row in enumerate(cursor):
        # Don't ever break if the outfile isn't stdout: means we're paging and
        # we'll use all the data to define column widths
        if f == sys.stdout and time() - t > WIDTHTIME and j > WIDTHCOUNT:
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
                if config.getboolean('statements', 'timestamps'):
                    print >>f, fmt.format(*[str(mktime(r.timetuple()))[:maxw]
                                            if isinstance(r, datetime)
                                            else str(r)[:maxw]
                                            for r in row])
                else:
                    print >>f, fmt.format(*[str(r)[:maxw] for r in row])
            else:
                if config.getboolean('statements', 'timestamps'):
                    print >>f, fmt.format(*[str(mktime(r.timetuple()))
                                            if isinstance(r, datetime)
                                            else str(r)
                                            for r in row])
                else:
                    print >>f, fmt.format(*[str(r)[:maxw] for r in row])
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

def edit(line, cursor, title, tables, params, help):
    # Open command line in editor
    f = NamedTemporaryFile(dir=os.path.expanduser(TMPDIR))
    print >>f, line
    vim(f, title, 'wrap')

    # Read edited command line
    g = open(f.name)
    line = g.read().strip() # Can't cope with any trailing newline
    readline.add_history(line)

    # Run command
    cmd, subline = line.split(None, 1)
    if cmd in ('describe', 'desc'):
        describe(subline, cursor, sys.stdout, title, tables)
    elif cmd == 'plan':
        plan(subline, cursor, sys.stdout, title, tables)
    elif cmd == 'show':
        show(subline, cursor, sys.stdout, title, tables, help)
    elif cmd == 'page':
        page(subline, cursor, title, tables, params, help)
    else:
        execute(line, cursor, params, sys.stdout, title, tables)

def show(obj, cursor, f, title, tables, help):
    obj = obj.rstrip(';').lower()

    # To make sure I don't forget adding any new one into OBJECTS
    if obj not in OBJECTS:
        help()
        return

    if obj == 'tables':
        # Don't use the in-memory dictionary because we want to display a
        # few extra columns
        sql = "SELECT table_name, tablespace_name FROM user_tables"
        execute(sql, cursor, {}, f, title, tables)
    elif obj == 'systables':
        # Seems that all_tab_cols has more than all_tables, for some reason
        params = dict(('t%d' % i, t.upper()) for i, t in enumerate(USEFUL))
        select = "SELECT table_name FROM all_tab_cols"
        where = " WHERE table_name IN (%s)" % \
            ', '.join(':%s' % k for k in params.keys())
        group = " GROUP BY table_name ORDER BY table_name"
        sql = select + where + group
        execute(sql, cursor, params, f, title, tables)
    elif obj in ('indices', 'indexes'):
        # We don't complete indices so let's not use the in-memory
        # dictionary and just query directly
        cols = 'user_indexes.table_name', 'index_name', 'uniqueness', \
               'distinct_keys', 'tablespace_name', 'column_name', \
               'column_position'
        select = "SELECT " + ', '.join(cols)
        tab = " FROM user_indexes JOIN user_ind_columns"
        using = " USING (index_name)"
        order = " ORDER BY table_name, column_position"
        sql = select + tab + using + order
        execute(sql, cursor, {}, f, title, tables)
    elif obj == 'usage':
        cols = 'segment_name', \
               'TO_CHAR(SUM(bytes) / 1073741824, 9999.9) "USAGE (GB)"'
        select = "SELECT " + ', '.join(cols)
        tab = " FROM user_segments"
        group = ' GROUP BY segment_name ORDER BY "USAGE (GB)"'
        sql = select + tab + group
        execute(sql, cursor, {}, f, title, tables)
    elif obj == 'quotas':
        cols = 'tablespace_name', \
               'TO_CHAR(bytes / 1048576, 999999.9) "USAGE (MB)"', \
               'TO_CHAR(max_bytes / 1048576, 999999.9) "QUOTA (MB)"'
        select = "SELECT " + ', '.join(cols)
        tab = " FROM user_ts_quotas"
        sql = select + tab
        execute(sql, cursor, {}, f, title, tables)

def plan(line, cursor, f, title, tables):
    execute("EXPLAIN PLAN FOR " + line, cursor, {}, sys.stdout, title, tables)

    cols = 'operation', 'options', 'object_name', 'optimizer', 'cost', \
           'cardinality', 'time'
    select = "SELECT " + ', '.join(cols) + " FROM plan_table"
    where = " WHERE plan_id = (SELECT MAX(plan_id) FROM plan_table)"
    sql = select + where
    execute(sql, cursor, {}, f, title, tables)

def page(line, cursor, title, tables, params, help):
    f = NamedTemporaryFile(dir=os.path.expanduser(TMPDIR))

    cmd, cmdline = line.split(' ', 1)
    if cmd in ('describe', 'desc'):
        describe(cmdline, cursor, f, title, tables)
    elif cmd == 'plan':
        plan(cmdline, cursor, f, title, tables)
    elif cmd == 'show':
        show(cmdline, cursor, f, title, tables, help)
    else:
        # XXX Check snowplough if problem with Unicode
        execute(line, cursor, params, f, title, tables)

def describe(table, cursor, f, title, tables):
    cols = 'column_name', 'nullable', 'data_type', \
           'data_length', 'data_precision', 'data_scale'
    select = "SELECT " + ', '.join(cols) + " FROM all_tab_cols"
    where = " WHERE table_name = :t"
    sql = select + where

    table = table.rstrip(';').upper()
    if table == 'PLAN_TABLE':
        table = table + '$'
    execute(sql, cursor, {'t': table}, f, title, tables)

def execute(line, cursor, params, f, title, tables, config):
    try:
        # Query and parameters
        if REPLSQL.match(line):
            sql = line
        else:
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
            rowc = table(cursor, f, MAXWIDTH, config)
        else:
            rowc = table(cursor, f, None, config)

        # Time query and retrieval
        if f == sys.stdout:
            filehandles = [f]
        else:
            filehandles = [f, sys.stdout]

        if rowc:
            for fh in filehandles:
                print >>fh, "%d row%s" % (rowc, 's' if rowc > 1 else ''),

                d = duration(time() - t)
                if d:
                    print >>fh, "in %s" % d,
                print >>fh

        # Display in pager if not stdout
        if f != sys.stdout:
            vim(f, title, 'nowrap')
            f.close()

        # Check if tables will be shuffled and whether completion needs updating
        if [s for s in ('CREATE', 'ALTER', 'DROP') if s in sql.upper()]:
            mkcomplete(cursor, tables)

    except cx_Oracle.DatabaseError, e:
        print ' ' * (len(prompt(title)) + e.args[0].offset) + '*'
        print str(e)[:-1]

def wintitle(title):
    print "\033]0;%s - sql\007\r" % title,

def prompt(title):
    return title + '% '

class Cli(cmd.Cmd):
    def __init__(self, username, password, tns, configfile):
        cmd.Cmd.__init__(self)

        self.params = {}

        # Read config file
        self.configfile = configfile
        self.do_readconf(None)

        # Set prompt and window title
        self.title = "%s@%s" % (username, tns)
        self.prompt = prompt(self.title)
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
        mkcomplete(self.cursor, self.tables)

    def precmd(self, line):
        readline.write_history_file(os.path.expanduser(HISTFILE))
        return line

    def do_edit(self, line):
        edit(line, self.cursor, self.title, self.tables, self.params,
             self.help_show)

    def help_edit(self):
        print "Edit statement in Vim"

    def do_page(self, line):
        page(line, self.cursor, self.title, self.tables, self.params,
             self.help_show)

    def complete_page(self, text, line, begidx, endidx):
        _, cmd, cmdline = line.split(' ', 2)
        if cmd in ('describe', 'desc'):
            return self.complete_desc(text, cmd + ' ' + cmdline, -1, -1)
        elif cmd == 'show':
            return self.complete_show(text, cmd + ' ' + cmdline, -1, -1)
        else:
            return self.completedefault(text, cmd + ' ' + cmdline, -1, -1)

    def help_page(self):
        print "Display results in Vim instead of stdout"

    def default(self, line):
        execute(line, self.cursor, self.params, sys.stdout, self.title,
                self.tables, self.config)

    def do_params(self, _):
        print self.params

    def help_params(self):
        print "Display set parameters and their value"

    def do_describe(self, line):
        describe(line, self.cursor, sys.stdout, self.title, self.tables)
    do_desc = do_describe

    def complete_describe(self, text, line, begidx, endidx):
        return [t for t in self.tables if t.startswith(text.lower())]
    complete_desc = complete_describe

    def help_describe(self):
        print "Describe table"
    help_desc = help_describe

    def do_plan(self, line):
        plan(line, self.cursor, sys.stdout, self.title, self.tables)

    def help_plan(self):
        print '''\
Display query execution plan. Note that COST doesn't have any particular unit
and that CARDINALITY is the number of rows accessed.  TIME is the estimated
time in seconds which will be spent.'''

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
        '''
        Always return all tables, only return columns whose table is already
        in the line, unless the FROM clause isn't in the line yet in which
        case return all columns
        '''
        columns = set()
        lowerline = line.lower()

        if 'from' in lowerline:
            # Add columns whose table is already in the line
            for t in self.tables:
                if t in lowerline:
                    columns |= set(self.tables[t])
        else:
            # Add all columns
            for t in self.tables:
                columns |= set(self.tables[t])

        return [t for t in self.tables if t.startswith(text.lower())] + \
               [c.lower() for c in columns if c.startswith(text.lower())]

    def do_show(self, line):
        show(line, self.cursor, sys.stdout, self.title, self.tables,
             self.help_show)

    def complete_show(self, text, line, begidx, endidx):
        return [t for t in OBJECTS if t.startswith(text.lower())]

    def do_readconf(self, _):
        self.config = ConfigParser.SafeConfigParser({'timestamps': 'false'})
        self.config.read(os.path.expanduser(self.configfile))

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
    p.add_argument('-c', '--config', help="config file", default='~/.sql.cfg')
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

    # Run CLI
    cli = Cli(username, password, args.tns, args.config)
    while True:
        try:
            cli.cmdloop()
        except KeyboardInterrupt:
            print

if __name__ == '__main__':
    sys.exit(main())
