# NAME

sql – An Oracle SQL client

# SYNOPSIS

**sql** [**-h**] [**-u** *USER*] [**-c** *CONFIG*] *tns*

# DESCRIPTION

An Oracle SQL client featuring completion, paging and convenience queries.

# CONFIGURATION FILE

You may want to write a configuration file, which is by default expected to
be **~/.sql.cfg**:

```ini
[statements]                                                                    

# Display date/time as UNIX timestamps instead of the default,                  
# human-readable format                                                         
timestamps = false                                                              

# Display column vertically
vertical   = false     
```

# COMMANDS AND STATEMENTS

You can type regular SQL statements straight into the command line
interface. The following commands are also available:

**conf**

:   Open the configuration file in Vim and apply changes immediately
    after it's closed.

**describe**, **desc**

:   Describe table.

**edit**

:   Edit statement in Vim.

**page**

:   Display results in Vim instead of stdout.

**param**

:   Assign value to parameter. E.g.:

    ```sql
    param t = date(1984,4,6)
    SELECT * FROM loc where eventTime = :t
    ```

    More on parameters in the PARAMETERS section.

**params**

:   Display set parameters and their value.  More on parameters in the
    PARAMETERS section.

**plan**

:   Display query execution plan. Note that **COST** doesn't have any
    particular unit and that **CARDINALITY** is the number of rows accessed.
    **TIME** is the estimated time in seconds which will be spent.

**reset**

:   Reset terminal.

**show**

:   Show objects: **usage**, **quotas**, **systables**, **tables**, **indices**

# PARAMETERS

You can specify literal values by assigning parameters, which might be
convenient if you wish to use Python objects, string which are hard to quote
or to keep running the same statement over different values. E.g.:

```sql
param t = datetime(1984,4,6)
select * from foo where timestamp > :t
```
