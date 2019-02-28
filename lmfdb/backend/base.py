"""
This module provides an interface to Postgres supporting
the kinds of queries needed by the LMFDB.

EXAMPLES::

    sage: from lmfdb import db
    sage: db
    Interface to Postgres database
    sage: len(db.tablenames)
    53
    sage: db.tablenames[0]
    'artin_field_data'
    sage: db.artin_field_data
    Interface to Postgres table artin_field_data

You can search using the methods ``search``, ``lucky`` and ``lookup``::

    sage: G = db.gps_small.lookup('8.2')
    sage: G['Exponent']
    4

- ``extra_table`` -- a string or None.  If provided, gives the name of a table that is linked to the search table by an ``id`` column and provides more data that cannot be searched on.  The reason to separate the data into two tables is to reduce the size of the search table.  For large tables this speeds up some queries.
- ``count_table`` -- a string or None.  If provided, gives the name of a table that caches counts for searches on the search table.  These counts are relevant when many results are returned, allowing the search pages to report the number of records even when it would take Postgres a long time to compute this count.

"""

import datetime, inspect, logging, os, random, re, shutil, signal, subprocess, tempfile, time, traceback
from collections import defaultdict, Counter

from psycopg2 import connect, DatabaseError, InterfaceError, ProgrammingError
from psycopg2.sql import SQL, Identifier, Placeholder, Literal, Composable
from psycopg2.extras import execute_values
from sage.all import cartesian_product_iterator, binomial

from lmfdb.backend.encoding import setup_connection, Json, copy_dumps, numeric_converter
from lmfdb.utils import KeyedDefaultDict
from lmfdb.logger import make_logger
from lmfdb.typed_data.artin_types import Dokchitser_ArtinRepresentation, Dokchitser_NumberFieldGaloisGroup

# This list is used when creating new tables
types_whitelist = [
    "int2", "smallint", "smallserial", "serial2",
    "int4", "int", "integer", "serial", "serial4",
    "int8", "bigint", "bigserial", "serial8",
    "numeric", "decimal",
    "float4", "real",
    "float8", "double precision",
    "boolean", "bool",
    "text", "char", "character", "character varying", "varchar",
    "json", "jsonb", "xml",
    "date", "interval", "time", "time without time zone", "time with time zone", "timetz",
    "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz",
    "bytea", "bit", "bit varying", "varbit",
    "point", "line", "lseg", "path", "box", "polygon", "circle",
    "tsquery", "tsvector",
    "txid_snapshot", "uuid",
    "cidr", "inet", "macaddr",
    "money", "pg_lsn",
]
# add arrays
types_whitelist += [ elt + '[]' for elt in types_whitelist]
# make it a set
types_whitelist = set(types_whitelist)

param_types_whitelist = [
    r"^(bit( varying)?|varbit)\s*\([1-9][0-9]*\)$",
    r'(text|(char(acter)?|character varying|varchar(\s*\(1-9][0-9]*\))?))(\s+collate "(c|posix|[a-z][a-z]_[a-z][a-z](\.[a-z0-9-]+)?)")?',
    r"^interval(\s+year|month|day|hour|minute|second|year to month|day to hour|day to minute|day to second|hour to minute|hour to second|minute to second)?(\s*\([0-6]\))?$",
    r"^timestamp\s*\([0-6]\)(\s+with(out)? time zone)?$",
    r"^time\s*\(([0-9]|10)\)(\s+with(out)? time zone)?$",
    r"^(numeric|decimal)\s*\([1-9][0-9]*(,\s*(0|[1-9][0-9]*))?\)$",
]
param_types_whitelist = [re.compile(s) for s in param_types_whitelist]

# The following is used in bucketing for statistics
pg_to_py = {}
for typ in ["int2", "smallint", "smallserial", "serial2",
    "int4", "int", "integer", "serial", "serial4",
    "int8", "bigint", "bigserial", "serial8"]:
    pg_to_py[typ] = int
for typ in ["numeric", "decimal"]:
    pg_to_py[typ] = numeric_converter
for typ in ["float4", "real", "float8", "double precision"]:
    pg_to_py[typ] = float
for typ in ["text", "char", "character", "character varying", "varchar"]:
    pg_to_py[typ] = str

# the non-default operator classes, used in creating indexes
_operator_classes = {'brin':   ['inet_minmax_ops'],
                     'btree':  ['bpchar_pattern_ops', 'cidr_ops', 'record_image_ops',
                                'text_pattern_ops', 'varchar_ops', 'varchar_pattern_ops'],
                     'gin':    ['jsonb_path_ops'],
                     'gist':   ['inet_ops'],
                     'hash':   ['bpchar_pattern_ops', 'cidr_ops', 'text_pattern_ops',
                                'varchar_ops', 'varchar_pattern_ops'],
                     'spgist': ['kd_point_ops']}
# Valid storage parameters by type, used in creating indexes
_valid_storage_params = {'brin':   ['pages_per_range', 'autosummarize'],
                         'btree':  ['fillfactor'],
                         'gin':    ['fastupdate', 'gin_pending_list_limit'],
                         'gist':   ['fillfactor', 'buffering'],
                         'hash':   ['fillfactor'],
                         'spgist': ['fillfactor']}

identifier_splitter = re.compile(r"(->|->>|#>|#>>|::)")

def IdentifierAndType(name, col_dict, convert_indexing=True):
    pass

def IdentifierWrapper(name, convert=True):
    """
    Returns a composable representing an SQL identifer.
    This is  wrapper for psycopg2.sql.Identifier that supports ARRAY slicers
    and coverts them (if desired) from the Python format to SQL,
    as SQL starts at 1, and it is inclusive at the end.

    It also supports jsonb path operators such as -> and ->> and #> and #>>

    Example:

        >>> IdentifierWrapper('name')
        Identifier('name')
        >>> print IdentifierWrapper('name[:10]').as_string(db.conn)
        "name"[:10]
        >>> print IdentifierWrapper('name[1:10]').as_string(db.conn)
        "name"[2:10]
        >>> print IdentifierWrapper('name[1:10]', convert = False).as_string(db.conn)
        "name"[1:10]
        >>> print IdentifierWrapper('name[1:10][0:2]').as_string(db.conn)
        "name"[2:10][1:2]
        >>> print IdentifierWrapper('name[1:10][0:1]').as_string(db.conn)
        "name"[2:10][1:1]
        >>> print IdentifierWrapper('name[1:10][0]').as_string(db.conn)
        "name"[2:10][1]
    """
    if '.' in name:
        return SQL(".").join(*map(IdentifierWrapper(name.split('.'))))
    if '[' not in name:
        return Identifier(name)
    i = name.index('[')
    knife = name[i:]
    name = name[:i]
    # convert python slicer to postgres slicer
    # SQL starts at 1, and it is inclusive at the end
    # so we just need to convert a:b:c -> a+1:b:c

    # first we remove spaces
    knife = knife.replace(' ','')

    # assert that the knife is of the shape [*]
    if knife[0] != '[' or knife[-1] != ']':
        raise ValueError("%s is not in the proper format" % knife)
    chunks = knife[1:-1].split('][')
    # Prevent SQL injection
    if not all(x.isdigit() for chunk in chunks for x in chunk.split(':')):
        raise ValueError("% is must be numeric, brackets and colons"%knife)
    if convert:
        for i, s in enumerate(chunks):
            # each cut is of the format a:b:c
            # where a, b, c are either integers or empty strings
            split = s.split(':',1)
            # nothing to adjust
            if split[0] == '':
                continue
            else:
                # we should increment it by 1
                split[0] = str(int(split[0]) + 1)
            chunks[i] = ':'.join(split)
        sql_slicer = '[' + ']['.join(chunks) + ']'
    else:
        sql_slicer = knife

    return SQL('{0}{1}').format(Identifier(name), SQL(sql_slicer))


class QueryLogFilter(object):
    """
    A filter used when logging slow queries.
    """
    def filter(self, record):
        if record.pathname.startswith('db_backend.py'):
            return 1
        else:
            return 0

class EmptyContext(object):
    """
    Used to simplify code in cases where we may or may not want to open an extras file.
    """
    name = None
    def __enter__(self):
        pass
    def __exit__(self, exc_type, exc_value, traceback):
        pass

class DelayCommit(object):
    """
    Used to set default behavior for whether to commit changes to the database connection.

    Entering this context in a with statement will cause `_execute` calls to not commit by
    default.  When the final DelayCommit is exited, the connection will commit.
    """
    def __init__(self, obj, final_commit=True, silence=None):
        self.obj = obj._db
        self.final_commit = final_commit
        self._orig_silenced = obj._db._silenced
        if silence is not None:
            obj._silenced = silence
    def __enter__(self):
        self.obj._nocommit_stack += 1
    def __exit__(self, exc_type, exc_value, traceback):
        self.obj._nocommit_stack -= 1
        self.obj._silenced = self._orig_silenced
        if exc_type is None and self.obj._nocommit_stack == 0 and self.final_commit:
            self.obj.conn.commit()

class PostgresBase(object):
    """
    A base class for various objects that interact with Postgres.

    Any class inheriting from this one must provide a connection
    to the postgres database, as well as a name used when creating a logger.
    """
    def __init__(self, loggername, db):
        # Have to record this object in the db so that we can reset the connection if necessary.
        # This function also sets self.conn
        db.register_object(self)
        self._db = db
        from lmfdb.utils.config import Configuration
        logging_options = Configuration().get_logging()
        self.slow_cutoff = logging_options['slowcutoff']
        handler = logging.FileHandler(logging_options['slowlogfile'])
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        filt = QueryLogFilter()
        handler.setFormatter(formatter)
        handler.addFilter(filt)
        self.logger = make_logger(loggername, hl = False, extraHandlers = [handler])


    def _execute(self, query, values=None, silent=None, values_list=False, template=None, commit=None, slow_note=None, reissued=False):
        """
        Execute an SQL command, properly catching errors and returning the resulting cursor.

        INPUT:

        - ``query`` -- an SQL Composable object, the SQL command to execute.
        - ``values`` -- values to substitute for %s in the query.  Quoting from the documentation
            for psycopg2 (http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries):

            Never, never, NEVER use Python string concatenation (+) or string parameters
            interpolation (%) to pass variables to a SQL query string. Not even at gunpoint.

        - ``silent`` -- boolean (default None).  If True, don't log a warning for a slow query.
            If None, allow DelayCommit contexts to control silencing.
        - ``values_list`` -- boolean (default False).  If True, use the ``execute_values`` method,
            designed for inserting multiple values.
        - ``template`` -- string, for use with ``values_list`` to insert constant values:
            for example ``"(%s, %s, 42)"``. See the documentation of ``execute_values``
            for more details.
        - ``commit`` -- boolean (default None).  Whether to commit changes on success.  The default
            is to commit unless we are currently in a DelayCommit context.
        - ``slow_note`` -- a tuple for generating more useful data for slow query logging.
        - ``reissued`` -- used internally to prevent infinite recursion when attempting to
            reset the connection.

        .. NOTE:

            If the Postgres connection has been closed, the execute statement will fail.
            We try to recover gracefully by attempting to open a new connection
            and issuing the command again.  However, this approach is not prudent if this
            execute statement is one of a chain of statements, which we detect by checking
            whether ``commit == False``.  In this case, we will reset the connection but reraise
            the interface error.

            The upshot is that you should use ``commit=False`` even for the last of a chain of
            execute statements, then explicitly call ``self.conn.commit()`` afterward.

        OUTPUT:

        - a cursor object from which the resulting records can be obtained via iteration.

        This function will also log slow queries.
        """
        if not isinstance(query, Composable):
            raise TypeError("You must use the psycopg2.sql module to execute queries")

        try:
            cur = self.conn.cursor()

            t = time.time()
            if values_list:
                if template is not None:
                    template = template.as_string(self.conn)
                execute_values(cur, query.as_string(self.conn), values, template)
            else:
                try:
                    cur.execute(query, values)
                except ProgrammingError:
                    print query.as_string(self.conn)
                    print values
                    raise
            if silent is False or (silent is None and not self._db._silenced):
                t = time.time() - t
                if t > self.slow_cutoff:
                    query = query.as_string(self.conn)
                    if values_list:
                        query = query.replace('%s','VALUES_LIST')
                    elif values:
                        query = query % (tuple(values))
                    self.logger.info(query + ' ran in \033[91m {0!s}s \033[0m'.format(t))
                    if slow_note is not None:
                        self.logger.info("Replicate with db.{0}.{1}({2})".format(slow_note[0], slow_note[1], ", ".join(str(c) for c in slow_note[2:])))
        except (DatabaseError, InterfaceError):
            if self.conn.closed != 0:
                # If reissued, we need to raise since we're recursing.
                if reissued:
                    raise
                # Attempt to reset the connection
                self._db.reset_connection()
                if commit or (commit is None and self._db._nocommit_stack == 0):
                    return self._execute(query, values=values, silent=silent, values_list=values_list, template=template, slow_note=slow_note, reissued=True)
                else:
                    raise
            else:
                self.conn.rollback()
                raise
        else:
            if commit or (commit is None and self._db._nocommit_stack == 0):
                self.conn.commit()
        return cur

    def _table_exists(self, tablename):
        cur = self._execute(SQL("SELECT to_regclass(%s)"), [tablename], silent=True)
        return cur.fetchone()[0] is not None

    def _constraint_exists(self, tablename, constraintname):
        #print tablename, constraintname
        cur = self._execute(SQL("SELECT 1 from information_schema.table_constraints where table_name=%s and constraint_name=%s"), [tablename, constraintname],  silent=True)
        return cur.fetchone() is not None

    @staticmethod
    def _sort_str(sort_list):
        """
        Constructs a psycopg2.sql.Composable object describing a sort order for Postgres from a list of columns.

        INPUT:

        - ``sort_list`` -- a list, either of strings (which are interpreted as column names in the ascending direction) or of pairs (column name, 1 or -1).

        OUTPUT:

        - a Composable to be used by psycopg2 in the ORDER BY clause.
        """
        L = []
        for col in sort_list:
            if isinstance(col, basestring):
                L.append(Identifier(col))
            elif col[1] == 1:
                L.append(Identifier(col[0]))
            else:
                L.append(SQL("{0} DESC").format(Identifier(col[0])))
        return SQL(", ").join(L)

class SearchTable(PostgresBase):
    """
    Common functionality for PostgresTable and JoinedTable
    """
    # The following are defaults used by PostgresTable that are overridden by JoinedTable
    def _identifier(self, col):
        return IdentifierWrapper(col)

    def _user_show(self, col):
        col = col.replace("_extra.", ".")
        col = col.replace(self.search_table + ".", "")
        return col

    def _qualify(self, col):
        if '.' in col:
            col, dot_extras = col.split('.', 1)
        if '-' in col:
            pass
        if col not in self.col_type:
            raise ValueError("Unknown column")
        coltype = self.col_type[col]
        if col in self._search_cols:
            col = self.search_table + "." + col
        elif col in self._extra_cols:
            col = self.extra_table + "." + col
        else:
            raise ValueError("Unknown column")

    def _build_query(self, query, limit=None, offset=0, sort=None):
        """
        Build an SQL query from a dictionary, including limit, offset and sorting.

        INPUT:

        - ``query`` -- a dictionary query, in the mongo style (but only supporting certain special operators, as in ``_parse_special``)
        - ``limit`` -- a limit on the number of records returned
        - ``offset`` -- an offset on how many records to skip
        - ``sort`` -- a sort order (to be passed into the ``_sort_str`` method, or None.

        OUTPUT:

        - an SQL Composable giving the WHERE, ORDER BY, LIMIT and OFFSET components of an SQL query, possibly including %s
        - a list of values to substitute for the %s entries

        EXAMPLES::

            sage: from lmfdb import db
            sage: statement, vals = db.nf_fields._build_query({"degree":2, "class_number":6})
            sage: statement.as_string(db.conn), vals
            (' WHERE "class_number" = %s AND "degree" = %s ORDER BY "degree", "disc_abs", "disc_sign", "label"', [6, 2])
            sage: statement, vals = db.nf_fields._build_query({"class_number":1}, 20)
            sage: statement.as_string(db.conn), vals
            (' WHERE "class_number" = %s ORDER BY "id" LIMIT %s', [1, 20])
        """
        qstr, values = self._parse_dict(query)
        if qstr is None:
            s = SQL("")
            values = []
        else:
            s = SQL(" WHERE {0}").format(qstr)
        if sort is None:
            has_sort = True
            if self._sort is None:
                if limit is not None and not (limit == 1 and offset == 0):
                    sort = Identifier("id")
                else:
                    has_sort = False
            elif self._primary_sort in query or self._out_of_order:
                # We use the actual sort because the postgres query planner doesn't know that
                # the primary key is connected to the id.
                sort = self._sort
            else:
                sort = Identifier("id")
        else:
            has_sort = bool(sort)
            sort = self._sort_str(sort)
        if has_sort:
            s = SQL("{0} ORDER BY {1}").format(s, sort)
        if limit is not None:
            s = SQL("{0} LIMIT %s").format(s)
            values.append(limit)
            if offset != 0:
                s = SQL("{0} OFFSET %s").format(s)
                values.append(offset)
        return s, values
