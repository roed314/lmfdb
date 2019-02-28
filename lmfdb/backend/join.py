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

class JoinedTable(SearchTable):
    """
    A Python class representing the join of multiple PostgresTables (or their extras).

    Note that we require that every table in the list (except the first)
    be connected to at least one previous table by a join condition.

    INPUT:

    - ``tables`` -- a list of PostgresTables whose search tables will be joined
    - ``joins`` -- a list of pairs of strings of the form 'tablename.colname' giving the join conditions
    - ``extras`` -- a list of PostgresTables whose extra tables will be joined in
        addition to their search tables
    - ``shortnames`` -- a list of short names by which to refer to the tables in queries.
        Defaults to the full name.  Omit extras (they can currently only be referred to
        with the _extra suffix appended to the short name of the corresponding search table).
    - ``join_type`` -- 'inner', 'outer', 'left', 'right' (we do not support cross joins)
    - ``distinguished`` -- whether the first table is distinguished: columns with no table
        specified will default to this table, and result keys from this table will not
        include tablename.  Otherwise, all table names must be specified.
    """
    def __init__(self, tables, joins, extras=None, shortnames=None, join_type='inner', distinguished=False):
        if len(tables) < 2:
            raise ValueError("Joins should only be used for multiple tables")
        self.join_type = join_type.upper()
        if self.join_type not in ['INNER', 'OUTER', 'LEFT', 'RIGHT']:
            raise ValueError("Invalid join type %s" % join_type)
        db = tables[0]._db
        ft = tables[0].search_table
        PostgresBase.__init__(ft, db)
        # Handle dt = None
        self.distinguished = distinguished
        self.tables = {T.search_table: T for T in tables}
        self.extras = {E.search_table: E for E in extras} # search_table for use in _qualify
        self.fullnames = [T.search_table for T in tables]
        if shortnames is None:
            if len(tables) != len(set(tables)):
                raise ValueError("Must provide shortnames when doing self join")
            self.shortnames = self.fullnames
            self.full_to_short = {name:name for name in self.fullnames}
        else:
            if len(shortnames) != len(tables):
                raise ValueError("If provided, shortnames must match tables")
            if len(set(shortnames)) != len(shortnames):
                raise ValueError("shortnames must be duplicate free")
            self.shortnames = shortnames
            if len(tables) == len(set(tables)):
                self.full_to_short = dict(zip(self.fullnames, shortnames))
            else:
                # full name not enough to specify which copy of table
                self.full_to_short = None
        if tables[0]._label_col is None:
            self._label_col = None
        else:
            self._label_col = ft + '.' + tables[0]._label_col
        self._search_cols = tuple(T.search_table + '.' + cname for T in tables for cname in T._search_cols)
        for E in extras:
            if E.extra_table is None:
                raise ValueError("%s has no extra table" % E.search_table)
            if E not in tables:
                raise ValueError("extra tables should also be included in tables")
            self.tablenames.append(E.extra_table)
        self._all_cols = self._search_cols + tuple(E.extra_table + '.' + cname for E in extras for cname in E._extra_cols)
        self._user_cols = tuple(self._user_show(col) for col in self._all_cols)

        self.joins = []
        joins_on_table = defaultdict(list)
        for a, b in joins:
            ta, ca = self._qualify(a).split('.', 1)
            tb, cb = self._qualify(b).split('.', 1)
            ia = self.shortnames.index(ta)
            ib = self.shortnames.index(tb)
            if ia < ib:
                joins_on_table[tb].append((ta, cb, ca))
            else:
                joins_on_table[ta].append((tb, ca, cb))
            self.joins.append((ta+"."+ca, tb+"."+cb))
        if len(joins_on_table) != len(tables):
            raise ValueError("Each table must be joined to a previous table")

        clauses = [SQL("{0} {1}").format(Identifier(self.fullnames[0]), Identifier(self.shortnames[0]))]
        for fn, sn in zip(self.fullnames[1:], self.shortnames[1:]):
            tjoins = joins_on_table[sn]
            tjoin = SQL(" AND ").join(SQL("{0}.{1} = {2}.{3}").format(*map(IdentifierWrapper, [sn, cn, otn, ocn])) for otn, cn, ocn in tjoins)
            clauses.append(SQL("{0} {1} ON {2}").format(fn, sn, tjoin))
        self._search_table_clause = SQL("%s JOIN"%self.join_type).join(clauses)
        # The user explicitly asked for extra tables to be included, so we don't worry about separating them out
        self._full_table_clause = self._search_table_clause

    def _user_show(self, col):
        """
        INPUT:

        - ``col`` -- a string, qualified with a table name

        OUTPUT:

        The string with _extra removed, and the name of the first table as well
        if self.distinguished is set.
        """
        col = col.replace("_extra.", ".")
        if self.distinguished:
            col = col.replace(self.tablenames[0] + ".", "")
        return col

    def _qualify(self, col):
        """
        INPUT:

        - ``col`` -- a string, as input by the user (without _extra)

        OUTPUT:

        The column, qualified by the appropriate table name
        """
        if '.' in col:
            tname, cname = col.split('.', 1)
            if tname not in self.shortnames:
                # Could be a full name
                if self.full_to_short and tname in self.full_to_short:
                    tname = self.full_to_short[tname]
                # Could be a qualified column in the first table
                elif self.distinguished:
                    # We check whether this is valid below
                    cname = col
                    tname = self.shortnames[0]
                else:
                    raise ValueError("%s not a valid table name"%tname)
        elif self.distinguished:
            cname = col
            tname = self.shortnames[0]
        else:
            raise ValueError("Table name not specified")
        if '.' in cname:
            i = cname.index('.')
        elif '[' in cname:
            i = cname.index('[')
        else:
            i = len(cname)
        cname, path = cname[:i], cname[i:]
        if cname in self.tables[tname]._search_cols:
            return tname + "." + cname + path
        elif tname in self.extras and cname in self.extras[tname]._extra_cols:
            return tname + "_extra." + cname + path
        else:
            raise ValueError("%s not a column of %s"%(cname, tname))

    def _identifier(self, col):
        """
        INPUT:

        - ``col`` -- a qualified column name

        OUTPUT:

        An SQL Composable object for use in db._execute
        """
        tname, cname = col.split('.', 1)
        return SQL("{0}.{1}").format(Identifier(tname), IdentifierWrapper(cname))

    def _parse_projection(self, projection):
        """
        INPUT:

        - ``projection`` -- either 0, 1, 2, a dictionary or list of column names,
            as for _parse_projection on PostgresTable

        OUTPUT:

        - a tuple of columns to be selected for the user, qualified with their tablename
        """
        cols = []
        if projection == 0:
            if self._label_col is None:
                raise RuntimeError("No label column for %s"%self.tablenames[0])
            return (self._label_col,)
        elif not projection:
            raise ValueError("You must specify at least one key.")
        if projection == 1:
            return self._search_cols
        elif projection == 2:
            return self._all_cols
        elif isinstance(projection, dict):
            projvals = set(bool(val) for val in projection.values())
            if len(projvals) > 1:
                raise ValueError("You cannot both include and exclude.")
            including = projvals.pop()
            for col in self._all_cols:
                user_col = self._user_show(col)
                if (user_col in projection) == including:
                    cols.append(col)
                projection.pop(user_col, None)
            if projection: # there were extra columns requested
                raise ValueError("%s not column of joined table"%(", ".join(projection)))
        else: # iterable or basestring
            if isinstance(projection, basestring):
                projection = [projection]
            for col in projection:
                colname = col.split('[',1)[0]
                array_part = col[len(colname):]
                if colname in self._user_cols:
                    qualified_name = self._qualify(colname)
                    cols.append(qualified_name + array_part)
                else:
                    raise ValueError("%s not column of joined table"%col)
        return tuple(cols)

    def _search_iterator(self, cur, cols, projection):
        """
        Returns an iterator over the results in a cursor.

        INPUT:

        - ``cur`` -- a psycopg2 cursor
        - ``cols`` -- a tuple of column names (qualified with their corresponding table names)
        - ``projection`` -- the projection requested.

        OUTPUT:

        If projection is 0 or a string, an iterator that yields the label/column values of the query results
        """
        for rec in cur:
            if projection == 0 or isinstance(projection, basestring):
                yield rec[0]
            else:
                yield {k:v for k,v in zip(cols, rec)}

    def search(self, query={}, projection=1, limit=None, offset=0, sort=None, silent=False):
        cols = self._parse_projection(projection)
        vars = SQL(", ").join(self._identifier(col) for col in cols)
        assert vars # FIXME, to make pyflakes happy for the time being
