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

from psycopg2.sql import SQL, Identifier, Literal

from lmfdb.backend.encoding import Json
from .base import IdentifierWrapper

def _special_log(value, col, col_type, parser):
    """
    We modify the SQL log function to use Sage semantics instead: log(x, b) rather than log(b, x).
    """
    if isinstance(value, list):
        if len(value) == 1:
            inp, vals = parser.parse(value[0])
            return SQL("log({0})").format(inp), vals
        elif len(value) == 2:
            inp0, vals0 = parser.parse(value[0])
            inp1, vals1 = parser.parse(value[1])
            # Note the changed order: our log is consistent with Python and Sage, NOT with SQL
            return SQL("log({0}, {1})").format(inp1, inp0), vals1 + vals0
    else:
        inp, vals = parser.parse(value)
        if col is None:
            return SQL("log({0})").format(inp), vals
        else:
            return SQL("{0} = log({1})").format(col, inp), vals

def _special_mod(value, col, col_type, parser):
    """
    We modify the SQL mod function to always return a result between 0 and b-1
    (rather then negative values on negative input).
    """
    inp0, vals0 = parser.parse(value[0])
    inp1, vals1 = parser.parse(value[1])
    return SQL("MOD({1} + MOD({0}, {1}), {1})").format(inp0, inp1), vals0 + vals1

def _special_cast(value, col, col_type, parser):
    """
    We whitelist the second value as a type.
    """
    if not isinstance(value[1], basestring):
        raise ValueError("Type must be a string")
    typ = value[1].lower()
    if typ not in types_whitelist:
        if not any(regexp.match(typ.lower()) for regexp in param_types_whitelist):
            raise ValueError("%s is not a valid type" % value[1])
    inp, vals = parser.parse(value[0])
    return SQL("{0}::%s"%typ).format(inp), vals

def _special_in(value, col, col_type, parser):
    if col is None:
        raise ValueError("$in invalid without column specified")
    

def _special_nin(value, col, col_type, parser):
    pass

def _special_contains(value, col, col_type, parser):
    pass

def _special_notcontains(value, col, col_type, parser):
    pass

def _special_exists(value, col, col_type, parser):
    pass

def _special_unnest(value, col, col_type, parser):
    pass

def _special_subselect(value, col, col_type, parser):
    pass

sql_funcs = dict(
    prefix = {
        'not': [1],
        'abs': [1],
        'cbrt': [1], # double only
        'ceil': [1],
        'degrees': [1], # double only
        'exp': [1],
        'floor': [1],
        'ln': [1],
        'pi': [0], # double output
        'power': [2],
        'radians': [1], # double only
        'round': [1,2],
        'sign': [1],
        'sqrt': [1],
        'trunc': [1,2],
        'acos': [1],
        'asin': [1],
        'atan': [1],
        'atan2': [2],
        'cos': [1],
        'cot': [1],
        'sin': [1],
        'tan': [1],

        'char_length': [1],
        'lower': [1],
        'upper': [1],
        'to_char': [2],
        'to_date': [2],
        'to_number': [2],
        'to_timestamp': [2],

        'array_append': [2],
        'array_prepend': [2],
        'array_cat': [2],
        'array_ndims': [1],
        'array_length': [2],
        'cardinality': [1],
        'array_lower': [2],
        'array_upper': [2],
        'array_position': [2,3],
        'array_positions': [2],
        'array_remove': [2],
        'array_replace': [3],
        'array_to_string': [2,3],
        'string_to_array': [2,3],

        'any': [1],
        'all': [1],

        'array_max': [1],
    },
    binary = set([
        '=',
        '<',
        '>',
        '<=',
        '>=',
        '!=',
        '@>', # contains
        '<@', # is contained by
        '&&', # overlap
    ]),
    joiners = set([
        'or',
        'and', # can also use implicit and given by multiple query entries
        '+',
        '-',
        '*',
        '/', # integer division truncates
        '^',
        '&', # binary and
        '|', # binary or
        '#', # binary xor
        '<<', # binary left shift
        '>>', # binary right shift
        '||', # string and array concatenation
    ]),
    aggregators = {
        'array_agg': [1],
        'avg': [1],
        'bit_and': [1],
        'bit_or': [1],
        'bool_and': [1],
        'bool_or': [1],
        'countstar': [0], # {'$countstar': []} = {'$count': [{'$star': []}]}
        'count': [1],
        'jsonb_agg': [1],
        'max': [1],
        'min': [1],
        'sum': [1],
        'string_agg': [2],

        'corr': [2],
        'covar_pop': [2],
        'covar_samp': [2],
        'regr_avgx': [2],
        'regr_avgy': [2],
        'regr_count': [2],
        'regr_intercept': [2],
        'regr_r2': [2],
        'regr_slope': [2],
        'regr_sxx': [2],
        'regr_sxy': [2],
        'regr_syy': [2],
        'stddev_pop': [2],
        'stddev_samp': [2],
        'var_pop': [2],
        'var_samp': [2],
    },
    special = {
        # Special: log, mod, div, ::, unnest
        'log': ([1,2], _special_log),
        'mod': ([2], _special_mod),
        '::': ([2], _special_cast),
        'star': (None, (lambda value, col, col_type, parser: (SQL('*'), []))),

        'unnest': (None, _special_unnest),
        'subselect': (None, _special_subselect),

        'in': (None, _special_in),
        'nin': (None, _special_nin),
        'contains': (None, _special_contains),
        'notcontains': (None, _special_notcontains),
        'exists': (None, _special_exists),
    },
    sugar = { # use the value of col from outside this dictionary
        'lte': '{0} <= %s',
        'lt': '{0} < %s',
        'gte': '{0} >= %s',
        'gt': '{0} > %s',
        'ne': '{0} != %s',
        'maxgte': 'array_max({0}) >= %s',
        'anylte': '%s >= ANY({0})',
        'containedin': '{0} <@ %s',
        'like': '{0} LIKE %s',
        'regex': '{0} ~ %s',
    }
)
sql_funcs['joiners'].update(sql_funcs['binary'])

class Parser(object):
    def __init__(self, table):
        self.table = table

    def parse(self, x, outer=None, outer_coltype=None):
        if isinstance(x, dict):
            return self._parse_dict(x, outer, outer_coltype)
        elif isinstance(x, basestring) and x and x[0] == '@':
            x = self._identifier(x)
            if outer is None:
                return x, []
            else:
                return SQL("{0} = {1}").format(outer, x), []
        else:
            if outer_coltype == 'jsonb':
                x = Json(x)
            if outer is None:
                return SQL("%s"), [x]
            else:
                return SQL("{0} = %s").format(outer), [x]

    def _parse_special(self, key, value, col, coltype):
        """
        Implements more complicated query conditions than just testing for equality:
        inequalities, containment and disjunctions.

        INPUT:
        - ``key`` -- a code starting with $ from the following list:
            - ``$lte`` -- less than or equal to
            - ``$lt`` -- less than
            - ``$gte`` -- greater than or equal to
            - ``$gt`` -- greater than
            - ``$ne`` -- not equal to
            - ``$in`` -- the column must be one of the given set of values
            - ``$nin`` -- the column must not be any of the given set of values
            - ``$contains`` -- for json columns, the given value should be a subset of the column.
            - ``$notcontains`` -- for json columns, the column must not contain any entry of the given value (which should be iterable)
            - ``$containedin`` -- for json columns, the column should be a subset of the given list
            - ``$exists`` -- if True, require not null; if False, require null.
            - ``$startswith`` -- for text columns, matches strings that start with the given string.
            - ``$like`` -- for text columns, matches strings according to the LIKE operand in SQL.
            - ``$regex`` -- for text columns, matches the given regex expression supported by PostgresSQL
        - ``value`` -- The value to compare to.  The meaning depends on the key.
        - ``col`` -- The name of the column, wrapped in SQL (None if no outer column)
        - ``coltype`` -- the type of tye column, as a string (None if no outer column)

        OUTPUT:

        - A string giving the SQL test corresponding to the requested query, with %s
        - values to fill in for the %s entries (see ``_execute`` for more discussion).

        EXAMPLES::

            sage: from lmfdb import db
            sage: statement, vals = db.nf_fields._parse_special("$lte", 5, "degree")
            ('"degree" <= %s', [5])
            sage: statement, vals = db.nf_fields._parse_special("$or", [{"degree":{"$lte":5}},{"class_number":{"$gte":3}}], None)
            sage: statement.as_string(db.conn), vals
            ('("degree" <= %s OR "class_number" >= %s)', [5, 3])
            sage: statement, vals = db.nf_fields._parse_special("$or", [{"$lte":5}, {"$gte":10}], "degree")
            sage: statement.as_string(db.conn), vals
            ('("degree" <= %s OR "degree" >= %s)', [5, 10])
            sage: statement, vals = db.nf_fields._parse_special("$and", [{"$gte":5}, {"$lte":10}], "degree")
            sage: statement.as_string(db.conn), vals
            ('("degree" >= %s AND "degree" <= %s)', [5, 10])
            sage: statement, vals = db.nf_fields._parse_special("$contains", [2,3,5], "ramps")
            sage: statement.as_string(db.conn), vals
            ('"ramps" @> %s', [[2, 3, 5]])
        """
        if not (isinstance(key, basestring) and key and key[0] == '$'):
            raise ValueError("key must start with $")
        prefix, binary, joiners, aggregators, special = [sql_funcs[c] for c in [
            'prefix', 'binary', 'joiners', 'aggregators', 'special']]
        if key in prefix and len(value) not in prefix[key]:
            raise ValueError("Function %s received %s arguments" % (key, len(value)))
        if key in binary and len(value) != 2:
            raise ValueError("Binary operator %s received %s arguments" % (key, len(value)))
        if key in joiners and len(value) < 2:
            raise ValueError("Joiner %s received %s arguments" % (key, len(value)))

        if key in joiners:
            pairs = [self._parse_expression(clause, outer=col, outer_coltype=coltype) for clause in value]
            # We disallow empty output of any subclause
            if any(pair[0] is None for pair in pairs):
                for clause, pair in zip(value, pairs):
                    if pair[0] is None:
                        raise ValueError("Subclause returned None: %s" % clause)
            strings, values = zip(*pairs)
            # flatten values
            values = [item for sublist in values for item in sublist]
            joiner = joiners[key]
            return SQL("({0})").format(SQL(joiner).join(strings)), values

        # First handle the cases that have unusual values
        if key == '$exists':
            if value:
                cmd = SQL("{0} IS NOT NULL").format(col)
            else:
                cmd = SQL("{0} IS NULL").format(col)
            value = []
        elif key == '$notcontains':
            if coltype == 'jsonb':
                cmd = SQL(" AND ").join(SQL("NOT {0} @> %s").format(col) * len(value))
                value = [Json(v) for v in value]
            else:
                cmd = SQL(" AND ").join(SQL("NOT (%s = ANY({0}))").format(col) * len(value))
        elif key == '$mod':
            if not (isinstance(value, (list, tuple)) and len(value) == 2):
                raise ValueError("Error building modulus operation: %s" % value)
            # have to take modulus twice since MOD(-1,5) = -1 in postgres
            cmd = SQL("MOD(%s + MOD({0}, %s), %s) = %s").format(col)
            value = [value[1], value[1], value[1], value[0] % value[1]]

        else:
            if key == '$lte':
                cmd = SQL("{0} <= %s")
            elif key == '$lt':
                cmd = SQL("{0} < %s")
            elif key == '$gte':
                cmd = SQL("{0} >= %s")
            elif key == '$gt':
                cmd = SQL("{0} > %s")
            elif key == '$ne':
                cmd = SQL("{0} != %s")
            # FIXME, we should do recursion with _parse_special
            elif key == '$maxgte':
                cmd = SQL("array_max({0}) >= %s")
            elif key == '$anylte':
                cmd = SQL("%s >= ANY({0})")
            elif key == '$in':
                if coltype == 'jsonb':
                    #jsonb_path_ops modifiers for the GIN index doesn't support this query
                    cmd = SQL("{0} <@ %s")
                else:
                    cmd = SQL("{0} = ANY(%s)")
            elif key == '$nin':
                if coltype == 'jsonb':
                    #jsonb_path_ops modifiers for the GIN index doesn't support this query
                    cmd = SQL("NOT ({0} <@ %s)")
                else:
                    cmd = SQL("NOT ({0} = ANY(%s)")
            elif key == '$contains':
                cmd = SQL("{0} @> %s")
                if coltype != 'jsonb':
                    value = [value]
            elif key == '$containedin':
                #jsonb_path_ops modifiers for the GIN index doesn't support this query
                cmd = SQL("{0} <@ %s")
            elif key == '$startswith':
                cmd = SQL("{0} LIKE %s")
                value = value.replace('_',r'\_').replace('%',r'\%') + '%'
            elif key == '$like':
                cmd = SQL("{0} LIKE %s")
            elif key == '$regex':
                cmd = SQL("{0} ~ '%s'")
            else:
                raise ValueError("Error building query: {0}".format(key))
            if coltype == 'jsonb':
                value = Json(value)
            cmd = cmd.format(col)
            value = [value]
        return cmd, value

    def _parse_values(self, D):
        """
        Returns the values of dictionary parse accordingly to be used as values in ``_execute``

        INPUT:
        - ``D`` -- a dictionary, or a scalar if outer is set

        OUTPUT:

        - A list of values to fill in for the %s in the string.  See ``_execute`` for more details

        EXAMPLES::

            sage: from lmfdb import db
            sage: sage: db.nf_fields._parse_dict({})
            []
            sage: db.lfunc_lfunctions._parse_values({'bad_lfactors':[1,2]})[1][0]
            '[1, 2]'
            sage: db.char_dir_values._parse_values({'values':[1,2]})
            [1, 2]
        """

        return [Json(val) if self.col_type[key] == 'jsonb' else val for key, val in D.iteritems()]

    def _parse_dict(self, D, outer=None, outer_coltype=None):
        """
        Parses a dictionary that specifies a query in something close to Mongo syntax into an SQL query.

        INPUT:

        - ``D`` -- a dictionary, or a scalar if outer is set
        - ``outer`` -- the column that we are parsing (None if not yet parsing any column).  Used in recursion.  Should be wrapped in SQL.
        - ``outer_coltype`` -- the outer column type

        OUTPUT:

        - An SQL Composable giving the WHERE component of an SQL query (possibly containing %s), or None if D imposes no constraint
        - A list of values to fill in for the %s in the string.  See ``_execute`` for more details.

        EXAMPLES::

            sage: from lmfdb import db
            sage: statement, vals = db.nf_fields._parse_dict({"degree":2, "class_number":6})
            sage: statement.as_string(db.conn), vals
            ('"class_number" = %s AND "degree" = %s', [6, 2])
            sage: statement, vals = db.nf_fields._parse_dict({"degree":{"$gte":4,"$lte":8}, "r2":1})
            sage: statement.as_string(db.conn), vals
            ('"r2" = %s AND "degree" <= %s AND "degree" >= %s', [1, 8, 4])
            sage: statement, vals = db.nf_fields._parse_dict({"degree":2, "$or":[{"class_number":1,"r2":0},{"disc_sign":1,"disc_abs":{"$lte":10000},"class_number":{"$lte":8}}]})
            sage: statement.as_string(db.conn), vals
            ('("class_number" = %s AND "r2" = %s OR "disc_sign" = %s AND "class_number" <= %s AND "disc_abs" <= %s) AND "degree" = %s', [1, 0, 1, 8, 10000, 2])
            sage: db.nf_fields._parse_dict({})
            (None, None)
        """
        if len(D) == 0:
            return None, None
        else:
            strings = []
            values = []
            for key, value in D.iteritems():
                if not key:
                    raise ValueError("Error building query: empty key")
                if key[0] == '$':
                    sub, vals = self._parse_special(key, value, outer, coltype=outer_coltype)
                    if sub is not None:
                        strings.append(sub)
                        values.extend(vals)
                    continue
                key, path, coltype = self._qualify(key)
                key = SQL("{0}{1}").format(IdentifierWrapper(key), SQL("").join(path))
                if '.' in key:
                    path = [int(p) if p.isdigit() else p for p in key.split('.')]
                    key = path[0]
                    if self.col_type.get(key) == 'jsonb':
                        path = [SQL("->{0}").format(Literal(p)) for p in path[1:]]
                    else:
                        path = [SQL("[{0}]").format(Literal(p)) for p in path[1:]]
                else:
                    path = None
                if key != 'id' and key not in self._search_cols:
                    raise ValueError("%s is not a column of %s"%(key, self.search_table))
                # Have to determine whether key is jsonb before wrapping it in Identifier
                coltype = self.col_type[key]
                if path:
                    key = SQL("{0}{1}").format(Identifier(key), SQL("").join(path))
                else:
                    key = Identifier(key)
                if isinstance(value, dict) and all(k.startswith('$') for k in value.iterkeys()):
                    sub, vals = self._parse_dict(value, key, outer_coltype=coltype)
                    if sub is not None:
                        strings.append(sub)
                        values.extend(vals)
                    continue
                if value is None:
                    strings.append(SQL("{0} IS NULL").format(key))
                else:
                    if coltype == 'jsonb':
                        value = Json(value)
                    cmd = "{0} = %s"
                    # For arrays, have to add an explicit typecast
                    if coltype.endswith('[]'):
                        if not path:
                            cmd += '::' + coltype
                        else:
                            cmd += '::' + coltype[:-2]

                    strings.append(SQL(cmd).format(key))
                    values.append(value)
            if strings:
                return SQL(" AND ").join(strings), values
            else:
                return None, None
