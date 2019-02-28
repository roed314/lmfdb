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

import logging, time
from collections import defaultdict

from psycopg2 import DatabaseError
from psycopg2.sql import SQL, Identifier, Literal
from sage.all import cartesian_product_iterator, binomial

from lmfdb.backend.encoding import Json
from lmfdb.utils import KeyedDefaultDict
from .base import PostgresBase, pg_to_py, DelayCommit

class PostgresStatsTable(PostgresBase):
    """
    This object is used for storing statistics and counts for a search table.

    INPUT:

    - ``table`` -- a ``PostgresTable`` object.
    """
    def __init__(self, table, total=None):
        PostgresBase.__init__(self, table.search_table, table._db)
        self.table = table
        self.search_table = st = table.search_table
        self.stats = st + "_stats"
        self.counts = st + "_counts"
        if total is None:
            total = self.quick_count({})
            if total is None:
                total = self._slow_count({}, extra=False)
        self.total = total

    def _has_stats(self, jcols, ccols, cvals, threshold, split_list=False, threshold_inequality=False):
        """
        Checks whether statistics have been recorded for a given set of columns.
        It just checks whether the "total" stat has been computed.

        INPUT:

        - ``jcols`` -- a list of the columns to be accumulated (wrapped in Json).
        - ``ccols`` -- a list of the constraint columns (wrapped in Json).
        - ``cvals`` -- a list of the values required for the constraint columns (wrapped in Json).
        - ``threshold`` -- an integer: if the number of rows with a given tuple of
           values for the accumulated columns is less than this threshold, those
           rows are thrown away.
        - ``split_list`` -- whether entries of lists should be counted once for each entry.
        """
        if split_list:
            values = [jcols, "split_total"]
        else:
            values = [jcols, "total"]
        if ccols is None:
            ccols = "constraint_cols IS NULL"
            cvals = "constraint_values IS NULL"
        else:
            values.extend([ccols, cvals])
            ccols = "constraint_cols = %s"
            cvals = "constraint_values = %s"
        if threshold is None:
            threshold = "threshold IS NULL"
        else:
            values.append(threshold)
            if threshold_inequality:
                threshold = "(threshold IS NULL OR threshold <= %s)"
            else:
                threshold = "threshold = %s"
        selecter = SQL("SELECT 1 FROM {0} WHERE cols = %s AND stat = %s AND {1} AND {2} AND {3}")
        selecter = selecter.format(Identifier(self.stats), SQL(ccols), SQL(cvals), SQL(threshold))
        cur = self._execute(selecter, values)
        return cur.rowcount > 0

    def quick_count(self, query, split_list=False, suffix=''):
        """
        Tries to quickly determine the number of results for a given query
        using the count table.

        INPUT:

        - ``query`` -- a mongo-style dictionary, as in the ``search`` method.

        OUTPUT:

        Either an integer giving the number of results, or None if not cached.
        """
        cols, vals = self._split_dict(query)
        selecter = SQL("SELECT count FROM {0} WHERE cols = %s AND values = %s AND split = %s").format(Identifier(self.counts + suffix))
        cur = self._execute(selecter, [cols, vals, split_list])
        if cur.rowcount:
            return int(cur.fetchone()[0])

    def _slow_count(self, query, split_list=False, record=True, suffix='', extra=True):
        """
        No shortcuts: actually count the rows in the search table.

        INPUT:

        - ``query`` -- a mongo-style dictionary, as in the ``search`` method.
        - ``record`` -- boolean (default False).  Whether to store the result in the count table.

        OUTPUT:

        The number of rows in the search table satisfying the query.
        """
        if split_list:
            raise NotImplementedError
        selecter = SQL("SELECT COUNT(*) FROM {0}").format(Identifier(self.search_table + suffix))
        qstr, values = self.table._parse_dict(query)
        if qstr is not None:
            selecter = SQL("{0} WHERE {1}").format(selecter, qstr)
        cur = self._execute(selecter, values)
        nres = cur.fetchone()[0]
        if record:
            self._record_count(query, nres, suffix, extra)
        return nres

    def _record_count(self, query, count, suffix='', extra=True):
        cols, vals = self._split_dict(query)
        data = [count, cols, vals]
        if self.quick_count(query) is None:
            updater = SQL("INSERT INTO {0} (count, cols, values, extra) VALUES (%s, %s, %s, %s)")
            data.append(extra)
        else:
            updater = SQL("UPDATE {0} SET count = %s WHERE cols = %s AND values = %s")
        try:
            # This will fail if we don't have write permission,
            # for example, if we're running as the lmfdb user
            self._execute(updater.format(Identifier(self.counts + suffix)), data)
        except DatabaseError:
            pass
        # We also store the total count in meta_tables to improve startup speed
        if not query:
            updater = SQL("UPDATE meta_tables SET total = %s WHERE name = %s")
            # This should never be called from the webserver, since we only record
            # counts for {} when data is updated.
            self._execute(updater, [count, self.search_table])

    def count(self, query={}, record=True):
        """
        Count the number of results for a given query.

        INPUT:

        - ``query`` -- a mongo-style dictionary, as in the ``search`` method.
        - ``record`` -- (default True) whether to record the number of results in the counts table.

        OUTPUT:

        The number of records satisfying the query.

        EXAMPLES::

            sage: from lmfdb import db
            sage: nf = db.nf_fields
            sage: nf.stats.count({'degree':int(6),'galt':int(7)})
            244006
        """
        if not query:
            return self.total
        nres = self.quick_count(query)
        if nres is None:
            nres = self._slow_count(query, record=record)
        return int(nres)

    def _quick_max(self, col, ccols, cvals):
        if ccols is None:
            constraint = SQL("constraint_cols IS NULL")
            values = ["max", Json([col])]
        else:
            constraint = SQL("constraint_cols = %s AND constraint_values = %s")
            values = ["max", Json([col]), ccols, cvals]
        selecter = SQL("SELECT value FROM {0} WHERE stat = %s AND cols = %s AND threshold IS NULL AND {1}").format(Identifier(self.stats), constraint)
        cur = self._execute(selecter, values)
        if cur.rowcount:
            return cur.fetchone()[0]

    def _slow_max(self, col, constraint):
        qstr, values = self.table._parse_dict(constraint)
        if qstr is None:
            where = SQL("")
            values = []
        else:
            where = SQL(" WHERE {0}").format(qstr)
        base_selecter = SQL("SELECT {0} FROM {1}{2} ORDER BY {0} DESC ").format(
            Identifier(col), Identifier(self.search_table), where)
        selecter = base_selecter + SQL("LIMIT 1")
        cur = self._execute(selecter, values)
        m = cur.fetchone()[0]
        if m is None:
            # the default order ends with NULLs, so we now have to use NULLS LAST,
            # preventing the use of indexes.
            selecter = base_selecter + SQL("NULLS LAST LIMIT 1")
            cur = self._execute(selecter, values)
            m = cur.fetchone()[0]
        return m

    def _record_max(self, col, ccols, cvals, m):
        try:
            inserter = SQL("INSERT INTO {0} (cols, stat, value, constraint_cols, constraint_values) VALUES (%s, %s, %s, %s, %s)")
            self._execute(inserter.format(Identifier(self.stats)), [Json([col]), "max", m, ccols, cvals])
        except Exception:
            pass

    def max(self, col, constraint={}, record=True):
        """
        The maximum value attained by the given column, which must be in the search table.

        EXAMPLES::

            sage: from lmfdb import db
            sage: db.nf_fields.stats.max('class_number')
            1892503075117056
        """
        if col == "id":
            # We just use the count in this case
            return self.count()
        if col not in self.table._search_cols:
            raise ValueError("%s not a column of %s"%(col, self.search_table))
        ccols, cvals = self._split_dict(constraint)
        m = self._quick_max(col, ccols, cvals)
        if m is None:
            m = self._slow_max(col, constraint)
            self._record_max(col, ccols, cvals, m)
        return m

    def _bucket_iterator(self, buckets, constraint):
        """
        Utility function for adding buckets to a constraint

        INPUT:

        - ``buckets`` -- a dictionary whose keys are columns, and whose values are
            lists of strings giving either single integers or intervals.
        - ``constraint`` -- a dictionary giving additional constraints on other columns.

        OUTPUT:

        Iterates over the cartesian product of the buckets formed, yielding in each case
        a dictionary that can be used as a query.
        """
        expanded_buckets = []
        for col, divisions in buckets.items():
            parse_singleton = pg_to_py[self.table.col_type[col]]
            cur_list = []
            for bucket in divisions:
                if not bucket:
                    continue
                if bucket[-1] == '-':
                    a = parse_singleton(bucket[:-1])
                    cur_list.append({col:{'$gte':a}})
                elif '-' not in bucket[1:]:
                    cur_list.append({col:parse_singleton(bucket)})
                else:
                    if bucket[0] == '-':
                        L = bucket[1:].split('-')
                        L[0] = '-' + L[0]
                    else:
                        L = bucket.split('-')
                    a, b = map(parse_singleton, L)
                    cur_list.append({col:{'$gte':a, '$lte':b}})
            expanded_buckets.append(cur_list)
        for X in cartesian_product_iterator(expanded_buckets):
            if constraint is None:
                bucketed_constraint = {}
            else:
                bucketed_constraint = dict(constraint) # copy
            for D in X:
                bucketed_constraint.update(D)
            yield bucketed_constraint

    def add_bucketed_counts(self, cols, buckets, constraint={}, commit=True):
        """
        A convenience function for adding statistics on a given set of columns,
        where rows are grouped into intervals by a bucketing dictionary.

        See the ``add_stats`` mehtod for the actual statistics computed.

        INPUT:

        - ``cols`` -- the columns to be displayed.  This will usually be a list of strings of length 1 or 2.
        - ``buckets`` -- a dictionary whose keys are columns, and whose values are lists of break points.
            The buckets are the values between these break points.  Repeating break points
            makes one bucket consist of just that point.
        - ``constraint`` -- a dictionary giving additional constraints on other columns.
        """
        # Conceptually, it makes sense to have the bucket keys included in the columns,
        # but they should be removed in order to treat the bucketed_constraint properly
        # as a constraint.
        cols = [col for col in cols if col not in buckets]
        for bucketed_constraint in self._bucket_iterator(buckets, constraint):
            self.add_stats(cols, bucketed_constraint, commit=commit)

    def _split_dict(self, D):
        """
        A utility function for splitting a dictionary into parallel lists of keys and values.
        """
        if D:
            ans = zip(*sorted(D.items()))
        else:
            ans = [], []
        return map(Json, ans)

    def _join_dict(self, ccols, cvals):
        """
        A utility function for joining a list of keys and of values into a dictionary.
        """
        assert len(ccols) == len(cvals)
        return dict(zip(ccols, cvals))

    def add_stats(self, cols, constraint=None, threshold=None, split_list=False, suffix='', commit=True):
        """
        Add statistics on counts, average, min and max values for a given set of columns.

        INPUT:

        - ``cols`` -- a list of columns, usually of length 1 or 2.
        - ``constraint`` -- only rows satisfying this constraint will be considered.
            It should take the form of a dictionary of the form used in search queries.
            Alternatively, you can provide a pair ccols, cvals giving the items in the dictionary.
        - ``threshold`` -- an integer or None.
        - ``split_list`` -- if True, then counts each element of lists separately.  For example,
            if the list [2,4,8] occurred as the value for a certain column,
            the counts for 2, 4 and 8 would each be incremented.  Constraint columns are not split.
            This option is not supported for nontrivial thresholds.

        OUTPUT:

        Counts for each distinct tuple of values will be stored,
        as long as the number of rows sharing that tuple is above
        the given threshold.  If there is only one column and it is numeric,
        average, min, and max will be computed as well.

        Returns a boolean: whether any counts were stored.
        """
        if split_list and threshold is not None:
            raise ValueError("split_list and threshold not simultaneously supported")
        cols = sorted(cols)
        where = [SQL("{0} IS NOT NULL").format(Identifier(col)) for col in cols]
        values, ccols, cvals = [], None, None
        if constraint is None or constraint == (None, None):
            allcols = cols
            constraint = None
        else:
            if isinstance(constraint, tuple):
                # reconstruct constraint from ccols and cvals
                ccols, cvals = constraint
                constraint = self._join_dict(ccols, cvals)
                ccols, cvals = Json(ccols), Json(cvals)
            else:
                ccols, cvals = self._split_dict(constraint)
            # We need to include the constraints in the count table if we're not grouping by that column
            allcols = sorted(list(set(cols + constraint.keys())))
            if any(key.startswith('$') for key in constraint.keys()):
                raise ValueError("Top level special keys not allowed")
            qstr, values = self.table._parse_dict(constraint)
            if qstr is not None:
                where.append(qstr)
        if allcols:
            where = SQL(" WHERE {0}").format(SQL(" AND ").join(where))
        else:
            where = SQL("")
        if self._has_stats(Json(cols), ccols, cvals, threshold, split_list):
            self.logger.info("Statistics already exist")
            return
        msg = "Adding stats to " + self.search_table
        if cols:
            msg += "for " + ", ".join(cols)
        if constraint:
            from lmfdb.utils import range_formatter
            msg += ": " + ", ".join("{col} = {disp}".format(col=col, disp=range_formatter(val)) for col, val in constraint.items())
        if threshold:
            msg += " (threshold=%s)" % threshold
        self.logger.info(msg)
        having = SQL("")
        if threshold is not None:
            having = SQL(" HAVING COUNT(*) >= {0}").format(Literal(threshold))
        if cols:
            vars = SQL(", ").join(map(Identifier, cols))
            groupby = SQL(" GROUP BY {0}").format(vars)
            vars = SQL("{0}, COUNT(*)").format(vars)
        else:
            vars = SQL("COUNT(*)")
            groupby = SQL("")
        now = time.time()
        seen_one = False
        with DelayCommit(self, commit, silence=True):
            selecter = SQL("SELECT {vars} FROM {table}{where}{groupby}{having}").format(vars=vars, table=Identifier(self.search_table + suffix), groupby=groupby, where=where, having=having)
            cur = self._execute(selecter, values)
            if split_list:
                to_add = defaultdict(int)
            else:
                to_add = []
            total = 0
            onenumeric = False # whether we're grouping by a single numeric column
            if len(cols) == 1:
                col = cols[0]
                if self.table.col_type.get(col) in ["numeric", "bigint", "integer", "smallint", "double precision"]:
                    onenumeric = True
                    avg = 0
                    mn = None
                    mx = None
            if split_list:
                allcols = tuple(allcols)
            for countvec in cur:
                seen_one = True
                colvals, count = countvec[:-1], countvec[-1]
                if constraint is None:
                    allcolvals = colvals
                else:
                    allcolvals = []
                    i = 0
                    for col in allcols:
                        if col in cols:
                            allcolvals.append(colvals[i])
                            i += 1
                        else:
                            allcolvals.append(constraint[col])
                if split_list:
                    listed = [(x if isinstance(x, list) else list(x)) for x in allcolvals]
                    for vals in cartesian_product_iterator(listed):
                        total += count
                        to_add[(allcols, vals)] += count
                else:
                    to_add.append((Json(allcols), Json(allcolvals), count, False))
                    total += count
                if onenumeric:
                    val = colvals[0]
                    avg += val * count
                    if mn is None or val < mn:
                        mn = val
                    if mx is None or val > mx:
                        mx = val
            if not seen_one:
                self.logger.info("No rows exceeded the threshold; returning after %.3f secs" % (time.time() - now))
                return False
            jcols = Json(cols)
            if split_list:
                stats = [(jcols, "split_total", total, ccols, cvals, threshold)]
            else:
                stats = [(jcols, "total", total, ccols, cvals, threshold)]
            if onenumeric and total != 0:
                avg = float(avg) / total
                stats.append((jcols, "avg", avg, ccols, cvals, threshold))
                stats.append((jcols, "min", mn, ccols, cvals, threshold))
                stats.append((jcols, "max", mx, ccols, cvals, threshold))
            # Note that the cols in the stats table does not add the constraint columns, while in the counts table it does.
            inserter = SQL("INSERT INTO {0} (cols, stat, value, constraint_cols, constraint_values, threshold) VALUES %s")
            self._execute(inserter.format(Identifier(self.stats + suffix)), stats, values_list=True)
            inserter = SQL("INSERT INTO {0} (cols, values, count, split) VALUES %s")
            if split_list:
                to_add = [(Json(c), Json(v), ct, True) for ((c, v), ct) in to_add.items()]
            self._execute(inserter.format(Identifier(self.counts + suffix)), to_add, values_list=True)
        self.logger.info("Added stats in %.3f secs"%(time.time() - now))
        return True

    def _approx_most_common(self, col, n):
        """
        Returns the n most common values for ``col``.  Counts are only approximate,
        but this functions should be quite fast.  Note that the returned list
        may have length less than ``n`` if there are not many common values.

        Returns a list of pairs ``(value, count)`` where ``count`` is
        the number of rows where ``col`` takes on the value ``value``.

        INPUT:

        - ``col`` -- a
        """
        if col not in self.table._search_cols:
            raise ValueError("Column %s not a search column for %s"%(col, self.search_table))
        selecter = SQL("""SELECT v.{0}, (c.reltuples * freq)::int as estimate_ct
FROM pg_stats s
CROSS JOIN LATERAL
   unnest(s.most_common_vals::text::""" + self.table.col_type[col] + """[]
        , s.most_common_freqs) WITH ORDINALITY v ({0}, freq, ord)
CROSS  JOIN (
   SELECT reltuples FROM pg_class
   WHERE oid = regclass 'public.nf_fields') c
WHERE schemaname = 'public' AND tablename = %s AND attname = %s
ORDER BY v.ord LIMIT %s""").format(Identifier(col))
        cur = self._execute(selecter, [self.search_table, col, n])
        return [tuple(x) for x in cur]

    def _common_cols(self, threshold=700):
        common_cols = []
        for col in self.table._search_cols:
            most_common = self._approx_most_common(col, 1)
            if most_common and most_common[0][1] >= threshold:
                common_cols.append(col)
        return common_cols

    def _clear_stats_counts(self, extra=True, cols=None):
        deleter = SQL("DELETE FROM {0}")
        self._execute(deleter.format(Identifier(self.stats)))
        if not extra:
            deleter = SQL("DELETE FROM {0} WHERE extra IS NOT TRUE") # false and null
        self._execute(deleter.format(Identifier(self.counts)))

    def add_stats_auto(self, cols=None, constraints=[None], max_depth=None, threshold=1000):
        with DelayCommit(self, silence=True):
            if cols is None:
                cols = self._common_cols()
            for constraint in constraints:
                ccols, cvals = self._split_dict(constraint)
                level = 0
                curlevel = [([],None)]
                while curlevel:
                    i = 0
                    logging.info("Starting level %s/%s (%s/%s colvecs)"%(level, len(cols), len(curlevel), binomial(len(cols), level)))
                    while i < len(curlevel):
                        colvec, _ = curlevel[i]
                        if self._has_stats(Json(cols), ccols, cvals, threshold=threshold, threshold_inequality=True):
                            i += 1
                            continue
                        added_any = self.add_stats(colvec, constraint=constraint, threshold=threshold)
                        if added_any:
                            i += 1
                        else:
                            curlevel.pop(i)
                    if max_depth is not None and level >= max_depth:
                        break
                    prevlevel = curlevel
                    curlevel = []
                    for colvec, m in prevlevel:
                        if m is None:
                            for j, col in enumerate(cols):
                                if not isinstance(col, list):
                                    col = [col]
                                curlevel.append((col, j))
                        else:
                            for j in range(m+1,len(cols)):
                                col = cols[j]
                                if not isinstance(col, list):
                                    col = [col]
                                curlevel.append((colvec + col, j))
                    level += 1

    def refresh_stats(self, total=True, suffix=''):
        """
        Regenerate stats and counts, using rows with ``stat = "total"`` in the stats
        table to determine which stats to recompute, and the rows with ``extra = True``
        in the counts table which have been added by user searches.

        INPUT:

        - ``total`` -- if False, doesn't update the total count (since we can often
            update the total cheaply)
        """
        with DelayCommit(self, silence=True):
            # Determine the stats and counts currently recorded
            selecter = SQL("SELECT cols, constraint_cols, constraint_values, threshold FROM {0} WHERE stat = %s").format(Identifier(self.stats))
            stat_cmds = list(self._execute(selecter, ["total"]))
            split_cmds = list(self._execute(selecter, ["split_total"]))
            col_value_dict = self.extra_counts(include_counts=False, suffix=suffix)

            # Delete all stats and counts
            deleter = SQL("DELETE FROM {0}")
            self._execute(deleter.format(Identifier(self.stats + suffix)))
            self._execute(deleter.format(Identifier(self.counts + suffix)))

            # Regenerate stats and counts
            for cols, ccols, cvals, threshold in stat_cmds:
                self.add_stats(cols, (ccols, cvals), threshold)
            for cols, ccols, cvals, threshold in split_cmds:
                self.add_stats(cols, (ccols, cvals), threshold, split_list=True)
            self._add_extra_counts(col_value_dict, suffix=suffix)

            if total:
                # Refresh total in meta_tables
                self.total = self._slow_count({}, suffix=suffix, extra=False)

    def _copy_extra_counts_to_tmp(self):
        """
        Generates the extra counts in the ``_tmp`` table using the
        extra counts that currently exist in the main table.
        """
        col_value_dict = self.extra_counts(include_counts=False)
        self._add_extra_counts(col_value_dict, suffix='_tmp')

    def _add_extra_counts(self, col_value_dict, suffix=''):
        """
        Records the counts requested in the col_value_dict.

        INPUT:

        - ``col_value_dict`` -- a dictionary giving queries to be counted,
            as output by the ``extra_counts`` function.
        - ``suffix`` -- A suffix (e.g. ``_tmp``) specifying where to
            perform and record the counts
        """
        for cols, values_list in col_value_dict.items():
            for values in values_list:
                query = self._join_dict(cols, values)
                if self.quick_count(query, suffix=suffix) is None:
                    self._slow_count(query, record=True, suffix=suffix)

    def extra_counts(self, include_counts=True, suffix=''):
        """
        Returns a dictionary of the extra counts that have been added by explicit ``count`` calls
        that were not included in counts generated by ``add_stats``.

        The keys are tuples giving the columns being counted, the values are lists of pairs,
        where the first entry is the tuple of values and the second is the count of rows
        with those values.  Note that sometimes the values could be dictionaries
        giving more complicated search queries on the corresponding columns.

        INPUT:

        - ``include_counts`` -- if False, will omit the counts and just give lists of values.
        - ``suffix`` -- Used when dealing with `_tmp` or `_old*` tables.
        """
        selecter = SQL("SELECT cols, values, count FROM {0} WHERE extra ='t'").format(Identifier(self.counts + suffix))
        cur = self._execute(selecter)
        ans = defaultdict(list)
        for cols, values, count in cur:
            if include_counts:
                ans[tuple(cols)].append((tuple(values), count))
            else:
                ans[tuple(cols)].append(tuple(values))

        return ans

    def _get_values_counts(self, cols, constraint, split_list, formatter, query_formatter, base_url, buckets=None):
        """
        Utility function used in ``display_data``.

        Returns a list of pairs (value, count), where value is a list of values taken on by the specified
        columns and count is an integer giving the number of rows with those values.

        If the relevant statistics are not available, it will compute and insert them.

        INPUT:

        - ``cols`` -- a list of column names that are stored in the counts table.
        - ``constraint`` -- a dictionary specifying a constraint on rows to consider.
        """
        selecter_constraints = [SQL("split = %s"), SQL("cols = %s")]
        if constraint:
            allcols = sorted(list(set(cols + constraint.keys())))
            selecter_values = [split_list, Json(allcols)]
            for i, x in enumerate(allcols):
                if x in constraint:
                    selecter_constraints.append(SQL("values->{0} = %s".format(i)))
                    selecter_values.append(Json(constraint[x]))
        else:
            allcols = sorted(cols)
            selecter_values = [split_list, Json(allcols)]
        positions = [allcols.index(x) for x in cols]
        selecter = SQL("SELECT values, count FROM {0} WHERE {1}").format(Identifier(self.counts), SQL(" AND ").join(selecter_constraints))
        headers = [[] for _ in cols]
        default_proportion = '      0.00%' if len(cols) == 1 else ''
        def make_count_dict(values, cnt):
            if isinstance(values, (list, tuple)):
                query = base_url + '&'.join(query_formatter[col](val) for col, val in zip(cols, values))
            else:
                query = base_url + query_formatter[cols[0]](values)
            return {'count': cnt,
                    'query': query,
                    'proportion': default_proportion, # will be overridden for nonzero cnts.
            }
        data = KeyedDefaultDict(lambda key: make_count_dict(key, 0))
        if buckets:
            buckets_seen = set()
            bucket_positions = [i for (i, col) in enumerate(cols) if col in buckets]
        for values, count in self._execute(selecter, values=selecter_values):
            values = [values[i] for i in positions]
            for val, header in zip(values, headers):
                header.append(val)
            D = make_count_dict(values, count)
            if len(cols) == 1:
                values = formatter[cols[0]](values[0])
                if buckets:
                    buckets_seen.add((values,))
            else:
                values = tuple(formatter[col](val) for col, val in zip(cols, values))
                if buckets:
                    buckets_seen.add(tuple(values[i] for i in bucket_positions))
            data[values] = D
        # Ensure that we have all the statistics necessary
        ok = True
        if buckets == {}:
            # Just check that the results are nonempty
            if not data:
                self.add_stats(cols, constraint, split_list=split_list)
                ok = False
        elif buckets:
            # Make sure that every bucket is hit in data
            bcols = [col for col in cols if col in buckets]
            ucols = [col for col in cols if col not in buckets]
            for bucketed_constraint in self._bucket_iterator(buckets, constraint):
                cseen = tuple(formatter[col](bucketed_constraint[col]) for col in bcols)
                if cseen not in buckets_seen:
                    logging.info("Adding statistics for %s with constraints %s" % (", ".join(cols), ", ".join("%s:%s" % (cc, cv) for cc, cv in bucketed_constraint.items())))
                    self.add_stats(ucols, bucketed_constraint)
                    ok = False
        if not ok:
            # Set buckets=False so we have no chance of infinite recursion
            return self._get_values_counts(cols, constraint, split_list, formatter, query_formatter, base_url, buckets=False)
        if len(cols) == 1:
            return headers[0], data
        else:
            return headers, data

    def _get_total_avg(self, cols, constraint, avg, split_list):
        """
        Utility function used in ``display_data``.

        Returns the total number of rows and average value for the column, subject to the given constraint.

        INPUT:

        - ``cols`` -- a list of columns
        - ``constraint`` -- a dictionary specifying a constraint on rows to consider.
        - ``avg`` -- boolean, whether to compute the average.

        OUTPUT:

        - the total number of rows satisying the constraint
        - the average value of the given column (only possible if cols has length 1), or None if the average not requested.
        """
        jcols = Json(cols)
        total_str = "split_total" if split_list else "total"
        totaler = SQL("SELECT value FROM {0} WHERE cols = %s AND stat = %s AND threshold IS NULL").format(Identifier(self.stats))
        if constraint:
            ccols, cvals = self._split_dict(constraint)
            totaler = SQL("{0} AND constraint_cols = %s AND constraint_values = %s").format(totaler)
            totaler_values = [jcols, total_str, ccols, cvals]
        else:
            totaler = SQL("{0} AND constraint_cols IS NULL").format(totaler)
            totaler_values = [jcols, total_str]
        cur_total = self._execute(totaler, values=totaler_values)
        if cur_total.rowcount == 0:
            raise ValueError("Database does not contain stats for %s"%(cols[0],))
        total = cur_total.fetchone()[0]
        if avg:
            # Modify totaler_values in place since query for avg is very similar
            totaler_values[1] = "avg"
            cur_avg = self._execute(totaler, values=totaler_values)
            avg = cur_avg.fetchone()[0]
        else:
            avg = False
        return total, avg

    def create_oldstats(self, filename):
        name = self.search_table + "_oldstats"
        with DelayCommit(self, silence=True):
            creator = SQL('CREATE TABLE {0} (_id text COLLATE "C", data jsonb)').format(Identifier(name))
            self._execute(creator)
            self._db.grant_select(name)
            cur = self.conn.cursor()
            with open(filename) as F:
                try:
                    cur.copy_from(F, self.search_table + "_oldstats")
                except Exception:
                    self.conn.rollback()
                    raise
        print "Oldstats created successfully"

    def get_oldstat(self, name):
        selecter = SQL("SELECT data FROM {0} WHERE _id = %s").format(Identifier(self.search_table + "_oldstats"))
        cur = self._execute(selecter, [name])
        if cur.rowcount != 1:
            raise ValueError("Not a unique oldstat identifier")
        return cur.fetchone()[0]
