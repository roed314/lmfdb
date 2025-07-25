
from mpmath import nstr, inf
from sage.all import floor, log
from lmfdb.logger import make_logger
from flask import render_template, request, url_for, Blueprint, Response

ZetaZeros = Blueprint("zeta zeros", __name__, template_folder="templates")
logger = make_logger(ZetaZeros)

from .platt_zeros import zeros_starting_at_N, zeros_starting_at_t

credit_string = "David Platt"


def learnmore_list():
    return [('Completeness of the data', url_for(".completeness")),
            ('Source of the data', url_for(".source")),
            ('Reliability of the data', url_for(".reliability"))]

def friends_list():
    return [('L-function', url_for("l_functions.l_function_riemann_page"))]

def downloads():
    return [('Bulk download', "https://beta.lmfdb.org/riemann-zeta-zeros/")]

# Return the learnmore list with the matchstring entry removed
def learnmore_list_remove(matchstring):
    return [t for t in learnmore_list() if t[0].find(matchstring) < 0]


@ZetaZeros.route("/")
def zetazeros():
    N = request.args.get("N", None, int)
    t = request.args.get("t", 0, float)
    limit = request.args.get("limit", 100, int)
    if limit > 1000:
        return list_zeros(N=N, t=t, limit=limit)
    else:
        title = r"Zeros of $\zeta(s)$"
        bread = [("Datasets", url_for("datasets")), (r'Zeros of $\zeta(s)$', ' ')]
        return render_template('zeta.html', N=N, t=t, limit=limit, title=title, bread=bread, learnmore=learnmore_list(), friends=friends_list(), downloads=downloads())


@ZetaZeros.route("/Completeness")
def completeness():
    t = 'Completeness of Riemann zeta zeros data'
    bread = [("L-functions", url_for("l_functions.index")),(r"Zeros of $\zeta(s)$", url_for(".zetazeros")),('Completeness', ' ')]
    return render_template("single.html", kid='rcs.cande.zeros.zeta', credit=credit_string, title=t, bread=bread, learnmore=learnmore_list_remove('Completeness'))

@ZetaZeros.route("/Source")
def source():
    t = 'Source of Riemann zeta zeros data'
    bread = [("L-functions", url_for("l_functions.index")),(r"Zeros of $\zeta(s)$", url_for(".zetazeros")),('Source', ' ')]
    return render_template("single.html", kid='rcs.source.zeros.zeta', credit=credit_string, title=t, bread=bread, learnmore=learnmore_list_remove('Source'))

@ZetaZeros.route("/Reliability")
def reliability():
    t = 'Reliability of Riemann zeta zeros data'
    bread = [("L-functions", url_for("l_functions.index")),(r"Zeros of $\zeta(s)$", url_for(".zetazeros")),('Reliability', ' ')]
    return render_template("single.html", kid='rcs.rigor.zeros.zeta', credit=credit_string, title=t, bread=bread, learnmore=learnmore_list_remove('Reliability'))

@ZetaZeros.route("/list")
def list_zeros(N=None,
               t=None,
               limit=None,
               fmt=None,
               download=None):
    if N is None:
        N = request.args.get("N", None, int)
    if t is None:
        t = request.args.get("t", 0, float)
    if limit is None:
        limit = request.args.get("limit", 100, int)
    if fmt is None:
        fmt = request.args.get("format", "plain")
    if download is None:
        download = request.args.get("download", "no")

    if limit < 0:
        limit = 100
    if N is not None:  # None is < 0!! WHAT THE WHAT!
        N = max(N, 0)
    t = max(t, 0)

    if limit > 100000:
        # limit = 100000
        #
        bread = [("L-functions", url_for("l_functions.index")),(r"Zeros of $\zeta(s)$", url_for(".zetazeros"))]
        return render_template('single.html', title="Too many zeros",
                               bread=bread, kid="dq.zeros.zeta.toomany")

    if N is not None:
        zeros = zeros_starting_at_N(N, limit)
    else:
        zeros = zeros_starting_at_t(t, limit)

    if fmt == 'plain':
        response = Response("%d %s\n" % (n, nstr(z,31+floor(log(z,10))+1,strip_zeros=False,min_fixed=-inf,max_fixed=+inf)) for n, z in zeros)
        response.headers['content-type'] = 'text/plain'
        if download == "yes":
            response.headers['content-disposition'] = 'attachment; filename=zetazeros'
    else:
        response = str(list(zeros))

    return response
