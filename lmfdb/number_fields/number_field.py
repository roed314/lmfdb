# -*- coding: utf-8 -*-D

import ast
import os
import re
from six import BytesIO

import time

from flask import render_template, request, url_for, redirect, send_file, make_response, Markup
from sage.all import ZZ, QQ, PolynomialRing, NumberField, latex, primes, RealField

from lmfdb import db
from lmfdb.app import app
from lmfdb.utils import (
    web_latex, to_dict, coeff_to_poly, pol_to_html, comma, format_percentage,
    flash_error, display_knowl, CountBox, prop_int_pretty,
    SearchArray, TextBox, YesNoBox, SubsetNoExcludeBox, TextBoxWithSelect,
    clean_input, nf_string_to_label, parse_galgrp, parse_ints, parse_bool,
    parse_signed_ints, parse_primes, parse_bracketed_posints, parse_nf_string,
    parse_floats, parse_subfield, search_wrap, parse_padicfields, bigint_knowl, 
    raw_typeset, flash_info, input_string_to_poly)
from lmfdb.utils.interesting import interesting_knowls
from lmfdb.galois_groups.transitive_group import (
    cclasses_display_knowl,character_table_display_knowl,
    group_phrase, galois_group_data,
    group_cclasses_knowl_guts, group_pretty_and_nTj,
    group_character_table_knowl_guts, group_alias_table)
from lmfdb.number_fields import nf_page, nf_logger
from lmfdb.number_fields.web_number_field import (
    field_pretty, WebNumberField, nf_knowl_guts, factor_base_factor,
    factor_base_factorization_latex, formatfield)

assert nf_logger

bread_prefix = lambda: [('Number fields', url_for(".number_field_render_webpage"))]

Completename = 'Completeness of the data'
dnc = 'data not computed'

FIELD_LABEL_RE = re.compile(r'^\d+\.\d+\.(\d+(e\d+)?(t\d+(e\d+)?)*)\.\d+$')

nfields = None
max_deg = None
init_nf_flag = False

# For imaginary quadratic field class group data
class_group_data_directory = os.path.expanduser('~/data/class_numbers')


def init_nf_count():
    global nfields, init_nf_flag, max_deg
    if not init_nf_flag:
        nfields = db.nf_fields.count()
        max_deg = db.nf_fields.max('degree')
        init_nf_flag = True


def group_cclasses_data(n, t):
    return Markup(group_cclasses_knowl_guts(n, t))


def group_character_table_data(n, t):
    return Markup(group_character_table_knowl_guts(n, t))


def number_field_data(label):
    return Markup(nf_knowl_guts(label))

def nf_label_pretty(label):
    if len(label) <= 25:
        return label
    s = label.split('.')
    s[2] = s[2][:3] + '...' + s[2][-3:]
    return '.'.join(s)


# fixed precision display of float, rounding off
def fixed_prec(r, digs=3):
    n = RealField(200)(r)*(10**digs)
    n = str(n.round())
    head = int(n[:-digs])
    if head >= 10**4:
        head = comma(head)
    return str(head) + '.' + n[-digs:]


@app.context_processor
def ctx_galois_groups():
    return {'galois_group_data': galois_group_data,
            'group_cclasses_data': group_cclasses_data,
            'group_character_table_data': group_character_table_data}


@app.context_processor
def ctx_number_fields():
    return {'number_field_data': number_field_data,
            'global_numberfield_summary': global_numberfield_summary,
            'ez_raw_typeset': ez_raw_typeset,
            'raw_typeset': raw_typeset}

def ez_raw_typeset(rawpol):
    return raw_typeset(rawpol)

def global_numberfield_summary():
    init_nf_count()
    return r'This database contains %s <a title="number fields" knowl="nf">number fields</a> of <a title="degree" knowl="nf.degree">degree</a> $n\leq %d$.  Here are some <a href="%s">further statistics</a>.  In addition, extensive data on <a href="%s">class groups of quadratic imaginary fields</a> is available for download.' %(comma(nfields),max_deg,url_for('number_fields.statistics'), url_for('number_fields.render_class_group_data'))


def learnmore_list():
    return [('Source and acknowledgments', url_for(".source")),
            (Completename, url_for(".render_discriminants_page")),
            ('Reliability of the data', url_for(".reliability")),
            ('Number field labels', url_for(".render_labels_page")),
            ('Galois group labels', url_for(".render_groups_page")),
            ('Quadratic imaginary class groups', url_for(".render_class_group_data"))]


# Return the learnmore list with the matchstring entry removed
def learnmore_list_remove(matchstring):
    return [t for t in learnmore_list() if t[0].find(matchstring) < 0]


def poly_to_field_label(pol):
    try:
        wnf = WebNumberField.from_polynomial(pol)
        return wnf.get_label()
    except Exception:
        return None


@nf_page.route("/Source")
def source():
    learnmore = learnmore_list_remove('Source')
    t = 'Source and acknowledgments for number field pages'
    bread = bread_prefix() + [('Source', ' ')]
    return render_template("double.html", kid='rcs.source.nf', kid2='rcs.ack.nf',
        title=t, bread=bread, learnmore=learnmore)


@nf_page.route("/Reliability")
def reliability():
    learnmore = learnmore_list_remove('Reliability')
    t = 'Reliability of number field data'
    bread = bread_prefix() + [('Reliability', ' ')]
    return render_template("single.html", kid='rcs.rigor.nf',
        title=t, bread=bread, learnmore=learnmore)


@nf_page.route("/GaloisGroups")
def render_groups_page():
    info = {}
    learnmore = learnmore_list_remove('Galois group')
    t = 'Galois group labels'
    bread = bread_prefix() + [('Galois group labels', ' ')]
    return render_template("galois_groups.html", al=group_alias_table(), info=info, title=t, bread=bread, learnmore=learnmore)


@nf_page.route("/FieldLabels")
def render_labels_page():
    info = {}
    learnmore = learnmore_list_remove('number field labels')
    t = 'Labels for number fields'
    bread = bread_prefix() + [('Labels', '')]
    return render_template("single.html", info=info, kid='nf.label', title=t, bread=bread, learnmore=learnmore)


@nf_page.route("/Completeness")
def render_discriminants_page():
    learnmore = learnmore_list_remove('Completeness')
    t = 'Completeness of number field data'
    bread = [('Number fields', url_for(".number_field_render_webpage")), ('Completeness', ' ')]
    return render_template("single.html", kid='rcs.cande.nf',
        title=t, bread=bread, learnmore=learnmore)


@nf_page.route("/QuadraticImaginaryClassGroups")
def render_class_group_data():
    info = to_dict(request.args)
    #nf_logger.info('******************* ')
    #for k in info.keys():
    # nf_logger.info(str(k) + ' ---> ' + str(info[k]))
    #nf_logger.info('******************* ')
    learnmore = learnmore_list_remove('Quadratic imaginary')
    t = 'Class groups of quadratic imaginary fields'
    bread = bread_prefix() + [(t, ' ')]
    info['message'] = ''
    info['filename'] = 'none'
    if 'Fetch' in info:
        if 'k' in info:
            # remove non-digits
            k = re.sub(r'\D', '', info['k'])
            if not k:
                info['message'] = 'The value of k is either invalid or empty'
                return class_group_request_error(info, bread)
            k = int(k)
            if k>4095:
                info['message'] = 'The value of k is too large'
                return class_group_request_error(info, bread)
        else:
            info['message'] = 'The value of k is missing'
            return class_group_request_error(info, bread)
        info['filenamebase'] = str(info['filenamebase'])
        if info['filenamebase'] in ['cl3mod8', 'cl7mod8', 'cl4mod16', 'cl8mod16']:
            filepath = "%s/%s/%s.%d.gz" % (class_group_data_directory,info['filenamebase'],info['filenamebase'],k)
            if os.path.isfile(filepath) and os.access(filepath, os.R_OK):
                return send_file(filepath, as_attachment=True, add_etags=False)
            else:
                info['message'] = 'File not found'
                return class_group_request_error(info, bread)
        else:
            info['message'] = 'Invalid congruence requested'
            return class_group_request_error(info, bread)

    return render_template("class_group_data.html", info=info, title=t, bread=bread, learnmore=learnmore)


def class_group_request_error(info, bread):
    t = 'Class groups of quadratic imaginary fields'
    return render_template("class_group_data.html", info=info, title=t, bread=bread)


# Helper for stats page
# Input is 3 parallel lists
# li has list of values for a fixed Galois group, each n
# tots is a list of the total number of fields in each degree n
# t is the list of t numbers
def galstatdict(li, tots, t):
    return [{'cnt': comma(li[nn]),
             'prop': format_percentage(li[nn], tots[nn]),
             'query': url_for(".number_field_render_webpage")+'?degree=%d&galois_group=%s'%(nn + 1, "%dt%d" % (nn + 1, t[nn]))} for nn in range(len(li))]


@nf_page.route("/stats")
def statistics():
    fields = db.nf_fields
    nfstatdb = fields.stats
    title = 'Number field statistics'
    bread = bread_prefix() + [('Statistics', '')]
    init_nf_count()
    ntrans = [0, 1, 1, 2, 5, 5, 16, 7, 50, 34, 45, 8, 301, 9, 63, 104, 1954,
              10, 983, 8, 1117, 164, 59, 7, 25000, 211, 96, 2392, 1854, 8, 5712]
    degree_stats = nfstatdb.column_counts('degree')
    n = [degree_stats[elt + 1] for elt in range(23)]

    degree_r2_stats = nfstatdb.column_counts(['degree', 'r2'])
    # if a count is missing it is because it is zero
    nsig = [[degree_r2_stats.get((deg+1, s), 0) for s in range((deg+3)//2)]
            for deg in range(23)]
    # Galois groups
    nt_stats = nfstatdb.column_counts(['degree', 'galois_label'])
    nt_stats = {(key[0],int(key[1].split('T')[1])): value for (key,value) in nt_stats.items()}
    # if a count is missing it is because it is zero
    nt_all = [[nt_stats.get((deg+1, t+1), 0) for t in range(ntrans[deg+1])]
              for deg in range(23)]
    nt = [nt_all[j] for j in range(7)]
    # Galois group families
    cn = galstatdict([u[0] for u in nt_all], n, [1 for u in nt_all])
    sn = galstatdict([u[max(len(u)-1,0)] for u in nt_all], n, [len(u) for u in nt_all])
    an = galstatdict([u[max(len(u)-2,0)] for u in nt_all], n, [len(u)-1 for u in nt_all])
    # t-numbers for D_n
    dn_tlist = [1, 1, 2, 3, 2, 3, 2, 6, 3, 3, 2, 12, 2, 3, 2, 56, 2, 13, 2, 10,
                5, 3, 2]
    dn = galstatdict([nt_stats[(j+1,dn_tlist[j])] for j in range(len(dn_tlist))], n, dn_tlist)

    hdeg_stats = {j: nfstatdb.column_counts('degree', {'class_number': {'$lt': 1+10**j, '$gt': 10**(j-1)}}) for j in range(1, 12)}
    hdeg_stats[0] = nfstatdb.column_counts('degree', {'class_number': 1})
    h = [sum(hdeg_stats[j].get(k+1,0) for k in range(max_deg)) for j in range(12)]
    # if a count is missing it is because it is zero
    hdeg = [[hdeg_stats[j].get(deg+1, 0) for j in range(12)] for deg in range(23)]
    has_hdeg_stats = nfstatdb.column_counts('degree', {'class_number': {'$exists': True}})
    has_hdeg = [has_hdeg_stats[deg+1] for deg in range(23)]
    has_h = sum(has_hdeg[j] for j in range(len(has_hdeg)))
    hdeg = [[{'cnt': comma(hdeg[nn][j]),
              'prop': format_percentage(hdeg[nn][j], has_hdeg[nn]),
              'query': url_for(".number_field_render_webpage")+'?degree=%d&class_number=%s' % (nn + 1, str(1 + 10**(j - 1)) + '-' + str(10**j))}
             for j in range(len(h))]
            for nn in range(len(hdeg))]

    has_hdeg = [{'cnt': comma(has_hdeg[nn]),
                 'prop': format_percentage(has_hdeg[nn], n[nn]),
                 'query': url_for(".number_field_render_webpage")+'?degree=%d&class_number=1-10000000000000' % (nn + 1)} for nn in range(len(has_hdeg))]
    maxt = 1+max([len(entry) for entry in nt])

    nt = [[{'cnt': comma(nt[nn][tt]),
            'prop': format_percentage(nt[nn][tt], n[nn]),
            'query': url_for(".number_field_render_webpage")+'?degree=%d&galois_group=%s' % (nn + 1, "%dt%d" % (nn + 1, tt + 1))}
           for tt in range(len(nt[nn]))]
          for nn in range(len(nt))]
    # Totals for signature table
    sigtotals = [comma(
                 sum([nsig[nn][r2]
                 for nn in range(max(r2*2 - 1, 0), 23)]))
                 for r2 in range(12)]
    nsig = [[{'cnt': comma(nsig[nn][r2]),
             'prop': format_percentage(nsig[nn][r2], n[nn]),
             'query': url_for(".number_field_render_webpage")+'?degree=%d&signature=[%d,%d]'%(nn+1,nn+1-2*r2,r2)} for r2 in range(len(nsig[nn]))] for nn in range(len(nsig))]
    h = [{'cnt': comma(h[j]),
          'prop': format_percentage(h[j], has_h),
          'label': '$10^{' + str(j - 1) + r'}<h\leq 10^{' + str(j) + '}$',
          'query': url_for(".number_field_render_webpage")+'?class_number=%s' % (str(1 + 10**(j - 1)) + '-' + str(10**j))} for j in range(len(h))]
    h[0]['label'] = '$h=1$'
    h[1]['label'] = r'$1<h\leq 10$'
    h[2]['label'] = r'$10<h\leq 10^2$'
    h[0]['query'] = url_for(".number_field_render_webpage")+'?class_number=1'

    # Class number 1 by signature
    sigclass1 = nfstatdb.column_counts(['degree', 'r2'], {'class_number': 1})
    sighasclass = nfstatdb.column_counts(['degree', 'r2'], {'class_number': {'$exists': True}})
    sigclass1 = [[{'cnt': comma(sigclass1.get((nn+1,r2),0)),
                   'prop': format_percentage(sigclass1.get((nn+1,r2),0), sighasclass.get((nn+1,r2),0)) if sighasclass.get((nn+1,r2),0) > 0 else 0,
                   'show': sighasclass.get((nn+1,r2),0) > 0,
                   'query': url_for(".number_field_render_webpage")+'?degree=%d&signature=[%d,%d]&class_number=1' % (nn + 1, nn + 1 - 2*r2, r2)}
                  for r2 in range(len(nsig[nn]))] for nn in range(len(nsig))]

    n = [{'cnt': comma(n[nn]),
          'prop': format_percentage(n[nn], nfields),
          'query': url_for(".number_field_render_webpage")+'?degree=%d' % (nn + 1)}
         for nn in range(len(n))]

    info = {'degree': n,
            'nt': nt,
            'nsig': nsig,
            'sigtotals': sigtotals,
            'h': h,
            'has_h': comma(has_h),
            'has_h_pct': format_percentage(has_h, nfields),
            'hdeg': hdeg,
            'has_hdeg': has_hdeg,
            'sigclass1': sigclass1,
            'total': comma(nfields),
            'maxt': maxt,
            'cn': cn, 'dn': dn, 'an': an, 'sn': sn,
            'maxdeg': max_deg}
    return render_template("nf-statistics.html",
                           info=info,
                           title=title,
                           bread=bread)


@nf_page.route("/")
def number_field_render_webpage():
    info = to_dict(request.args, search_array=NFSearchArray())
    sig_list = sum([[[d - 2 * r2, r2] for r2 in range(
        1 + (d // 2))] for d in range(1, 11)], []) + sum([[[d, 0]] for d in range(11, 21)], [])
    sig_list = [str(s).replace(' ','') for s in sig_list[:16]]
    if not request.args:
        init_nf_count()
        discriminant_list_endpoints = [-10000, -1000, -100, 0, 100, 1000, 10000, 100000, 1000000]
        discriminant_list = ["%s..%s" % (start, end - 1) for start, end in zip(
            discriminant_list_endpoints[:-1], discriminant_list_endpoints[1:])]
        info['degree_list'] = list(range(1, max_deg + 1))
        info['signature_list'] = sig_list
        info['class_number_list'] = list(range(1, 25))
        info['count'] = '50'
        info['nfields'] = comma(nfields)
        info['maxdeg'] = max_deg
        info['discriminant_list'] = discriminant_list
        t = 'Number fields'
        bread = bread_prefix()
        return render_template("nf-index.html", info=info, title=t, bread=bread, learnmore=learnmore_list())
    else:
        return number_field_search(info)


def coeff_to_nf(c):
    return NumberField(coeff_to_poly(c), 'a')


def sig2sign(sig):
    return -1 if sig[1] % 2 else 1


def list2string(li):
    """
    Turn a list into a string (without brackets)
    """
    return ','.join(str(x) for x in li)


def string2list(s):
    s = str(s)
    if not s:
        return []
    return [int(a) for a in s.split(',')]


def render_field_webpage(args):
    data = None
    info = {}
    bread = bread_prefix()

    # This function should not be called unless label is set.
    label = clean_input(args['label'])
    nf = WebNumberField(label)
    data = {}
    if nf.is_null():
        if re.match(r'^\d+\.\d+\.\d+\.\d+$', label):
            flash_error("Number field %s was not found in the database.", label)
        else:
            flash_error("%s is not a valid label for a number field.", label)
        return redirect(url_for(".number_field_render_webpage"))

    info['wnf'] = nf
    data['degree'] = nf.degree()
    data['class_number'] = nf.class_number_latex()
    ram_primes = nf.ramified_primes()
    t = nf.galois_t()
    n = nf.degree()
    data['is_galois'] = nf.is_galois()
    data['autstring'] = r'\Gal' if data['is_galois'] else r'\Aut'
    data['is_abelian'] = nf.is_abelian()
    if nf.is_abelian():
        conductor = nf.conductor()
        data['conductor'] = conductor
        dirichlet_chars = nf.dirichlet_group()
        if dirichlet_chars:
            data['dirichlet_group'] = [r'<a href = "%s">$\chi_{%s}(%s,&middot;)$</a>' % (url_for('characters.render_Dirichletwebpage',modulus=data['conductor'], number=j), data['conductor'], j) for j in dirichlet_chars]
            if len(data['dirichlet_group']) == 1:
                data['dirichlet_group'] = r'<span style="white-space:nowrap">$\lbrace$' + data['dirichlet_group'][0] + r'$\rbrace$</span>'
            else:
                data['dirichlet_group'] = r'$\lbrace$' + ', '.join(data['dirichlet_group'][:-1]) + ', <span style="white-space:nowrap">' + data['dirichlet_group'][-1] + r'$\rbrace$</span>'
        if data['conductor'].is_prime() or data['conductor'] == 1:
            data['conductor'] = r"\(%s\)" % str(data['conductor'])
        else:
            factored_conductor = factor_base_factor(data['conductor'], ram_primes)
            factored_conductor = factor_base_factorization_latex(factored_conductor)
            data['conductor'] = r"\(%s=%s\)" % (str(data['conductor']), factored_conductor)
    data['galois_group'] = group_pretty_and_nTj(n,t,True)
    data['auts'] = db.gps_transitive.lookup(r'{}T{}'.format(n,t))['auts']
    data['cclasses'] = cclasses_display_knowl(n, t)
    data['character_table'] = character_table_display_knowl(n, t)
    data['class_group'] = nf.class_group()
    data['class_group_invs'] = nf.class_group_invariants()
    data['signature'] = nf.signature()
    data['coefficients'] = nf.coeffs()
    nf.make_code_snippets()
    D = nf.disc()
    data['disc_factor'] = nf.disc_factored_latex()
    if D.abs().is_prime() or D == 1:
        data['discriminant'] = bigint_knowl(D,cutoff=60,sides=3)
    else:
        data['discriminant'] = bigint_knowl(D,cutoff=60,sides=3) + r"\(\medspace = %s\)" % data['disc_factor']
    if nf.frobs():
        data['frob_data'], data['seeram'] = see_frobs(nf.frobs())
    else:  # fallback in case we haven't computed them in a case
        data['frob_data'], data['seeram'] = frobs(nf)
    # This could put commas in the rd, we don't want to trigger spaces
    data['rd'] = ('$%s$' % fixed_prec(nf.rd(),2)).replace(',','{,}')
    # Bad prime information
    npr = len(ram_primes)
    ramified_algebras_data = nf.ramified_algebras_data()
    if isinstance(ramified_algebras_data, str):
        loc_alg = ''
    else:
        # [label, latex, e, f, c, gal]
        loc_alg = ''
        for j in range(npr):
            if ramified_algebras_data[j] is None:
                loc_alg += '<tr><td>$%s$</td><td colspan="7">Data not computed</td></tr>'%str(ram_primes[j]).rstrip('L')
            else:
                from lmfdb.local_fields.main import show_slope_content
                primefirstline=True
                mydat = ramified_algebras_data[j]
                p = ram_primes[j]
                loc_alg += '<tr><td rowspan="%d">$%s$</td>'%(len(mydat),str(p))
                for mm in mydat:
                    if primefirstline:
                        primefirstline=False
                    else:
                        loc_alg += '<tr>'
                    if len(mm)==4:
                        loc_alg += '<td></td><td>Deg {}</td><td>{}</td><td>{}</td><td>{}</td><td></td><td></td>'.format(
                            mm[1]*mm[2], mm[1], mm[2], mm[3])
                    else:
                        lab = mm[0]
                        myurl = url_for('local_fields.by_label', label=lab)
                        if mm[3]*mm[2] == 1:
                            lab = r'$\Q_{%s}$'%str(p)
                        loc_alg += '<td><a href="%s">%s</a></td><td>$%s$</td><td>$%d$</td><td>$%d$</td><td>$%d$</td><td>%s</td><td>$%s$</td>'%(myurl,lab,mm[1],mm[2],mm[3],mm[4],mm[5],show_slope_content(mm[8],mm[6],mm[7]))
            loc_alg += '</tr>\n'
        loc_alg += '</tbody></table>\n'

    ram_primes = str(ram_primes)[1:-1]
    # Get rid of python L for big numbers
    ram_primes = ram_primes.replace('L', '')
    if not ram_primes:
        ram_primes = r'\textrm{None}'
    data['phrase'] = group_phrase(n, t)
    zkraw = nf.zk()
    Ra = PolynomialRing(QQ, 'a')
    zk = [latex(Ra(x)) for x in zkraw]
    zk = ['$%s$' % x for x in zk]
    zk = ', '.join(zk)
    zkraw = ', '.join(zkraw)
    grh_label = '<small>(<a title="assuming GRH" knowl="nf.assuming_grh">assuming GRH</a>)</small>' if nf.used_grh() else ''
    # Short version for properties
    grh_lab = nf.short_grh_string()
    if 'computed' in str(data['class_number']):
        grh_lab=''
        grh_label=''
    pretty_label = field_pretty(label)
    if label != pretty_label:
        pretty_label = "%s: %s" % (label, pretty_label)

    info.update(data)
    rootof1raw = unlatex(nf.root_of_1_gen())
    rootofunity = raw_typeset(rootof1raw, nf.root_of_1_gen(),
        extra='&nbsp;(order ${}$)'.format(nf.root_of_1_order()))
    safe_units = nf.units_safe()
    if 'too long' in safe_units:
        myunits = safe_units
    else:
        myunits = raw_typeset(unlatex(safe_units), safe_units)

    info.update({
        'label': pretty_label,
        'label_raw': label,
        'polynomial': raw_typeset(nf.poly()),
        'ram_primes': ram_primes,
        'integral_basis': raw_typeset(zkraw, zk),
        'regulator': web_latex(nf.regulator()),
        'unit_rank': nf.unit_rank(),
        'root_of_unity': rootofunity,
        'fund_units': myunits,
        'cnf': nf.cnf(),
        'grh_label': grh_label,
        'loc_alg': loc_alg
    })

    bread.append(('%s' % nf_label_pretty(info['label_raw']), ' '))
    info['downloads_visible'] = True
    info['downloads'] = [('worksheet', '/')]
    info['friends'] = []
    if nf.can_class_number():
        # hide ones that take a long time to compute on the fly
        # note that the first degree 4 number field missed the zero of the zeta function
        if abs(D**n) < 50000000:
            info['friends'].append(('L-function', "/L/NumberField/%s" % label))
    info['friends'].append(('Galois group', "/GaloisGroup/%dT%d" % (n, t)))
    if 'dirichlet_group' in info:
        info['friends'].append(('Dirichlet character group',
                                url_for("characters.dirichlet_group_table",
                                        modulus=int(conductor),
                                        char_number_list=','.join(
                                            str(a) for a in dirichlet_chars),
                                        poly=nf.poly())))
    resinfo = []
    galois_closure = nf.galois_closure()
    if galois_closure[0]>0:
        if galois_closure[1]:
            resinfo.append(('gc', galois_closure[1]))
            if galois_closure[2]:
                info['friends'].append(('Galois closure',url_for(".by_label", label=galois_closure[2][0])))
        else:
            resinfo.append(('gc', [dnc]))

    sextic_twins = nf.sextic_twin()
    if sextic_twins[0]>0:
        if sextic_twins[1]:
            resinfo.append(('sex', r' $\times$ '.join(sextic_twins[1])))
        else:
            resinfo.append(('sex', dnc))

    siblings = nf.siblings()
    # [degsib list, label list]
    # first is list of [deg, num expected, list of knowls]
    if siblings[0]:
        for sibdeg in siblings[0]:
            if not sibdeg[2]:
                sibdeg[2] = dnc
            else:
                nsibs = len(sibdeg[2])
                sibdeg[2] = ', '.join(sibdeg[2])
                if nsibs<sibdeg[1]:
                    sibdeg[2] += ', some '+dnc

        resinfo.append(('sib', siblings[0]))
        for lab in siblings[1]:
            if lab:
                labparts = lab.split('.')
                info['friends'].append(("Degree %s sibling" % labparts[0],
                                        url_for(".by_label", label=lab)))

    arith_equiv = nf.arith_equiv()
    if arith_equiv[0]>0:
        if arith_equiv[1]:
            resinfo.append(('ae', ', '.join(arith_equiv[1]), len(arith_equiv[1])))
            for aelab in arith_equiv[2]:
                info['friends'].append(('Arithmetically equivalent sibling',url_for(".by_label", label=aelab)))
        else:
            resinfo.append(('ae', dnc, len(arith_equiv[1])))

    info['resinfo'] = resinfo
    learnmore = learnmore_list()
    title = "Number field %s" % info['label']

    if npr == 1:
        primes = 'prime'
    else:
        primes = 'primes'
    if len(ram_primes) > 30:
        ram_primes = 'see page'
    else:
        ram_primes = '$%s$' % ram_primes

    properties = [('Label', nf_label_pretty(label)),
                  ('Degree', prop_int_pretty(data['degree'])),
                  ('Signature', '$%s$' % data['signature']),
                  ('Discriminant', prop_int_pretty(D)),
                  ('Root discriminant', '%s' % data['rd']),
                  ('Ramified ' + primes + '', ram_primes),
                  ('Class number', '%s %s' % (data['class_number'], grh_lab)),
                  ('Class group', '%s %s' % (data['class_group_invs'], grh_lab)),
                  ('Galois group', group_pretty_and_nTj(data['degree'], t))]
    downloads = [('Stored data to gp',
                  url_for('.nf_download', nf=label, download_type='data'))]
    for lang in [["Magma","magma"], ["SageMath","sage"], ["Pari/GP", "gp"]]:
        downloads.append(('Download {} code'.format(lang[0]),
                          url_for(".nf_download", nf=label, download_type=lang[1])))
    from lmfdb.artin_representations.math_classes import NumberFieldGaloisGroup
    from lmfdb.artin_representations.math_classes import artin_label_pretty
    try:
        info["tim_number_field"] = NumberFieldGaloisGroup(nf._data['coeffs'])
        arts = [z.label() for z in info["tim_number_field"].artin_representations()]
        #print arts
        for ar in arts:
            info['friends'].append(('Artin representation '+artin_label_pretty(ar),
                url_for("artin_representations.render_artin_representation_webpage", label=ar)))
        v = nf.factor_perm_repn(info["tim_number_field"])

        def dopow(m):
            if m == 0:
                return ''
            if m == 1:
                return '*'
            return '*<sup>%d</sup>' % m

        info["mydecomp"] = [dopow(x) for x in v]
    except AttributeError:
        pass
    return render_template("nf-show-field.html", properties=properties, title=title, bread=bread, code=nf.code, friends=info.pop('friends'), downloads=downloads, learnmore=learnmore, info=info, formatfield=formatfield, KNOWL_ID="nf.%s"%label)


def format_coeffs2(coeffs):
    return format_coeffs(string2list(coeffs))


def format_coeffs(coeffs):
    return pol_to_html(str(coeff_to_poly(coeffs)))
#    return web_latex(coeff_to_poly(coeffs))

#@nf_page.route("/")
# def number_fields():
#    if len(request.args) != 0:
#        return number_field_search(**request.args)
#    info['learnmore'] = [('Number field labels', url_for(".render_labels_page")), ('Galois group labels',url_for(".render_groups_page")), (Completename,url_for(".render_discriminants_page"))]
#    return render_template("nf-index.html", info = info)


def url_for_label(label):
    return url_for(".by_label", label=label)

@nf_page.route("/<label>")
def by_label(label):
    if label == "random":
        #This version leaves the word 'random' in the URL:
        #return render_field_webpage({'label': label})
        return redirect(url_for_label(db.nf_fields.random()), 301)
    try:
        nflabel = nf_string_to_label(clean_input(label))
        if label != nflabel:
            return redirect(url_for_label(nflabel), 301)
        return render_field_webpage({'label': nflabel})
    except ValueError as err:
        flash_error("%s is not a valid input for a <span style='color:black'>label</span>.  %s", label, str(err))
        return redirect(url_for(".number_field_render_webpage"))


@nf_page.route("/interesting")
def interesting():
    return interesting_knowls(
        "nf",
        db.nf_fields,
        url_for_label,
        title=r"Some interesting number fields",
        bread=bread_prefix() + [("Interesting", " ")],
        learnmore=learnmore_list(),
    )

def download_search(info):
    dltype = info.get('Submit')
    delim = 'bracket'
    com = r'\\'  # single line comment start
    com1 = ''  # multiline comment start
    com2 = ''  # multiline comment end
    filename = 'fields.gp'
    mydate = time.strftime("%d %B %Y")
    if dltype == 'sage':
        com = '#'
        filename = 'fields.sage'
    if dltype == 'mathematica':
        com = ''
        com1 = '(*'
        com2 = '*)'
        delim = 'brace'
        filename = 'fields.ma'
    if dltype == 'magma':
        com = ''
        com1 = '/*'
        com2 = '*/'
        delim = 'magma'
        filename = 'fields.m'
    s = com1 + "\n"
    s += com + ' Number fields downloaded from the LMFDB downloaded %s\n'% mydate
    s += com + ' Below is a list called data. Each entry has the form:\n'
    s += com + '   [label, polynomial, discriminant, t-number, class group]\n'
    s += com + ' Here the t-number is for the Galois group\n'
    s += com + ' If a class group was not computed, the entry is [-1]\n'
    s += '\n' + com2
    s += '\n'
    if dltype == 'magma':
        s += 'data := ['
    else:
        s += 'data = ['
    s += '\\\n'
    Qx = PolynomialRing(QQ,'x')
    # reissue saved query here
    res = db.nf_fields.search(ast.literal_eval(info["query"]))
    for f in res:
        pol = Qx(f['coeffs'])
        D = f['disc_abs'] * f['disc_sign']
        gal_t = int(f['galois_label'].split('T')[1])
        if 'class_group' in f:
            cl = f['class_group']
        else:
            cl = [-1]
        entry = ', '.join(['"'+str(f['label'])+'"', str(pol), str(D), str(gal_t), str(cl)])
        s += '[' + entry + ']' + ',\\\n'
    s = s[:-3]
    if dltype == 'gp':
        s += '];\n'
    else:
        s += ']\n'
    if delim == 'brace':
        s = s.replace('[', '{')
        s = s.replace(']', '}')
    if delim == 'magma':
        s = s.replace('[', '[*')
        s = s.replace(']', '*]')
        s += ';'
    strIO = BytesIO()
    strIO.write(s.encode('utf-8'))
    strIO.seek(0)
    return send_file(strIO,
                     attachment_filename=filename,
                     as_attachment=True,
                     add_etags=False)

def number_field_jump(info):
    query = {'label_orig': info['jump']}
    try:
        parse_nf_string(info, query, 'jump',name="Label", qfield='label')
        # we end up parsing the string twice, but that is okay
        F1, _, _ = input_string_to_poly(info['jump'])
        # we only use the output of input_string_to_poly with single-letter variable names
        if F1  and len(str(F1.parent().gen())) == 1 and F1.list() != db.nf_fields.lookup(query['label'], 'coeffs'):
            flash_info(r"The requested field $\Q[{}]/\langle {}\rangle$ is isomorphic to the field below, but uses a different defining polynomial.".format(str(F1.parent().gen()), latex(F1)))
        return redirect(url_for(".by_label", label=query['label']))
    except ValueError:
        return redirect(url_for(".number_field_render_webpage"))

# This doesn't seem to be used currently
#def number_field_algebra(info):
#    fields = info['algebra'].split('_')
#    fields2 = [WebNumberField.from_coeffs(a) for a in fields]
#    for j in range(len(fields)):
#        if fields2[j] is None:
#            fields2[j] = WebNumberField.fakenf(fields[j])
#    t = 'Number field algebra'
#    info = {'results': fields2}
#    return render_template("number_field_algebra.html", info=info, title=t, bread=bread)


@search_wrap(template="nf-search.html",
             table=db.nf_fields,
             title='Number field search results',
             err_title='Number field search error',
             per_page=50,
             shortcuts={'jump':number_field_jump,
                        #'algebra':number_field_algebra,
                        'download':download_search},
             url_for_label=url_for_label,
             bread=lambda:[('Number fields', url_for(".number_field_render_webpage")),
                           ('Search results', '.')],
             learnmore=learnmore_list)
def number_field_search(info, query):
    parse_ints(info,query,'degree')
    parse_galgrp(info,query, qfield=('galois_label', 'degree'))
    parse_bracketed_posints(info,query,'signature',qfield=('degree','r2'),exactlength=2, allow0=True, extractor=lambda L: (L[0]+2*L[1],L[1]))
    parse_signed_ints(info,query,'discriminant',qfield=('disc_sign','disc_abs'))
    parse_floats(info, query, 'rd')
    parse_floats(info, query, 'regulator')
    parse_ints(info,query,'class_number')
    parse_ints(info,query,'num_ram')
    parse_bool(info,query,'cm_field',qfield='cm')
    parse_bool(info,query,'is_galois')
    parse_bracketed_posints(info,query,'class_group',check_divisibility='increasing',process=int)
    parse_primes(info,query,'ur_primes',name='Unramified primes',
                 qfield='ramps',mode='exclude')
    parse_primes(info,query,'ram_primes',name='Ramified primes',
                 qfield='ramps',mode=info.get('ram_quantifier'),radical='disc_rad')
    parse_subfield(info, query, 'subfield', qfield='subfields', name='Intermediate field')
    parse_padicfields(info, query, 'completions', qfield='local_algs', name='$p$-adic completions')
    info['wnf'] = WebNumberField.from_data
    info['gg_display'] = group_pretty_and_nTj


def residue_field_degrees_function(nf):
    """ Given the result of pari(nfinit(...)), returns a function that has
            input: a prime p
            output: the residue field degrees at the prime p
    """
    D = nf.disc()

    def decomposition(p):
        if not ZZ(p).divides(D):
            return [z[3] for z in nf.idealprimedec(p)]
        else:
            raise ValueError("Expecting a prime not dividing D")

    return decomposition


# Format Frobenius cycle types coming from the database
def see_frobs(frob_data):
    ans = []
    seeram = False
    plist = [p for p in primes(2, 60)]
    for i in range(len(plist)):
        p = plist[i]
        dec = frob_data[i][1]
        if dec[0] == 0:
            ans.append([p, 'R'])
            seeram = True
        else:
            s = '$'
            firstone = True
            for j in dec:
                if not firstone:
                    s += r'{,}\,'
                if j[0]<15:
                    s += r'{\href{%s}{%d} }'%(url_for('local_fields.by_label',
                        label="%d.%d.0.1"%(p,j[0])), j[0])
                else:
                    s += str(j[0])
                if j[1] > 1:
                    s += '^{' + str(j[1]) + '}'
                firstone = False
            s += '$'
            ans.append([p, s])
    return ans, seeram


# Compute Frobenius cycle types, returns string nicely presenting this
def frobs(nf):
    frob_at_p = residue_field_degrees_function(nf.gpK())
    D = nf.disc()
    ans = []
    seeram = False
    for p in primes(2, 60):
        if not ZZ(p).divides(D):
            # [3] ,   [2,1]
            dec = frob_at_p(p)
            vals = list(set(dec))
            vals = sorted(vals, reverse=True)
            dec = [[x, dec.count(x)] for x in vals]
            #dec2 = ["$" + str(x[0]) + ('^{' + str(x[1]) + '}$' if x[1] > 1 else '$') for x in dec]
            s = '$'
            firstone = True
            for j in dec:
                if not firstone:
                    s += r'{,}\,'
                if j[0]<15:
                    s += r'{\href{%s}{%d} }'%(url_for('local_fields.by_label',
                        label="%d.%d.0.1"%(p,j[0])), j[0])
                else:
                    s += str(j[0])
                if j[1] > 1:
                    s += '^{' + str(j[1]) + '}'
                firstone = False
            s += '$'
            ans.append([p, s])
        else:
            ans.append([p, 'R'])
            seeram = True
    return ans, seeram


# utility for downloading data
def unlatex(s):
    s = re.sub(r'\\+', r'\\',s)
    s = s.replace(r'&nbsp;', r' ')
    s = s.replace('\\(', '')
    s = s.replace('\\)', '')
    s = re.sub(r'\\frac{(.+?)}{(.+?)}', r'(\1)/(\2)', s)
    s = s.replace(r'{',r'(')
    s = s.replace(r'}',r')')
    s = re.sub(r'([^\s+-,])\s*a', r'\1*a',s)
    return s


@nf_page.route('/<nf>/download/<download_type>')
def nf_download(**args):
    typ = args['download_type']
    if typ == 'data':
        response = make_response(nf_data(**args))
    else:
        response = make_response(nf_code(**args))
    response.headers['Content-type'] = 'text/plain'
    return response


def nf_data(**args):
    label = args['nf']
    nf = WebNumberField(label)
    data = '/* Data is in the following format\n'
    data += '   Note, if the class group has not been computed, it, the class number, the fundamental units, regulator and whether grh was assumed are all 0.\n'
    data += '[polynomial,\ndegree,\nt-number of Galois group,\nsignature [r,s],\ndiscriminant,\nlist of ramifying primes,\nintegral basis as polynomials in a,\n1 if it is a cm field otherwise 0,\nclass number,\nclass group structure,\n1 if grh was assumed and 0 if not,\nfundamental units,\nregulator,\nlist of subfields each as a pair [polynomial, number of subfields isomorphic to one defined by this polynomial]\n]'
    data += '\n*/\n\n'
    zk = nf.zk()
    Ra = PolynomialRing(QQ, 'a')
    zk = [str(Ra(x)) for x in zk]
    zk = ', '.join(zk)
    units = str(unlatex(nf.units()))
    units = units.replace('&nbsp;', ' ')
    subs = nf.subfields()
    subs = [[coeff_to_poly(string2list(z[0])),z[1]] for z in subs]

    # Now add actual data
    data += '[%s, '%nf.poly()
    data += '%s, '%nf.degree()
    data += '%s, '%nf.galois_t()
    data += '%s, '%nf.signature()
    data += '%s, '%nf.disc()
    data += '%s, '%nf.ramified_primes()
    data += '[%s], '%zk
    data += '%s, '%str(1 if nf.is_cm_field() else 0)
    if nf.can_class_number():
        data += '%s, '%nf.class_number()
        data += '%s, '%nf.class_group_invariants_raw()
        data += '%s, '%(1 if nf.used_grh() else 0)
        data += '[%s], '%units
        data += '%s, '%nf.regulator()
    else:
        data += '0,0,0,0,0, '
    data += '%s'%subs
    data += ']'
    return data


sorted_code_names = ['field', 'poly', 'degree', 'signature',
                     'discriminant', 'ramified_primes',
                     'integral_basis', 'class_group', 'unit_group',
                     'unit_rank', 'unit_torsion_gen',
                     'fundamental_units', 'regulator', 'galois_group',
                     'prime_cycle_types']

code_names = {'field': 'Define the number field',
              'poly': 'Defining polynomial',
              'degree': 'Degree over Q',
              'signature': 'Signature',
              'discriminant': 'Discriminant',
              'ramified_primes': 'Ramified primes',
              'integral_basis': 'Integral basis',
              'class_group': 'Class group',
              'unit_group': 'Unit group',
              'unit_rank': 'Unit rank',
              'unit_torsion_gen': 'Generator for roots of unity',
              'fundamental_units': 'Fundamental units',
              'regulator': 'Regulator',
              'galois_group': 'Galois group',
              'prime_cycle_types': 'Frobenius cycle types'}

Fullname = {'magma': 'Magma', 'sage': 'SageMath', 'gp': 'Pari/GP'}
Comment = {'magma': '//', 'sage': '#', 'gp': '\\\\', 'pari': '\\\\'}


def nf_code(**args):
    label = args['nf']
    lang = args['download_type']
    nf = WebNumberField(label)
    nf.make_code_snippets()
    code = "{} {} code for working with number field {}\n\n".format(Comment[lang],Fullname[lang],label)
    code += "{} (Note that not all these functions may be available, and some may take a long time to execute.)\n".format(Comment[lang])
    if lang == 'gp':
        lang = 'pari'
    for k in sorted_code_names:
        if lang in nf.code[k]:
            code += "\n{} {}: \n".format(Comment[lang],code_names[k])
            code += nf.code[k][lang] + ('\n' if '\n' not in nf.code[k][lang] else '')
    return code

class NFSearchArray(SearchArray):
    noun = "field"
    plural_noun = "fields"
    jump_example = "x^7 - x^6 - 3 x^5 + x^4 + 4 x^3 - x^2 - x + 1"
    jump_egspan = r"e.g. 2.2.5.1, Qsqrt5, x^2-5, or x^2-x-1 for \(\Q(\sqrt{5})\)"
    jump_knowl = "nf.search_input"
    jump_prompt = "Label, name, or polynomial"
    def __init__(self):
        degree = TextBox(
            name="degree",
            label="Degree",
            knowl="nf.degree",
            example=3)
        signature = TextBox(
            name="signature",
            label="Signature",
            knowl="nf.signature",
            example="[1,1]")
        discriminant = TextBox(
            name="discriminant",
            label="Discriminant",
            knowl="nf.discriminant",
            example="-1000..-1",
            example_span="-3 or 1000-2000")
        rd = TextBox(
            name="rd",
            label="Root discriminant",
            knowl="nf.root_discriminant",
            example="1..4.3",
            example_span="a range such as 1..4.3 or 3-10")
        cm_field = YesNoBox(
            name="cm_field",
            label="CM field",
            knowl="nf.cm_field")
        gal = TextBox(
            name="galois_group",
            label="Galois group",
            knowl="nf.galois_search",
            example="C5",
            example_span="[8,3], C5 or 7T2")
        is_galois = YesNoBox(
            name="is_galois",
            label="Is Galois",
            knowl="nf.galois_group")
        regulator = TextBox(
            name="regulator",
            label="Regulator",
            knowl="nf.regulator",
            example="1..3.5",
            example_span="a range such as 1..3.5")
        class_number = TextBox(
            name="class_number",
            label="Class number",
            knowl="nf.class_number",
            example="5")
        class_group = TextBox(
            name="class_group",
            label="Class group structure",
            knowl="nf.ideal_class_group",
            example="[2,4]",
            example_span="[ ], [3], or [2,4]")
        num_ram = TextBox(
            name="num_ram",
            label="Ramified prime count",
            knowl="nf.ramified_primes",
            example=2)
        ram_quantifier = SubsetNoExcludeBox(
            name="ram_quantifier")
        ram_primes = TextBoxWithSelect(
            name="ram_primes",
            label="Ramified",
            knowl="nf.ramified_primes",
            example="2,3",
            select_box=ram_quantifier)
        ur_primes = TextBox(
            name="ur_primes",
            label="Unramified primes",
            knowl="nf.unramified_prime",
            example="2,3")
        subfield = TextBox(
            name="subfield",
            label="Intermediate field",
            knowl="nf.intermediate_fields",
            example_span="2.2.5.1 or x^2-5 or a "+
                display_knowl("nf.nickname", "field nickname"),
            example="x^2-5")
        completion = TextBox(
            name="completions",
            label="$p$-adic completions",
            knowl="nf.padic_completion.search",
            example_span="2.4.10.7 or 2.4.10.7,3.2.1.2",
            example="2.4.10.7")
        count = CountBox()

        self.browse_array = [
            [degree, signature],
            [discriminant, rd],
            [gal, is_galois],
            [class_number, class_group],
            [num_ram, cm_field],
            [ram_primes, ur_primes],
            [regulator, subfield],
            [count, completion]]

        self.refine_array = [
            [degree, signature, class_number, class_group, cm_field],
            [num_ram, ram_primes, ur_primes, gal, is_galois],
            [discriminant, rd, regulator, subfield, completion]]
