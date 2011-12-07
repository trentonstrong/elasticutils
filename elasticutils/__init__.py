from functools import wraps
from itertools import izip
import logging
from operator import itemgetter
from threading import local

from pyes import ES, exceptions

try:
    from statsd import statsd
except ImportError:
    statsd = None

try:
    from django.conf import settings
except ImportError:
    import es_settings as settings


_local = local()
_local.disabled = {}
log = logging.getLogger('elasticsearch')


def get_es():
    """Return one es object."""
    if not hasattr(_local, 'es'):
        timeout = getattr(settings, 'ES_TIMEOUT', 1)
        dump = getattr(settings, 'ES_DUMP_CURL', False)
        _local.es = ES(settings.ES_HOSTS,
                       default_indexes=[settings.ES_INDEXES['default']],
                       timeout=timeout, dump_curl=dump)
    return _local.es


def es_required(f):
    @wraps(f)
    def wrapper(*args, **kw):
        if settings.ES_DISABLED:
            # Log once.
            if f.__name__ not in _local.disabled:
                log.debug('Search disabled for %s.' % f)
                _local.disabled[f.__name__] = 1
            return

        return f(*args, es=get_es(), **kw)
    return wrapper


def es_required_or_50x(disabled_msg, error_msg):
    """
    This takes a Django view that requires ElasticSearch.

    If `ES_DISABLED` is `True` then we raise a 501 Not Implemented and display
    the disabled_msg.  If we try the view and an ElasticSearch exception is
    raised we raise a 503 error with the error_msg.

    We use user-supplied templates in elasticutils/501.html and
    elasticutils/503.html.
    """
    def wrap(f):
        @wraps(f)
        def wrapper(request, *args, **kw):
            from django.shortcuts import render
            if settings.ES_DISABLED:
                response = render(request, 'elasticutils/501.html',
                                  {'msg': disabled_msg})
                response.status_code = 501
                return response
            else:
                try:
                    return f(request, *args, **kw)
                except exceptions.ElasticSearchException as error:
                    response = render(request, 'elasticutils/503.html',
                            {'msg': error_msg, 'error': error})
                    response.status_code = 503
                    return response

        return wrapper

    return wrap


def _split(string):
    if '__' in string:
        return string.rsplit('__', 1)
    else:
        return string, None


def _process_filters(filters):
    rv = []
    for f in filters:
        if isinstance(f, F):
            rv.append(f.filters)
        else:
            key, val = f
            key, field_action = _split(key)
            if key == 'or_':
                rv.append({'or':_process_filters(val.items())})
            elif field_action is None:
                rv.append({'term': {key: val}})
            elif field_action == 'in':
                rv.append({'in': {key: val}})
            elif field_action in ('gt', 'gte', 'lt', 'lte'):
                rv.append({'range': {key: {field_action: val}}})
    return rv


class F(object):
    """
    Filter objects.
    """
    def __init__(self, **filters):
        if filters:
            items = _process_filters(filters.items())
            if len(items) > 1:
                self.filters = {'and': items }
            else:
                self.filters = items[0]
        else:
            self.filters = {}

    def _combine(self, other, conn='and'):
        """
        OR and AND will create a new F, with the filters from both F objects
        combined with the connector `conn`.
        """
        f = F()
        if conn in self.filters:
            f.filters = self.filters
            f.filters[conn].append(other.filters)
        elif conn in other.filters:
            f.filters = other.filters
            f.filters[conn].append(self.filters)
        else:
            f.filters = {conn: [self.filters, other.filters]}
        return f

    def __or__(self, other):
        return self._combine(other, 'or')

    def __and__(self, other):
        return self._combine(other, 'and')

    def __invert__(self):
        f = F()
        if (len(self.filters) < 2 and
           'not' in self.filters and 'filter' in self.filters['not']):
            f.filters = self.filters['not']['filter']
        else:
            f.filters = {'not': {'filter': self.filters}}
        return f


# Number of results to show before truncating when repr(S)
REPR_OUTPUT_SIZE = 20


class S(object):
    """
    Represents a lazy ElasticSearch lookup, with a similar api to Django's
    QuerySet.
    """
    def __init__(self, type_):
        self.type = type_
        self.steps = []
        self.start = 0
        self.stop = None
        self.as_list = self.as_dict = False
        self._results_cache = None
        self._query_fields = set()
        self._highlight_fields = []
        self._highlight_options = {}
        self._weights = {}

    def __repr__(self):
        data = list(self)[:REPR_OUTPUT_SIZE + 1]
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return repr(data)

    def _clone(self, next_step=None):
        new = self.__class__(self.type)
        new.steps = list(self.steps)
        new._query_fields = self._query_fields.copy()
        new._weights = self._weights.copy()
        if next_step:
            new.steps.append(next_step)
        new.start = self.start
        new.stop = self.stop
        new._highlight_fields = self._highlight_fields
        new._highlight_options = self._highlight_options
        return new

    def values(self, *fields):
        """
        Returns a new S instance whose SearchResults will be of the class
        ListSearchResults.
        """
        return self._clone(next_step=('values', fields))

    def values_dict(self, *fields):
        """
        Returns a new S instance whose SearchResults will be of the class
        DictSearchResults.
        """
        return self._clone(next_step=('values_dict', fields))

    def order_by(self, *fields):
        """
        Returns a new S instance with the ordering changed.
        """
        return self._clone(next_step=('order_by', fields))

    def query(self, *args, **kw):
        """
        Returns a new S instance with the query args combined to the existing
        set.

        Take either a single arg (which is text to be matched against any of
        the fields specified by ``query_fields()``) or a series of kwargs
        which set comparison values for each field separately.
        """
        if bool(args) == bool(kw):
            raise TypeError('query() takes either an arg or one or more kwargs.')
        if kw:
            return self._clone(next_step=('query', kw.items()))
        if len(args) != 1:
            return TypeError('query() takes at most one non-keyword argument.')
        return self._clone(next_step=('query_default_fields', args[0]))

    def weight(self, **kw):
        """
        Set the per-field boosting of results.

        For example::

            s = S().weight(summary__text=0.8, heat=3)

        Weights given here are added to any defaults or any previously
        specified weights, though later references to the same field override
        earlier ones.

        Weights apply only to fields mentioned in the call to ``query()`` or
        implicitly used due to a previous call to ``query_fields``.

        Note: If we need to clear weights, add a ``clear_weights()`` method. If
        we ever need index boosting, ``weight_indices()`` might be nice.

        """
        new = self._clone()
        new._weights.update(kw)
        return new

    def filter(self, *filters, **kw):
        """
        Returns a new S instance with the filter args combined to the existing
        set.
        """
        return self._clone(next_step=('filter', list(filters) + kw.items()))

    def facet(self, **kw):
        """
        Returns a new S instance with the facet args combined to the existing
        set.
        """
        return self._clone(next_step=('facet', kw.items()))

    def highlight(self, *highlight_fields, **kwargs):
        """Set highlight/excerpting with specified options.

        This highlight will override previous highlights.

        This won't let you clear it--we'd need to write a
        ``clear_highlight()``.

        :arg highlight_fields: The list of fields to highlight.

        Additional keyword options:

        * ``before_match`` -- Text to insert before each highlighted portion
        * ``after_match`` -- Text to insert after each highlighted portion

        """
        # TODO: Implement `limit` kwarg if useful.
        # TODO: Once oedipus is no longer needed in SUMO, support ranked lists
        # of before_match and after_match tags. ES can highlight more
        # significant stuff brighter.
        return self._clone(next_step=('highlight', (highlight_fields, kwargs)))

    def excerpt(self, result):
        """
        Take a result and return the excerpts as a list of
        items--one for each highlight_field in the order specified.

        Each item is a list of text fragments, with portions surrounded by
        highlight markers.

        """
        if not self._results_cache:
            raise ExcerptError(
                'excerpt() was called before results were fetched.')  # test

        # To enforce oedipus compatibility, we could complain if
        # highlight_fields are not a subset of the fields requested by a
        # values() or values_list() call, but ES has no need for such a
        # restriction.

        return [result._elasticutils_highlights.get(f, [u''])
                for f in self._highlight_fields]

    def query_fields(self, *args):
        """
        Add to the fields that a single-arg call to ``query()`` will query.

        A call like this... ::

            s.query_fields('a', 'b__text').query('woot')

        is equivalent to... ::

            s.query(or_=dict(a='woot', b='woot'))

        """
        new = self._clone()
        # Use a dedicated field for the query_fields so we're guaranteed to
        # have them in place before _build_query() processes any query() steps
        # that use them.
        new._query_fields |= set(args)
        return new

    def extra(self, **kw):
        """
        Returns a new S instance with the extra args combined with the existing
        set.
        """
        new = self._clone()
        actions = 'values values_dict order_by query filter facet'.split()
        for key, vals in kw.items():
            assert key in actions
            if hasattr(vals, 'items'):
                new.steps.append((key, vals.items()))
            else:
                new.steps.append((key, vals))
        return new

    def count(self):
        """
        Returns the number of hits for the current query and filters as an
        integer.
        """
        if self._results_cache:
            return self._results_cache.count
        else:
            return self[:0].raw()['hits']['total']

    def __len__(self):
        return len(self._do_search())

    def __getitem__(self, k):
        new = self._clone()
        # TODO: validate numbers and ranges
        if isinstance(k, slice):
            new.start, new.stop = k.start or 0, k.stop
            return new
        else:
            new.start, new.stop = k, k + 1
            return list(new)[0]

    def _build_query(self):
        """
        Loops self.steps to build the query format that will be sent to
        ElasticSearch, and returns it as a dict.
        """
        filters = []
        queries = []
        sort = []
        fields = ['id']
        facets = {}
        as_list = as_dict = False
        for action, value in self.steps:
            if action == 'order_by':
                sort = []
                for key in value:
                    if key.startswith('-'):
                        sort.append({key[1:]: 'desc'})
                    else:
                        sort.append(key)
            elif action == 'values':
                fields.extend(value)
                as_list, as_dict = True, False
            elif action == 'values_dict':
                if not value:
                    fields = []
                else:
                    fields.extend(value)
                as_list, as_dict = False, True
            elif action == 'query':
                queries.extend(self._process_queries(value))
            elif action == 'query_default_fields':
                queries.extend(self._process_queries(
                    {'or_': dict((f, value) for f in self._query_fields)}))
            elif action == 'filter':
                filters.extend(_process_filters(value))
            elif action == 'facet':
                facets.update(value)
            elif action == 'highlight':
                self._highlight_fields = value[0]
                self._highlight_options = value[1]
            else:
                raise NotImplementedError(action)

        qs = {}
        if len(filters) > 1:
            qs['filter'] = {'and': filters}
        elif filters:
            qs['filter'] = filters[0]

        if len(queries) > 1:
            qs['query'] = {'bool': {'must': queries}}
        elif queries:
            qs['query'] = queries[0]

        if fields:
            qs['fields'] = fields
        if facets:
            qs['facets'] = facets
            # Copy filters into facets. You probably wanted this.
            for facet in facets.values():
                if 'facet_filter' not in facet and filters:
                    facet['facet_filter'] = qs['filter']
        if sort:
            qs['sort'] = sort
        if self.start:
            qs['from'] = self.start
        if self.stop is not None:
            qs['size'] = self.stop - self.start

        if self._highlight_fields:
            qs['highlight'] = self._build_highlight()

        self.fields, self.as_list, self.as_dict = fields, as_list, as_dict
        return qs

    def _build_highlight(self):
        """Return the portion of the query that controls highlighting."""
        options = self._highlight_options
        ret = {'fields': dict((f, {}) for f in self._highlight_fields)}
        if 'before_match' in options:
            ret['pre_tags'] = [options['before_match']]
        if 'after_match' in options:
            ret['post_tags'] = [options['after_match']]
        return ret

    _action_map = {None: 'term',
                   'startswith': 'prefix',
                   'text': 'text',
                   'fuzzy': 'fuzzy'}

    def _process_queries(self, value):
        def _weighted_key_value(value):
            """
            Return a key-value pair in the format ES queries need.

            Weight the pair according to any previous calls to ``weight()``.

            """
            if key in self._weights:
                return {field: {'boost': self._weights[key],
                                'query' if action == 'text' else 'value':
                                    value}}
            return {field: value}

        rv = []
        value = dict(value)
        or_ = value.pop('or_', [])
        for key, val in value.items():
            field, action = _split(key)
            if action in ('gt', 'gte', 'lt', 'lte'):
                rv.append(
                    {'range': {field: _weighted_key_value({action: val})}})
            else:
                rv.append({self._action_map[action]: _weighted_key_value(val)})
        if or_:
            rv.append({'bool': {'should': self._process_queries(or_.items())}})
        return rv

    def _do_search(self):
        """
        Performs the search, then converts that raw format into a
        SearchResults instance and returns it.
        """
        if not self._results_cache:
            hits = self.raw()
            if self.as_dict:
                ResultClass = DictSearchResults
            elif self.as_list:
                ResultClass = ListSearchResults
            else:
                ResultClass = ObjectSearchResults
            self._results_cache = ResultClass(self.type, hits, self.fields)
        return self._results_cache

    def raw(self):
        """
        Builds query and passes to ElasticSearch, then returns the raw format
        returned.
        """
        qs = self._build_query()
        es = get_es()
        index = (settings.ES_INDEXES.get(self.type)
                 or settings.ES_INDEXES['default'])
        try:
            hits = es.search(qs, index, self.type._meta.db_table)
        except Exception:
            log.error(qs)
            raise
        if statsd:
            statsd.timing('search', hits['took'])
        log.debug('[%s] %s' % (hits['took'], qs))
        return hits

    def __iter__(self):
        return iter(self._do_search())

    def raw_facets(self):
        return self._do_search().results.get('facets', {})

    @property
    def facets(self):
        facets = {}
        for key, val in self.raw_facets().items():
            if val['_type'] == 'terms':
                facets[key] = [v for v in val['terms']]
            elif val['_type'] == 'range':
                facets[key] = [v for v in val['ranges']]
        return facets


class SearchResults(object):
    def __init__(self, type, results, fields):
        self.type = type
        self.took = results['took']
        self.count = results['hits']['total']
        self.results = results
        self.fields = fields
        self.set_objects(results['hits']['hits'])

    def set_objects(self, hits):
        raise NotImplementedError()

    def __iter__(self):
        return iter(self.objects)

    def __len__(self):
        return len(self.objects)


class _DictResult(dict):
    """Wrapper for a dict that allows us to attach other attributes"""


class DictSearchResults(SearchResults):
    def set_objects(self, hits):
        key = 'fields' if self.fields else '_source'
        self.objects = [_decorate_with_highlights(_DictResult(r[key]), r)
                        for r in hits]


class _ListResult(list):
    """Wrapper for a list that allows us to attach other attributes"""


class ListSearchResults(SearchResults):
    def set_objects(self, hits):
        if self.fields:
            getter = itemgetter(*self.fields)
            objs = [getter(r['fields']) for r in hits]
        else:
            objs = [r['_source'].values() for r in hits]
        self.objects = [_decorate_with_highlights(_ListResult(o), h)
                        for o, h in izip(objs, hits)]


class ObjectSearchResults(SearchResults):
    def set_objects(self, hits):
        self.ids = [int(r['_id']) for r in hits]
        self.objects = self.type.objects.filter(id__in=self.ids)

    def __iter__(self):
        objs = dict((obj.id, obj) for obj in self.objects)
        return (_decorate_with_highlights(objs[id], r)
                for (id, r) in
                izip(self.ids, self.results['hits']['hits']) if id in objs)


def _decorate_with_highlights(obj, hit):
    """Return obj with its dict of its highlights tacked on."""
    # There's no simple way to map from a result back to its entry in
    # the hits hash in constant time, so we'd better annotate it now.
    obj._elasticutils_highlights = hit.get('highlight', {})
    # TODO: Once oedipus goes away in SUMO, perhaps a renamed
    # _elasticutils_highlights should be the public API for
    # getting at highlights. The FlightDeck branch uses a search_meta hash on
    # each instance for such things; maybe do that.
    return obj


class ExcerptError(Exception):
    """Exception raised if ``S.excerpt()`` is called before results are fetched
    """
    pass
