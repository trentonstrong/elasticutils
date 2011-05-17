.. ElasticUtils documentation master file, created by
   sphinx-quickstart on Mon May 16 15:52:49 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ElasticUtils
============

ElasticUtils makes querying and filtering and collecting facets from
ElasticSearch simple ::


    q = (Q(product='firefox').filter(version='4.0', platform='all')
                             .facet('product', True).facet('version')
                             .facet('platform').facet('type'))

Search All
----------
By default ``Q()`` will do a ``match_all`` query in ElasticSearch.

Search Query
------------

*Not implemented*

``Q('taco trucks')`` will do a query for "taco trucks".

Filters
-------

``Q('taco trucks').filter(style='korean')`` will do a query for "taco trucks"
filtering on the attribute ``style``.  This is how we find Korean Taco Trucks.

*Note*: Repeat calls to ``.filter`` will reset the filters.

Multiple Filters
~~~~~~~~~~~~~~~~

``Q('taco trucks').filter(style='korean', price='FREE')``
will do a query for "taco trucks" that are
"korean" style and have a price of
"FREE".

This is however equivalent to the more succinct::

Q('taco trucks', style='korean', price='FREE')

Complicate Filtering
~~~~~~~~~~~~~~~~~~~~

Sometimes you want something complicated.  For that we have the ``F`` (filter)
object.

``Q('taco trucks').filter(F(style='korean')|F(style='thai'))``
will find you "thai" or "korean" style taco trucks.

Let's say you only want "korean" tacos if you can get it for "FREE" or "thai"
tacos at any price::

Q('taco trucks').filter(F(style='korean', price='FREE')|F(style='thai'))


Facets
------
``Q('taco trucks').facet('style').facet('location')`` will do a query for
"taco trucks" and return facets for the ``style`` and ``location`` fields.
