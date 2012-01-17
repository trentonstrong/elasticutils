from django.core.management.base import BaseCommand, CommandError
from django.db.models import get_app, get_models
from django.core.paginator import Paginator

from elasticutils.models import SearchMixin

class Command(BaseCommand):
    args = '<app app ...>'
    help = 'Indexes the specified applications for search.  Only indexes models utilizing the SearchMixin'

    def handle(self, *args, **options):
        for app_name in args:
            try:
                app = get_app(app_name) 
            except ImproperlyConfigured:
                raise CommandError('App "%s" does not exist or is improperly configured' % app_name)

            searchable_models = [model for model in get_models(app) if issubclass(model, SearchMixin)]

            for searchable_model in searchable_models:
                self.stdout.write('Indexing model %s' % searchable_model)
                count = searchable_model.objects.count()
                index = searchable_model.index
                for instance in searchable_model.objects.iterator():
                    index(instance.fields)
