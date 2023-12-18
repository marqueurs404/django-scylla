from django.db.models import Field, ForeignObject
from django.db.models.lookups import FieldGetDbPrepValueMixin, BuiltinLookup


@Field.register_lookup
class NotEqual(FieldGetDbPrepValueMixin, BuiltinLookup):
    lookup_name = 'ne'


@ForeignObject.register_lookup
@Field.register_lookup
class IsNull(BuiltinLookup):
    lookup_name = 'isnull'
    prepare_rhs = False

    def as_sql(self, compiler, connection):
        lhs_sql, params = self.process_lhs(compiler, connection)
        if self.rhs:
            return '%s = NULL' % lhs_sql, params
        else:
            return '%s IS NOT NULL' % lhs_sql, params
