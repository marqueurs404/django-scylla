from django.db.models import DateField as _DateField, DateTimeField as _DateTimeField


class DateField(_DateField):
    def to_python(self, value):
        # if value:
        #     value = value.date()
        return super().to_python(value)


class DateTimeField(_DateTimeField):
    def to_python(self, value):
        # if value:
        #     value = value.date()
        return super().to_python(value)
