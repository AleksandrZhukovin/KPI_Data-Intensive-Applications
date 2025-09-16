from django.db import models


class Counter(models.Model):
    counter = models.IntegerField(default=0)

