from django.db.models import F
from django.shortcuts import render
from django.http import HttpResponse

from .models import Counter


def index(request):
    return render(request, "counter/index.html")


def counter(request):
    counter = Counter.objects.get(id=1)
    return render(request, "counter/counter.html", {"counter": counter})


def increase(request):
    Counter.objects.filter(id=1).update(counter=F("counter") + 1)
    return HttpResponse(status=204)
