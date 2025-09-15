from django.shortcuts import render
from django.http import HttpResponse


counter_val = 0


def index(request):
    return render(request, "counter/index.html")


def counter(request):
    global counter_val
    return render(request, "counter/counter.html", {"counter": counter_val})


def increase(request):
    global counter_val
    counter_val += 1
    return HttpResponse(status=204)
