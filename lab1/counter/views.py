from django.shortcuts import render
from django.http import HttpResponse
import threading


counter_val = 0
counter_lock = threading.Lock()


def index(request):
    return render(request, "counter/index.html")


def counter(request):
    global counter_val
    return render(request, "counter/counter.html", {"counter": counter_val})


def increase(request):
    global counter_val
    with counter_lock:
        counter_val += 1
    return HttpResponse(status=204)


def test_request(request):
    from time import sleep
    print('Request received')
    sleep(10)
    print("Request completed")
    return HttpResponse(status=204)