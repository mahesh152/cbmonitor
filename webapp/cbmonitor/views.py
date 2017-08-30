import json
import logging

from cbmonitor.plotter import Plotter

from cbmonitor import n1ql_handler

from django.core.exceptions import ObjectDoesNotExist
from django.db.utils import IntegrityError
from django.http import HttpResponse, Http404
from django.shortcuts import render_to_response
from django.views.decorators.cache import cache_page

logger = logging.getLogger(__name__)


@cache_page()
def html_report(request):
    """Static HTML reports with PNG charts"""
    try:
        snapshots = parse_snapshots(request)
    except ObjectDoesNotExist:
        return HttpResponse("Wrong or missing snapshot", status=400)
    plotter = Plotter()
    images = plotter.plot(snapshots)

    def id_from_url(url):
        return url.split("/")[2].split(".")[0]

    urls = [(id_from_url(url), title, url) for title, url in images]

    if urls:
        return render_to_response("report.html", {"urls": urls})
    else:
        return HttpResponse("No metrics found", status=400)


def parse_snapshots(request):
    snapshots = []
    for snapshot in request.GET.getlist("snapshot"):
        snapshot = n1ql_handler.get_snapshot(snapshot)
        snapshots.append(snapshot)
    return snapshots


class ValidationError(Exception):
    def __init__(self, form):
        self.error = {item[0]: item[1][0] for item in form.errors.items()}

    def __str__(self):
        return json.dumps(self.error)


def validation(method):
    def wrapper(*args, **kargs):
        try:
            response = method(*args, **kargs)
        except Http404, error:
            logger.warn(error)
            return HttpResponse(content=error, status=404)
        except ValidationError, error:
            logger.warn(error)
            return HttpResponse(content=error, status=400)
        except IntegrityError, error:
            logger.warn(error)
            return HttpResponse(content=error, status=400)
        else:
            return response or HttpResponse(content="Success")

    return wrapper


@validation
def add_cluster(request):
    n1ql_handler.add_cluster(request.POST['name'])


@validation
def add_server(request):
    n1ql_handler.add_server(request.POST['address'], request.POST['cluster'])


@validation
def add_bucket(request):
    n1ql_handler.add_bucket(request.POST['name'], request.POST['cluster'])


@validation
def add_index(request):
    n1ql_handler.add_index(request.POST['name'], request.POST['cluster'])


def get_clusters(request):
    clusters = n1ql_handler.get_clusters()
    content = json.dumps(sorted(clusters))
    return HttpResponse(content)


@validation
def get_servers(request):
    servers = n1ql_handler.get_servers(request.GET["cluster"])
    content = json.dumps(sorted(servers))
    return HttpResponse(content)


@validation
def get_buckets(request):
    buckets = n1ql_handler.get_buckets(request.GET["cluster"])
    content = json.dumps(sorted(buckets))
    return HttpResponse(content)


@validation
def get_indexes(request):
    indexes = n1ql_handler.get_indexes(request.GET["cluster"])
    content = json.dumps(sorted(indexes))
    return HttpResponse(content)


@validation
def add_metric(request):
    bucket = request.POST["bucket"] if "bucket" in request.POST else None
    index = request.POST["index"] if "index" in request.POST else None
    server = request.POST["server"] if "server" in request.POST else None
    n1ql_handler.add_metric(cluster=request.POST["cluster"], bucket=bucket,
                            index=index, server=server,
                            collector=request.POST["collector"], name=request.POST["name"])


@validation
def get_metrics(request):
    bucket = request.GET["bucket"] if "bucket" in request.GET else None
    index = request.GET["index"] if "index" in request.GET else None
    server = request.GET["server"] if "server" in request.GET else None
    collector = request.GET["collector"] if "collector" in request.GET else None
    metrics = n1ql_handler.get_metrics(cluster=request.GET["cluster"], bucket=bucket,
                                       index=index, server=server,
                                       collector=collector)
    content = json.dumps(sorted(metrics))
    return HttpResponse(content)


@validation
def add_snapshot(request):
    n1ql_handler.add_snapshot(request.POST['name'], request.POST['cluster'])


def get_snapshots(request):
    snapshots = n1ql_handler.get_snapshots(request.GET["cluster"])
    snapshots.insert(0, "all_data")
    content = json.dumps(snapshots)
    return HttpResponse(content)
