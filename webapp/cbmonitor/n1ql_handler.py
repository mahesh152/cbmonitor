from couchbase.n1ql import N1QLQuery
from couchbase.bucket import Bucket
from couchbase import LOCKMODE_WAIT

couchbase_bucket = Bucket('couchbase://localhost/observable', lockmode=LOCKMODE_WAIT)


def add_cluster(name):
    value = {"cluster": name}
    num = couchbase_bucket.stats()["curr_items_tot"]['localhost:11210']
    couchbase_bucket.upsert("c{}".format(num), value)


def add_server(address, cluster):
    value = {"cluster": cluster, "address": address}
    num = couchbase_bucket.stats()["curr_items_tot"]['localhost:11210']
    couchbase_bucket.upsert("c{}".format(num), value)


def add_bucket(name, cluster):
    value = {"cluster": cluster, "bucket": name}
    num = couchbase_bucket.stats()["curr_items_tot"]['localhost:11210']
    couchbase_bucket.upsert("c{}".format(num), value)


def add_index(name, cluster):
    value = {"cluster": cluster, "index": name}
    num = couchbase_bucket.stats()["curr_items_tot"]['localhost:11210']
    couchbase_bucket.upsert("c{}".format(num), value)


def add_snapshot(name, cluster):
    value = {"cluster": cluster, "snapshot": name}
    num = couchbase_bucket.stats()["curr_items_tot"]['localhost:11210']
    couchbase_bucket.upsert("c{}".format(num), value)


def get_clusters():
    q = N1QLQuery('SELECT DISTINCT `cluster` FROM `observable` where `cluster` IS NOT MISSING')
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        ret.append(row["cluster"])
    return ret


def get_clusters_all(cluster):
    q = N1QLQuery('SELECT  * FROM `observable` where `cluster` = $cluster and `name` IS NOT MISSING', cluster=cluster)
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        ret.append(row["observable"])
    return ret


def get_servers(cluster):
    q = N1QLQuery('SELECT DISTINCT `address` FROM `observable` where `cluster` = $cluster', cluster=cluster)
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        if "address" in row:
            ret.append(row["address"])
    return ret


def get_servers_all(cluster, server):
    q = N1QLQuery(
        'SELECT  * FROM `observable` where `cluster` = $cluster and `address` = $address and `name` IS NOT MISSING',
        cluster=cluster, address=server)
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        ret.append(row["observable"])
    return ret


def get_buckets(cluster):
    q = N1QLQuery('SELECT DISTINCT `bucket` FROM `observable` where `cluster` = $cluster', cluster=cluster)
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        if "bucket" in row:
            ret.append(row["bucket"])
    return ret


def get_buckets_all(cluster, bucket):
    q = N1QLQuery(
        'SELECT  * FROM `observable` where `cluster` = $cluster and `bucket` = $bucket and `name` IS NOT MISSING',
        cluster=cluster, bucket=bucket)
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        ret.append(row["observable"])
    return ret


def get_indexes(cluster):
    q = N1QLQuery('SELECT DISTINCT `index` FROM `observable` where `cluster` = $cluster', cluster=cluster)
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        if "index" in row:
            ret.append(row["index"])
    return ret


def get_indexes_all(cluster, index):
    q = N1QLQuery(
        'SELECT  * FROM `observable` where `cluster` = $cluster and `index` = $index and `name` IS NOT MISSING',
        cluster=cluster, index=index)
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        ret.append(row["observable"])
    return ret


def get_snapshots(cluster):
    q = N1QLQuery('SELECT DISTINCT `snapshot` FROM `observable` where `cluster` = $cluster', cluster=cluster)
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        if "snapshot" in row:
            ret.append(row["snapshot"])
    return ret


def get_snapshot(snapshot):
    q = N1QLQuery('SELECT * FROM `observable` where `snapshot` = $snapshot', snapshot=snapshot)
    for row in couchbase_bucket.n1ql_query(q):
        return row["observable"]


def add_metric(cluster, bucket, index, server, collector, name):
    value = {"cluster": cluster, "collector": collector, "name": name}
    if bucket:
        value["bucket"] = bucket
    if index:
        value["index"] = index
    if server:
        value["server"] = server
    num = couchbase_bucket.stats()["curr_items_tot"]['localhost:11210']
    couchbase_bucket.upsert("c{}".format(num), value)


def get_metrics(cluster, bucket, index, server, collector):
    query = "SELECT DISTINCT collector, name FROM `observable` where `cluster` = $cluster"
    if bucket:
        query += " and `bucket`='{}'".format(bucket)
    if index:
        query += " and `index`='{}'".format(index)
    if server:
        query += " and `server`='{}'".format(server)
    if collector:
        query += " and `collector`='{}'".format(collector)
    q = N1QLQuery(query, cluster=cluster)
    ret = []
    for row in couchbase_bucket.n1ql_query(q):
        if "name" in row:
            ret.append(row)
    return ret
