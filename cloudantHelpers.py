import couchdb
import itertools
import json
import os
from urlparse import urlparse

NAME= "[CLOUDANT HELPER] - "

def getSanitizedURI(uri):
    uriSplit= uri.split('@')
    if (len(uriSplit) >1):
        p = urlparse(uri)
        return p.geturl().replace(p.password, 'XXXXX')
    # running locally
    return uri

def getCouchServerURI():
    return "https://bmdevops-ys1-integration-us-south:j8DjkGDkdtfkqmme9nJGDte@bmdevops-ys1-integration-us-south.cloudant.com"

    couchServer = os.getenv('CLOUDANT_URI')
    if couchServer:
        print(NAME + "Using Environment variable value")
        return couchServer

    # get the VCAP_SERVICES env. variable
    services = os.getenv('VCAP_SERVICES')
    if services:
        couchInfo = json.loads(services).get('cloudantNoSQLDB')
        if couchInfo:
            couchServer = couchInfo[0]["credentials"]["url"]
            print("Using VCAP_SERVICES")
            return couchServer
        raise RuntimeError("Cloudant not configured. Missing CLOUDANT_URI env var, and no bound service.")

    # running locally
    print("Using local DB")
    return 'http://127.0.0.1:5984'

def connectToCloudantService():
    couchServer = getCouchServerURI()
    if couchServer:
        # print "log level in cloudantHelpers:", logging.getLevelName(logging.getLogger('').level)
        print "Connecting to CouchDB or Cloudant at: %s..." % getSanitizedURI(couchServer)
        couch = couchdb.Server(couchServer)
        return couch
    raise 'No CouchDB or Cloudant URL could be determined.'

# from couchdb/client.py, modified to avoid using startkey_docid for reduce views
def iterview(db, name, batch, wrapper=None, **options):
    """Iterate the rows in a view, fetching rows in batches and yielding
    one row at a time.

    Since the view's rows are fetched in batches any rows emitted for
    documents added, changed or deleted between requests may be missed or
    repeated.

    :param name: the name of the view; for custom views, use the format
                 ``design_docid/viewname``, that is, the document ID of the
                 design document and the name of the view, separated by a
                 slash.
    :param batch: number of rows to fetch per HTTP request.
    :param wrapper: an optional callable that should be used to wrap the
                    result rows
    :param options: optional query string parameters
    :return: row generator
    """
    # Check sane batch size.
    if batch <= 0:
        raise ValueError('batch must be 1 or more')
    # Save caller's limit, it must be handled manually.
    limit = options.get('limit')
    if limit is not None and limit <= 0:
        raise ValueError('limit must be 1 or more')
    while True:

        loop_limit = min(limit or batch, batch)
        # Get rows in batches, with one extra for start of next batch.
        options['limit'] = loop_limit + 1
        rows = list(db.view(name, wrapper, **options))

        # Yield rows from this batch.
        for row in itertools.islice(rows, loop_limit):
            yield row

        # Decrement limit counter.
        if limit is not None:
            limit -= min(len(rows), batch)

        # Check if there is nothing else to yield.
        if len(rows) <= batch or (limit is not None and limit == 0):
            break

        # Update options with start keys for next loop.
#         print "iterview next key:", rows[-1]['key']
        if options.get('reduce', False):
            options.update(startkey=rows[-1]['key'], skip=0)
        else:
            options.update(startkey=rows[-1]['key'],
                           startkey_docid=rows[-1]['id'], skip=0)
