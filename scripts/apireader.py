#!/usr/bin/env python
# -*- coding: utf8 -*-
from optparse import OptionParser
import os, sys
import json
import tornado.ioloop
from tornado import autoreload
import tornado.httpserver
import tornado.web
import csv
from StringIO import StringIO
from pymongo import Connection
from bson import json_util
from dateutil.parser import parse
from datetime import datetime
commands = ['analyze', 'prepare', 'serve']


MONGO_SERVER = 'localhost'
MONGO_PORT = 27017

WEB_PORT = 8000

DATE_PATTERNS = ["%d.%m.%Y", "%Y-%m-%d", "%y-%m-%d", "%Y-%m-%dT%H:%M:%S"]
DEFAULT_DICT_SHARE = 20


def format_data(data, format='json', keys=None, params = {}):
    """Returns data using specific format CSV or JSON"""
    if format == 'csv':
        io = StringIO()
        wr = csv.writer(io, dialect='excel')
        row = []
        keys = ['key', 'value']
        for k in keys:
            row.append(k)
        wr.writerow(row)
        for r in data.items():
            row = []
            row.append(r[0].encode('utf8', 'ignore'))
            row.append(unicode(r[1]).encode('utf8', 'ignore'))
            wr.writerow(row)
            value = io.getvalue()
        return value, "text/csv"
    elif format == 'csvlist':
        io = StringIO()
        wr = csv.writer(io, dialect='excel')
        row = []
        for k in keys:
            row.append(k)
        wr.writerow(row)
        for r in data:
            row = []
            for k in keys:
                row.append(unicode(r[k]).encode('utf8', 'ignore'))
            wr.writerow(row)
            value = io.getvalue()
        return value, "text/csv"
    elif format == 'json':
        s = json.dumps(data, indent=4, default=json_util.default)
        value = u'\n'.join([l.rstrip() for l in  s.splitlines()])
        return value, "application/json"
    else:   # by default - return JSON data
        s = json.dumps(data, indent=4, default=json_util.default)
        value = u'\n'.join([l.rstrip() for l in  s.splitlines()])
        return value, "application/json"

def guess_int_size(i):
    if i < 255:
        return 'uint8'
    if i < 65535:
        return 'uint16'
    return 'uint32'

def guess_datatype(s):
    """Guesses type of data by string provided"""
    attrs = {'base' : 'str'}
    if s.isdigit():
        if s[0] == 0:
            attrs = {'base' : 'numstr'}
        else:
            attrs = {'base' : 'int', 'subtype' : guess_int_size(long(s))}
    else:
        try:
            i = float(s)
            attrs = {'base' : 'float'}
            return attrs
        except ValueError:
            pass

        is_date = False
        for pat in DATE_PATTERNS:
            try:
                dt = datetime.strptime(s, pat)
                attrs = {'base' : 'date', 'pat': pat}
                is_date = True
                break
            except:
                pass
        if not is_date:
            if len(s.strip()) == 0:
                attrs = {'base' : 'empty'}
    return attrs



class BaseAPIHandler(tornado.web.RequestHandler):
    def initialize(self, config):
        self.config = config

    def _load_config(self):
        return json.load(open(self.app_key + '.config'), 'r')

    def get(self):
        raise NotImplementedError


class QueryHandler(BaseAPIHandler):
    def initialize(self, config):
        self.config = config
        self.conn = Connection(MONGO_SERVER, MONGO_PORT)
        self.db = self.conn[self.config['app_key']]
        self.datacoll = self.db['data']

    def get(self):
        keysort = self.get_argument('sort', None)
        reverse = self.get_argument('reverse', "true").lower()
        reverse = 1 if reverse == "true" else -1
        limit = self.get_argument('limit', 50)
        start = self.get_argument('start', 0)
        published = self.get_argument('published', None)


        keys = self.request.arguments.keys()
        query = {}
        data_query = []
        for key in keys:
            if key in ['sort', 'start', 'limit',] : continue
            query[key] = {'$in' : self.get_argument(key).split(',')}
            data_query.append("%s=%s" %(key, self.get_argument(key)))
        print query, keysort, reverse
        if keysort:
            data = self.datacoll.find(query).sort(keysort, reverse).skip(int(start)).limit(int(limit))
        else:
            data = self.datacoll.find(query).skip(int(start)).limit(int(limit))
        total = self.datacoll.find(query).count()
        data = list(data)
        d = {'items' : data, 'info' : {'query' : query, 'limit' : int(limit), 'start' : int(start), 'obtained' : len(data), 'total' : total}, 'result' : {'error' : 0, 'msg' : 'ok'}}
        value, mimetype = format_data(d)
        self.set_header('Content-Type', mimetype)
        print value
        self.write(value)


class InfoHandler(BaseAPIHandler):
    def initialize(self, config):
        self.config = config


    def get(self):
        value = self.config
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(value, indent=4))


class ListHandler(BaseAPIHandler):
    def initialize(self, config):
        self.config = config
        self.conn = Connection(MONGO_SERVER, MONGO_PORT)
        self.db = self.conn[self.config['app_key']]
        self.datacoll = self.db['data']

    def get(self):
        format = self.get_argument('format', 'json')
        keysort = self.get_argument('sort', None)
        reverse = self.get_argument('reverse', "true").lower()
        reverse = 1 if reverse == "true" else -1
        keystext = self.get_argument('keys', None)
        if not keystext:
            keys = self.config['fieldtypes'].keys()
        else:
            keys = keystext.split(',')
        if keysort is not None:
            objects = self.datacoll.find(fields=keys).sort(keysort, reverse)
        else:
            objects = self.datacoll.find(fields=keys)
        objects = list(objects)
        d = {'items' : objects, 'info' : {'total' : len(objects)}, 'result' : {'error' : 0, 'msg' : 'ok'}}
        if format == 'csv':
            value, mimetype = format_data(objects, format='csvlist', keys=keys)
        else:
            d = {'items' : objects, 'info' : {'total' : len(objects)}, 'result' : {'error' : 0, 'msg' : 'ok'}}
            value, mimetype = format_data(d)

        self.set_header('Content-Type', mimetype)
        self.write(value)


class ItemHandler(BaseAPIHandler):
    def initialize(self, config):
        self.config = config
        self.conn = Connection(MONGO_SERVER, MONGO_PORT)
        self.db = self.conn[self.config['app_key']]
        self.datacoll = self.db['data']


    def get(self, key):
        format = self.get_argument('format', 'json')
        obj = self.datacoll.find_one({self.config['uniqkey'] : key})
        d = {'item' : obj, 'result' : {'error' : 0, 'msg' : 'ok'}}
        if format == 'csv':
            value, mimetype = format_data(obj, format='csv')
        else:
            value, mimetype = format_data(obj)

        self.set_header('Content-Type', mimetype)
        self.write(value)

class DictHandler(BaseAPIHandler):
    def initialize(self, config):
        self.config = config
        self.conn = Connection(MONGO_SERVER, MONGO_PORT)
        self.db = self.conn[self.config['app_key']]
        self.dictcoll = self.db['dicts']


    def get(self, key):
        format = self.get_argument('format', 'json')
        query = self.dictcoll.find({'dkey': key}).sort('key', 1)
        obj = list(query)
        if format == 'csv':
            value, mimetype = format_data(obj, format='csv')
        else:
            value, mimetype = format_data(obj)

        self.set_header('Content-Type', mimetype)
        self.write(value)



class MainHandler(tornado.web.RequestHandler):
    def initialize(self, config):
        self.config = config


    def get(self):
        from pprint import pprint
        pprint(self.config, self)
#        self.write()



class Application(tornado.web.Application):
    """
    This is out application class where we can be specific about  its
    configuration etc.
    """

    def __init__(self, config_file):
        self.config = json.load(open(config_file, 'r'))
        self.app_key = self.config['app_key']
        self.version = self.config['version']

        if self.version:
            handlers = [
                (r"/%s/v%s/" % (self.app_key, self.version), MainHandler, dict(config=self.config)),
                (r"/%s/v%s/info" % (self.app_key, self.version), InfoHandler, dict(config=self.config)),
                (r"/%s/v%s/query/" % (self.app_key, self.version), QueryHandler, dict(config=self.config)),
                (r"/%s/v%s/dicts/([^/]+)" % (self.app_key, self.version), DictHandler, dict(config=self.config)),
                (r"/%s/v%s/list/" % (self.app_key, self.version), ListHandler, dict(config=self.config)),
                (r"/%s/v%s/key/([^/]+)" % (self.app_key, self.version), ItemHandler, dict(config=self.config)),
            ]

        else:
            handlers = [
                (r"/%s/" % (self.app_key), MainHandler, dict(config=self.config)),
                (r"/%s/info" % (self.app_key), InfoHandler, dict(config=self.config)),
                (r"/%s/query/" % (self.app_key), QueryHandler, dict(config=self.config)),
                (r"/%s/dicts/([^/]+)" % (self.app_key,), DictHandler, dict(config=self.config)),
                (r"/%s/list/" % (self.app_key), ListHandler, dict(config=self.config)),
                (r"/%s/key/([^/]+)" % (self.app_key), ItemHandler, dict(config=self.config)),
            ]

        handlers.append((r"/", MainHandler, dict(config=self.config)),)
        # app settings
        settings = {
            'template_path' : 'templates',
            'static_path' : 'static',
        }
        tornado.web.Application.__init__(self, handlers, **settings)



def prepare(options):
    """Prepares data to run"""
    config = json.load(open(options.config, 'r'))
    conn = Connection(MONGO_SERVER, MONGO_PORT)
    db = conn[config['app_key']]
    # clean up
    db.drop_collection(db['data'])
    db.drop_collection(db['dicts'])
    # reload
    datacoll = db['data']
    dictcoll = db['dicts']
    delimiter = config['delimiter']
    if delimiter:
        if delimiter == '\\t':
            delimiter = '\t'
    else:
        delimiter = '\t'
    delimiter = delimiter.encode('utf8')
    reader = csv.DictReader(open(config['source'], 'r'), delimiter=delimiter)
    i = 0

    # TODO: Data types should be carefully put into mongo.
    # FIXME: All datatypes are strings right now. Should be fixed to guessed data types
    dicts = {}
    for k in config['dictkeys']:
        dicts[k] = {}
    for r in reader:
        i += 1
        datacoll.save(r)
        for k in config['dictkeys']:
            v = dicts[k].get(r[k], 0)
            dicts[k][r[k]] = v + 1
        if i % 100 == 0:
            print 'Loaded %d records' % i

    print "Start loading dicts"
    for dictkey in config['dictkeys']:
        for k, v in dicts[dictkey].items():
            r = {'dkey' : dictkey, 'key': k, 'value' : v}
            dictcoll.save(r)
        print '- loaded dict: "%s"' %(dictkey)
    dictcoll.ensure_index("dkey", 1)
    dictcoll.ensure_index("key", 1)
    dictcoll.ensure_index("value", 1)

    print 'Finished. %d records' % i


def serve(options):
    """Serves the server"""
    config = options.config
    app = Application(config)
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(WEB_PORT)
    ioloop = tornado.ioloop.IOLoop.instance()
    autoreload.start(ioloop)
    ioloop.start()

def analyze(options):
    """Analyzes data CSV file and generates api configuration"""
    print options
    delimiter = options.delimiter
    config = options.config
    dictshare = options.dictshare
    if dictshare and dictshare.isdigit():
        dictshare = int(dictshare)
    else:
        dictshare = DEFAULT_DICT_SHARE

    if delimiter:
        if delimiter == '\\t':
            delimiter = '\t'
    else:
        delimiter = '\t'
    profile = {'version': None, 'app_key' : options.source.rsplit('/', 1)[-1].rsplit('.', 1)[0],
               'source': options.source, 'format' : options.format, 'delimiter' : options.delimiter}
    fielddata = {}
    fieldtypes = {}
    reader = csv.DictReader(open(options.source, 'r'), delimiter=delimiter)
    count = 0
    nfields = 0
    for r in reader:
        count += 1
        nfields = len(r)
        for k, v in r.items():
            if k not in fielddata.keys():
                fielddata[k] = {'key' : k, 'uniq' : {}, 'n_uniq' : 0, 'total': 0, 'share_uniq' : 0.0}
            fd = fielddata[k]
            uniqval = fd['uniq'].get(v, 0)
            fd['uniq'][v] = uniqval + 1
            fd['total'] += 1
            if uniqval == 0:
                fd['n_uniq'] += 1
                fd['share_uniq'] = (fd['n_uniq'] * 100.0) / fd['total']
            fielddata[k] = fd

            if k not in fieldtypes.keys():
                fieldtypes[k] = {'key' : k, 'types' : {}}
            fd = fieldtypes[k]
            thetype = guess_datatype(v)['base']
#            print thetype
            uniqval = fd['types'].get(thetype, 0)
            fd['types'][thetype] = uniqval + 1
            fieldtypes[k] = fd
            pass
    profile['count'] = count
    profile['num_fields'] = nfields
    dictkeys = []
    dicts = {}
    print profile
    for fd in fielddata.values():
        print fd['key'], fd['n_uniq'], fd['share_uniq']
        if fd['share_uniq'] < dictshare:
            dictkeys.append(fd['key'])
            dicts[fd['key']] = {'items': fd['uniq'], 'count' : fd['n_uniq'], 'type' : 'str'} #TODO: Shouldn't be "str" by default
#            for k, v in fd['uniq'].items():
#                print fd['key'], k, v
    profile['dictkeys'] = dictkeys
    finfields = {}
#    profile['debug'] = {'fieldtypes' : fieldtypes.copy()}
    for fd in fieldtypes.values():
        fdt = fd['types'].keys()
        if 'empty' in fdt:
            del fd['types']['empty']
        if len(fd['types'].keys()) != 1:
            ftype = 'str'
        else:
            ftype = fd['types'].keys()[0]
        finfields[fd['key']] = ftype

    profile['fieldtypes'] = finfields
    from pprint import pprint
    pprint(profile)
    if config:
        f = open(config, 'w')
        json.dump(profile, f, indent=4)
        f.close()


def main():
    parser = OptionParser()
    parser.add_option("-f", "--format", dest="format",
                      help="file format (csv or json)", metavar="FORMAT")
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    parser.add_option('-s', "--source", dest="source", help="source of the data")
    parser.add_option('-d', "--delimiter", dest="delimiter", help="records delimiter for csv")
    parser.add_option('-c', "--config", dest="config", help="configuration")
    parser.add_option('-i', "--dictshare", dest="dictshare", help="dictionary share")
    parser.add_option('-u', "--update", dest="update", help="Do not rewrite config. Update it")

    (options, args) = parser.parse_args()

    if args[0] == 'analyze':
        analyze(options)
    elif args[0] == 'prepare':
        prepare(options)
    elif args[0] == 'serve':
        serve(options)



if __name__ == "__main__":
    main()
