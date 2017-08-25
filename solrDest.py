import pysolr
import json


def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('docs',
                        help='file with JSON solr doc per line',
                        type=argparse.FileType('r'))

    parser.add_argument('--dest',
                        help='solr URL http://localhost:8983/solr/collection1',
                        type=str)

    parser.add_argument('--batch_size',
                        type=int,
                        help='Commit every <batch_size> documents',
                        default=500)

    parser.add_argument('--trim',
                        help='remove system fields like _version_ pulled from Solr',
                        type=bool,
                        default=True)

    parser.add_argument('--fields',
                        help='comma delim fields to index, if blank all fields indexed',
                        type=str,
                        default=None)

    return vars(parser.parse_args())


if __name__ == "__main__":
    args = parse_args()
    solr_conn = pysolr.Solr(args['dest'])
    docs = args['docs']
    trim = args['trim']
    fields = args['fields'].split(',') if args['fields'] else None
    commitEvery = args['batch_size']

    idx = 0
    for docLine in docs:
        idx += 1
        srcDoc = json.loads(docLine)
        if args['trim']:
            del srcDoc['_version_']

        doc = {}
        if fields:
            for field in fields:
                if field in srcDoc:
                    doc[field] = srcDoc[field]
        else:
            doc = srcDoc
        commit = True if (idx % 500) == 0 else False
        solr_conn.add([doc],
                      commit=commit)
    solr_conn.add([], commit=True)
