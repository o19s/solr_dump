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
                        type=lambda x: (str(x).lower() in ['true','1', 'yes']))

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

    numDocs = 0
    for docLine in docs:
        numDocs += 1
        srcDoc = json.loads(docLine)
        if args['trim'] == True:
            srcDoc.pop('_version_',None)

        doc = {}
        if fields:
            for field in fields:
                if field in srcDoc:
                    doc[field] = srcDoc[field]
        else:
            doc = srcDoc
        commit = True if (numDocs % 500) == 0 else False
        solr_conn.add([doc],
                      commit=commit)
        if (numDocs % 1000 == 0):
            print("Uploaded %s docs" % numDocs)

    solr_conn.add([], commit=True)
