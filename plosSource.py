import requests

class InvalidPagingConfigError(RuntimeError):
    def __init__(self, message):
        super(RuntimeError, self).__init__(message)




class _SolrPagingIter:
    """ Traditional search paging, most flexible but will
        gradually get slower on each request due to deep-paging

        See graph here:
        http://opensourceconnections.com/blog/2014/07/13/reindexing-collections-with-solrs-cursor-support/
        """
    def __init__(self, query, **options):
        self.current = 0
        self.query = query

        try:
            self.rows = options['rows']
            del options['rows']
        except KeyError:
            self.rows = 0
        try:
            self.fl = options['fl']
            del options['fl']
        except KeyError:
            self.fl = None
        self.options = options
        self.max = None
        self.docs = None

    def __iter__(self):
        r = requests.get('http://api.plos.org/search?q=' + self.query + '&rows=0')
        response = r.json()
        self.max = response['response']['numFound']
        print("Found %s docs" % self.max)
        return self

    def __next__(self):
        if self.docs is not None:
            try:
                return next(self.docs)
            except StopIteration:
                self.docs = None
        if self.docs is None:
            if self.current * self.rows < self.max:
                self.current += 1
                url = 'http://api.plos.org/search?q=' + self.query + '&rows=' + str(self.rows) + "&start=" + str(((self.current - 1) * self.rows))
                if self.fl:
                    url = url + "&fl=" + (','.join(self.fl))
                print(url)
                r = requests.get(url)
                response = r.json()
                self.docs = iter(response['response']['docs'])
                return next(self.docs)
            else:
                raise StopIteration()


SolrDocs = _SolrPagingIter

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('--query',
                        type=str,
                        default='*:*')

    parser.add_argument('--sort',
                        type=str,
                        default='id desc')

    parser.add_argument('--fields',
                        type=str,
                        default='')

    parser.add_argument('--batch_size',
                        type=int,
                        default=500)

    parser.add_argument('--dest',
                        type=argparse.FileType('w'))

    return vars(parser.parse_args())



if __name__ == "__main__":
    args = parse_args()
    solr_fields = args['fields'].split() if args['fields'] else ''
    solr_itr = SolrDocs(args['query'], rows=args['batch_size'], fl=solr_fields)
    destFile = args['dest']

    import json

    numDocs = 0
    for doc in solr_itr:

        destFile.write(json.dumps(doc) + "\n")
        numDocs += 1
        if (numDocs % 1000 == 0):
            print("Wrote %s docs" % numDocs)
    print("Wrote %s docs" % numDocs)
