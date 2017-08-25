import pysolr

class InvalidPagingConfigError(RuntimeError):
    def __init__(self, message):
        super(RuntimeError, self).__init__(message)


class _SolrCursorIter:
    """ Cursor-based iteration, most performant. Requires a sort on id somewhere
        in required "sort" argument.

        This is recommended approach for iterating docs in a Solr collection
        """
    def __init__(self, solr_conn, query, sort='id desc', **options):
        self.query = query
        self.solr_conn = solr_conn
        self.lastCursorMark = ''
        self.cursorMark = '*'
        self.sort = sort

        try:
            self.rows = options['rows']
            del options['rows']
        except KeyError:
            self.rows = 0
        self.options = options
        self.max = None
        self.docs = None

    def __iter__(self):
        response = self.solr_conn.search(self.query, rows=0, **self.options)
        self.max = response.hits
        return self

    def __next__(self):
        try:
            if self.docs is not None:
                try:
                    return next(self.docs)
                except StopIteration:
                    self.docs = None
            if self.docs is None:

                if self.lastCursorMark != self.cursorMark:
                    response = self.solr_conn.search(self.query, rows=self.rows,
                                                     cursorMark=self.cursorMark,
                                                     sort=self.sort,
                                                     **self.options)
                    self.docs = iter(response.docs)
                    self.lastCursorMark = self.cursorMark
                    self.cursorMark = response.nextCursorMark
                    return next(self.docs)
                else:
                    raise StopIteration()
        except pysolr.SolrError as e:
            print(e)
            if "Cursor" in e.message:
                raise InvalidPagingConfigError(e.message)
            raise e


class _SolrPagingIter:
    """ Traditional search paging, most flexible but will
        gradually get slower on each request due to deep-paging

        See graph here:
        http://opensourceconnections.com/blog/2014/07/13/reindexing-collections-with-solrs-cursor-support/
        """
    def __init__(self, solr_conn, query, **options):
        self.current = 0
        self.query = query
        self.solr_conn = solr_conn
        try:
            self.rows = options['rows']
            del options['rows']
        except KeyError:
            self.rows = 0
        self.options = options
        self.max = None
        self.docs = None

    def __iter__(self):
        response = self.solr_conn.search(self.query, rows=0, **self.options)
        self.max = response.hits
        return self

    def __next__(self):
        if self.docs is not None:
            try:
                return self.docs.next()
            except StopIteration:
                self.docs = None
        if self.docs is None:
            if self.current * self.rows < self.max:
                self.current += 1
                response = self.solr_conn.search(self.query, rows=self.rows,
                                                 start=(self.current - 1) * self.rows,
                                                 **self.options)
                self.docs = iter(response.docs)
                return next(self.docs)
            else:
                raise StopIteration()

SolrDocs = _SolrCursorIter # recommended, see note for SolrCursorIter
SlowSolrDocs = _SolrPagingIter

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('solr_url',
                        type=str)

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
    solr_conn = pysolr.Solr(args['solr_url'])
    solr_fields = args['fields'].split() if args['fields'] else ''
    solr_itr = SolrDocs(solr_conn, args['query'], rows=args['batch_size'], sort=args['sort'], fl=solr_fields)
    destFile = args['dest']

    import json

    numDocs = 0
    for doc in solr_itr:
        destFile.write(json.dumps(doc) + "\n")
        numDocs += 1
        if (numDocs % 1000 == 0):
            print("Wrote %s docs" % numDocs)
    print("Wrote %s docs" % numDocs)
