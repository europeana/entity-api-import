
class MetricsRecord:
    """
       Contains definitions for the Europeana-related metrics 
    """

    METRIC_PAGERANK = 'pagerank'
    METRIC_ENRICHMENT_HITS = 'europeana_enrichment_hits'
    METRIC_WIKI_HITS = "wikipedia_hits"
    METRIC_TERM_HITS = 'europeana_string_hits'
    
    #URI_MARKUP = 'URI_MARKUP'
    #QUERY_ENRICHMENT_HITS = "&q=\"" + URI_MARKUP + "\" AND contentTier:(2 OR 3 OR 4)"
    AGENT = 'agent'
    PLACE = 'place'
    CONCEPT = 'concept'
    ORGANIZATION = 'organization'
    #WIKIDATA_PREFFIX = 'http://www.wikidata.org/entity/'
    #WIKIDATA_DBPEDIA_PREFIX = 'http://wikidata.dbpedia.org/resource/'
        
    #wikidata_europeana_mapping = None
    METRIC_MAX_VALS = {
        METRIC_PAGERANK : {
            AGENT : 1204,
            PLACE : 24772,
            CONCEPT : 4055,
            ORGANIZATION : 244
            },
        METRIC_ENRICHMENT_HITS : {
            AGENT : 31734,
            PLACE : 3065416,
            CONCEPT : 1448506,
            ORGANIZATION : 1
            },
        METRIC_TERM_HITS : {
            AGENT : 2297502,
            PLACE : 24576199,
            CONCEPT : 8106790,
            ORGANIZATION : 8977503
            }
        }
    
    METRIC_TRUST = {
        METRIC_PAGERANK : 10,
        METRIC_ENRICHMENT_HITS : 2,
        METRIC_TERM_HITS : 1
    }
    
    RANGE_EXTENSION_FACTOR = 10000
    
    metrics = {}
     
    def __init__(self, entity_id, label, wikidata_id = None, uri_hits=0, term_hits=0, wpd_hits=0, pagerank=0):
        self.id = entity_id
        self.def_label = label
        self.wikidata_id = wikidata_id
        self.uri_hits = uri_hits
        self.term_hits = term_hits
        self.wpd_hits = wpd_hits
        self.pagerank = pagerank
        self.all_labels = []
             
    def get_metrics(self):
        return self.metrics
    
    def set_metrics(self, _metrics):
        self.metrics = _metrics

