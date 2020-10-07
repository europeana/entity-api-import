from EnrichmentEntity import EnrichmentEntity
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
    #AGENT = 'agent'
    #PLACE = 'place'
    #CONCEPT = 'concept'
    #ORGANIZATION = 'organization'
    WIKIDATA_PREFFIX = 'http://www.wikidata.org/entity/'
    WIKIDATA_DBPEDIA_PREFIX = 'http://wikidata.dbpedia.org/resource/'
        
    #wikidata_europeana_mapping = None
    METRIC_MAX_VALS = {
        METRIC_PAGERANK : {
            EnrichmentEntity.TYPE_AGENT : 1204,
            EnrichmentEntity.TYPE_PLACE : 24772,
            EnrichmentEntity.TYPE_CONCEPT : 4055,
            EnrichmentEntity.TYPE_ORGANIZATION : 244,
            EnrichmentEntity.TYPE_TIMESPAN : 3912
            },
        METRIC_ENRICHMENT_HITS : {
            EnrichmentEntity.TYPE_AGENT : 31734,
            EnrichmentEntity.TYPE_PLACE : 3065416,
            EnrichmentEntity.TYPE_CONCEPT : 1448506,
            EnrichmentEntity.TYPE_ORGANIZATION : 1,
            EnrichmentEntity.TYPE_TIMESPAN : 3383871
            },
        METRIC_TERM_HITS : {
            EnrichmentEntity.TYPE_AGENT : 2297502,
            EnrichmentEntity.TYPE_PLACE : 24576199,
            EnrichmentEntity.TYPE_CONCEPT : 8106790,
            EnrichmentEntity.TYPE_ORGANIZATION : 8977503,
            EnrichmentEntity.TYPE_TIMESPAN : 1
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

