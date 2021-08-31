import requests
import os
import math
from urllib.parse import quote_plus
import sqlite3 as slt
from MetricsRecord import MetricsRecord
from EnrichmentEntity import EnrichmentEntity

class RelevanceCounter:
    """
       Calculates the relevance of a given entity, based on two factors:
       Wikidata PageRank and the number of exact-match hits an OR query
       using the entity's various language labels retrieves. Two other factors
       are also available: the number of enrichments using the entity's URL found in
       Europeana datastores; and Wikipedia clickstream popularity.


       In terms of process, this class is very simple: while processing each
       entity, it checks to see whether the above metrics are already stored
       in the relevant sqlite database. If so, it retrieves the results; if not,
       it calculates the Europeana-related metrics (enrichment and term count)
       and inserts these into the database for later retrieval.
   """
    
    URI_MARKUP = 'URI_MARKUP'
    QUERY_ENRICHMENT_HITS = "&q=\"" + URI_MARKUP + "\" AND contentTier:(2 OR 3 OR 4)"
    #AGENT = 'agent'
    #PLACE = 'place'
    #TIMESPAN = 'timespan'
    #CONCEPT = 'concept'
    #ORGANIZATION = 'organization'
        
    RANGE_EXTENSION_FACTOR = 10000
     
    def __init__(self, name):
        import HarvesterConfig
        self.config = HarvesterConfig.HarvesterConfig()
        self.db = None
        self.name = name
        self.penalized_entities = []
        with open(os.path.join(os.path.dirname(__file__), 'resources', 'worst_bets.txt')) as wbets:
            for line in wbets.readlines():
                line = line.strip()
                self.penalized_entities.append(line)
        
    def normalize_string(self, normanda):
        import re

        normatus = re.sub("[()[]\",]", " ", normanda.strip())
        normatus = re.sub("\s+", " ", normatus)
        normatus = re.sub(" ", "_", normatus)
        return normatus

    def db_connect(self):
        if(self.db == None):
            self.dbpath = os.path.join(os.path.dirname(__file__), 'db', self.name + ".db")
            self.db = slt.connect(self.dbpath)
    
    def fetch_metrics_from_db(self, entity):
            self.db_connect()
            csr = self.db.cursor()
            entity_id = entity[EnrichmentEntity.ENTITY][EnrichmentEntity.ABOUT]
            csr.execute("SELECT id, wikidata_id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank FROM hits WHERE id='"+ entity_id + "'")
            return csr.fetchone()
    
    #TODO move method to metric importer        
    def get_raw_relevance_metrics(self, entity):
        first_row = self.fetch_metrics_from_db(entity)
        entity_id = entity[EnrichmentEntity.ENTITY][EnrichmentEntity.ABOUT]
        wikipedia_hits = -1 

        if(first_row is not None):
            (_, wikidata_id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank) = first_row
            if(pagerank is None):
                pagerank = self.importer.get_pagerank_from_solr(wikidata_id)
        else:
            # wikipedia_hits is not used anymore
            europeana_enrichment_hits = self.get_enrichment_count(entity_id)
            europeana_string_hits = self.get_label_count(entity[EnrichmentEntity.REPRESENTATION])
            wikidata_id = self.importer.extract_wikidata_uri(entity)
            #TODO import page rank to DB file
            #TODO use MetricsRecord object
            pagerank = self.importer.get_pagerank_from_solr(wikidata_id)
            csr = self.db.cursor()
            csr.execute("INSERT INTO hits(id, wikidata_id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank) VALUES (?, ?, ?, ?, ?, ?)", (entity_id, wikidata_id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank))
            self.db.commit()
        
        label = self.importer.extract_def_label(entity)
        return MetricsRecord(entity_id, label, wikidata_id, europeana_enrichment_hits, europeana_string_hits, wikipedia_hits, pagerank)
               
    def extract_wikidata_identifier(self, wikidata_uri):
        wikidata_identifier = str(wikidata_uri).replace(self.WIKIDATA_PREFFIX, '')
        #print("has wikidata identifier: " + wikidata_identifier)   
        return wikidata_identifier
    
    def load_wikidata_identifiers_for_places(self):
        if (self.wikidata_europeana_mapping is not None):
            return
        self.wikidata_europeana_mapping = {}
        with open(os.path.join(os.path.dirname(__file__), 'resources', 'places_data_wikidata_all.csv'), encoding="UTF-8") as wikidata_mapping:
            for line in wikidata_mapping.readlines():
                line = str(line).split(';', 3)
                europeana_id = line[0]
                wikidata_id = line[1]
                self.wikidata_europeana_mapping[europeana_id] = wikidata_id     
    
    def get_max_metrics(self):
        self.db_connect()
        csr = self.db.cursor()
        csr.execute("SELECT MAX(europeana_enrichment_hits) as meeh, MAX(europeana_string_hits) as mesh, MAX(pagerank) as mpr, MAX(wikipedia_hits) mwph FROM hits")
        first_row = csr.fetchone()
        if(first_row is not None):
            (max_eeh, max_esh, max_pr, max_wphs) = first_row
        
        metrics = {
            "max_enrichment_hits" : max_eeh,
            "max_europeana_string_hits" : max_esh,
            "max_page_rank" : max_pr,
            "max_wikipedia_hits" : max_wphs
        }
        return metrics

    def get_max_pagerank(self):
        self.db_connect()
        csr = self.db.cursor()
        csr.execute("SELECT id, pagerank FROM hits where pagerank = (select max(pagerank) from hits)")
        first_row = csr.fetchone()
        if(first_row is not None):
            (entity_id, max_pr) = first_row
        metrics = {
            "id" : entity_id,
            "max_page_rank" : max_pr,
        }
        return metrics
    
    #def get_enrichment_count(self, uri):
    #    qry = self.config.get_relevance_solr() + self.get_enrichment_count_query(uri)
    #    res = requests.get(qry)
    #    try:
    #        return res.json()['response']['numFound']
    #    except:
    #        return 0
    def get_enrichment_count(self, uri):
        api_search_url = self.get_enrichment_count_query(uri)
        return self.get_total_results(api_search_url)
        

    def get_total_results(self, api_search_url):
        try:
            res = requests.get(api_search_url, verify=False)
            return res.json()['totalResults']
        except Exception as ex:
            print("INFO: cannot collect metric value when using api URL, setting value to 0: " + api_search_url)
            print("API CALL ERROR: " + str(ex))
            return 0
        
    
    def get_enrichment_count_query(self, uri):
        return self.config.get_relevance_api_url() + self.get_enrichment_count_field() + ':"' + uri + '"'
              
    def get_label_count(self, representation):
        all_labels = []
        [all_labels.extend(l) for l in representation['prefLabel'].values()]
        #qry_labels = ["\"" + label + "\"" for label in all_labels]
        #TODO limit the number of pref labels and ensure usage fo default label
        qry_labels = ["\"" + label + "\"" for label in all_labels]
        
        api_search_url = self.build_term_hits_query(qry_labels)
        term_hits = self.get_total_results(api_search_url)
        
        if(term_hits == 0 and len(qry_labels) > 10):
            api_search_url = self.build_term_hits_query(qry_labels[0:10])
            term_hits = self.get_total_results(api_search_url)    
            #if still 0 check logs:    
            #print("cannot get term hits with query: " + api_search_url)
            #    term_hits = 0
        
        return term_hits    

    #generic method for building term hit query, may be overwritten in subclasses
    #def build_term_hits_query(self, lbls):
    #    qs = " OR ".join(lbls)
    #    qry = self.config.get_relevance_solr() + "&q=" + quote_plus(qs)
    #    return qry
    def build_term_hits_query(self, lbls):
        qs = " OR ".join(lbls)
        qry = self.self.config.get_relevance_api_url() + quote_plus(qs)
        return qry
    
        
    def calculate_relevance_score(self, uri, pagerank, eu_enrichment_count, eu_hit_count):
        if(pagerank is None or pagerank < 1): pagerank = 1
        #pagerank = pagerank + 1 # eliminate values > 1
        # two effects: pagerank only boosts values
        # old: no europeana hits drops relevance to zero
        # old: no pagerank value leaves relevance as europeana hits
        # new: no enrichments for this entity found -> use term hits
        # new: use 1+ln(term hits) to reduce the effect of false positives (the search is ambiguous)
        #relevance = 0;
        #for organizations the default enrichment count is set to 1
        if(eu_enrichment_count > 1):
            relevance = eu_enrichment_count * pagerank
        elif(eu_hit_count > 0):
            relevance = (1 + math.log(eu_hit_count, 10)) * pagerank   
        else:    
            return 0
        
        deprecation_factor = 1
        if(id in self.penalized_entities):
            deprecation_factor = 0.5
        normed_relevance = math.floor(math.log(relevance) * 10000) * deprecation_factor
        return normed_relevance

    def calculate_normalized_score(self, pagerank, eu_enrichment_count, eu_hit_count):
        entity_type = self.name
        normalized_pr = self.calculate_normalized_metric_value(entity_type, MetricsRecord.METRIC_PAGERANK, pagerank)
        normalized_eh = self.calculate_normalized_metric_value(entity_type, MetricsRecord.METRIC_ENRICHMENT_HITS, eu_enrichment_count)
        normalized_th = self.calculate_normalized_metric_value(entity_type, MetricsRecord.METRIC_TERM_HITS, eu_hit_count)
        normalized_score = normalized_pr * max(normalized_eh, normalized_th)
                            
        return math.floor(normalized_score * self.RANGE_EXTENSION_FACTOR)
        
    
    def calculate_normalized_metric_value(self, entity_type, metric, metric_value):
        #min value for normalized value = 1
        if(metric_value <= 1):
            return 1
        #TODO check if deprecation list is needed 
        coordination_factor = self.coordination(entity_type, metric)
        normalized_metric_value = 1 + self.trust(metric) * math.log(coordination_factor * metric_value)
        return normalized_metric_value    
    
    def coordination(self, entity_type, metric):
        max_of_metric = max(MetricsRecord.METRIC_MAX_VALS[metric].values()) 
        max__of_metric_for_type = MetricsRecord.METRIC_MAX_VALS[metric][entity_type]   
        #enforce result as float
        return max_of_metric / float(max__of_metric_for_type);
    
    def trust(self, entity_type):
        return MetricsRecord.METRIC_TRUST[entity_type]
    
class AgentRelevanceCounter(RelevanceCounter):

    def __init__(self, importer):
        RelevanceCounter.__init__(self, EnrichmentEntity.TYPE_AGENT)
        self.importer = importer
        self.importer.init_database()
        
    def get_enrichment_count_field(self):
        return 'edm_agent'

class ConceptRelevanceCounter(RelevanceCounter):

    def __init__(self, importer):
        RelevanceCounter.__init__(self, EnrichmentEntity.TYPE_CONCEPT)
        self.importer = importer
        self.importer.init_database()
    
    def get_enrichment_count_field(self):
        return 'skos_concept'

class PlaceRelevanceCounter(RelevanceCounter):

    def __init__(self, importer):
        RelevanceCounter.__init__(self, EnrichmentEntity.TYPE_PLACE)
        self.importer = importer
        self.importer.init_database()

    def get_enrichment_count_field(self):
        return 'edm_place'

class TimespanRelevanceCounter(RelevanceCounter):

    def __init__(self, importer):
        RelevanceCounter.__init__(self, EnrichmentEntity.TYPE_TIMESPAN)
        self.importer = importer
        self.importer.init_database()
        
    def get_enrichment_count_field(self):
        return 'edm_timespan'


class OrganizationRelevanceCounter(RelevanceCounter):

    def __init__(self, importer):
        RelevanceCounter.__init__(self, EnrichmentEntity.TYPE_ORGANIZATION)
        self.importer = importer
        self.importer.init_database()

    #def get_enrichment_count(self, uri):
    #    #TODO add proper implementation of counting items for organizations
    #    enrichmentSearchUrl = self.config.get_relevance_api_url() + 'foaf_organization:' + uri + '"'
    #    print("return default enrichment count 1 for organization: " + uri)
    #    return 1
    
    def get_enrichment_count_field(self):
        return 'foaf_organization'
    
    def build_term_hits_query(self, lbls):
        #solr_term_hit_query = self.config.get_relevance_solr()
        #labels are already quoted
        fielded_query = "PROVIDER:XXXXX OR DATA_PROVIDER:XXXXX OR provider_aggregation_edm_intermediateProvider: XXXXX"
        
        qrs = []
        for lbl in lbls:
            fq = fielded_query.replace('XXXXX', lbl)
            qrs.append(fq)
        fielded_query = "(" + " OR ".join(qrs) + ")"
        fielded_query = quote_plus(fielded_query)
        term_hits_query = self.config.get_relevance_api_url() + fielded_query
        #print(term_hits_query)
        return term_hits_query
    

