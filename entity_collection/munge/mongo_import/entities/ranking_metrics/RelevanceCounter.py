import requests
import os
import math
from urllib.parse import quote_plus
import sqlite3 as slt

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

    METRIC_PAGERANK = 'pagerank'
    METRIC_ENRICHMENT_HITS = 'europeana_enrichment_hits'
    METRIC_WIKI_HITS = "wikipedia_hits"
    METRIC_TERM_HITS = 'europeana_string_hits'
    
    URI_MARKUP = 'URI_MARKUP'
    QUERY_ENRICHMENT_HITS = "&q=\"" + URI_MARKUP + "\" AND contentTier:(2 OR 3 OR 4)"
    AGENT = 'agent'
    PLACE = 'place'
    CONCEPT = 'concept'
    ORGANIZATION = 'organization'
    WIKIDATA_PREFFIX = 'http://www.wikidata.org/entity/'
    WIKIDATA_DBPEDIA_PREFIX = 'http://wikidata.dbpedia.org/resource/'
        
    wikidata_europeana_mapping = None
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
        self.init_database()
                
    def init_database(self):
        #conn = sqlite3.connect(self.database)
        self.db_connect()
        csr = self.db.cursor()
        try:
            DB_CREATE_STATEMENT = "CREATE TABLE IF NOT EXISTS hits (id VARCHAR(200) PRIMARY KEY, wikidata_id VARCHAR(400), wikipedia_hits INTEGER, europeana_enrichment_hits INTEGER, europeana_string_hits INTEGER, pagerank DOUBLE)"
            csr.execute(DB_CREATE_STATEMENT)
            csr = self.db.commit()
        except slt.OperationalError as error:
            print("cannot initialize database: " + str(error))
        
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
        
    def get_raw_relevance_metrics(self, uri, entity):
        self.db_connect()
        csr = self.db.cursor()
        csr.execute("SELECT id, wikidata_id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank FROM hits WHERE id='"+ uri + "'")
        first_row = csr.fetchone()
                
        if(first_row is not None):
            (_, wikidata_id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank) = first_row
            if(pagerank is None):
                pagerank = 0
        else:
            # wikipedia_hits is not used anymore
            wikipedia_hits = -1 
            europeana_enrichment_hits = self.get_enrichment_count(uri)
            europeana_string_hits = self.get_label_count(entity['representation'])
            wikidata_id = self.extract_wikidata_uri(entity)
            #TODO import page rank to DB file
            #TODO use MetricRecord object
            pagerank = 0.0
            csr.execute("INSERT INTO hits(id, wikidata_id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank) VALUES (?, ?, ?, ?, ?, ?)", (uri, wikidata_id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank))
            self.db.commit()
        metrics = {
            self.METRIC_WIKI_HITS : wikipedia_hits,
            self.METRIC_ENRICHMENT_HITS : europeana_enrichment_hits,
            self.METRIC_TERM_HITS : europeana_string_hits,
            self.METRIC_PAGERANK : pagerank
        }
        return metrics

    def extract_wikidata_uri(self, entity):
        wikidata_uri = None
        #places do not have the wikidata id in same as
        if self.PLACE == self.name:
            wikidata_uri = self.get_wikidata_uri_for_place(entity)        
        else: 
            representation = entity['representation']
            if('owlSameAs' in representation.keys()):
                for uri in representation['owlSameAs']:
                    if (uri.startswith(self.WIKIDATA_PREFFIX)):
                        wikidata_uri = uri
                        #print("has wikidata uri: " + str(wikidata_uri))
                        break
                    elif (uri.startswith(self.WIKIDATA_DBPEDIA_PREFIX)):
                        wikidata_uri= str(wikidata_uri).replace(self.WIKIDATA_DBPEDIA_PREFIX, self.WIKIDATA_PREFFIX)
                        #print("has wikidata uri: " + str(wikidata_uri))
        return wikidata_uri
    
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
    
    def get_wikidata_uri_for_place(self, entity):
        self.load_wikidata_identifiers_for_places()
        wikidata_id = None
        if (self.wikidata_europeana_mapping is not None):
            europeana_id = entity['codeUri']
            if (europeana_id in self.wikidata_europeana_mapping.keys()):
                wikidata_id = self.wikidata_europeana_mapping[europeana_id]
        
        return wikidata_id
    
    
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
    
    def get_enrichment_count(self, uri):
        qry = self.config.get_relevance_solr() + self.get_enrichment_count_query(uri)
        res = requests.get(qry)
        try:
            return res.json()['response']['numFound']
        except:
            return 0

    def get_enrichment_count_query(self, uri):
        return str(self.QUERY_ENRICHMENT_HITS).replace(self.URI_MARKUP, uri)
              
    def get_label_count(self, representation):
        all_labels = []
        [all_labels.extend(l) for l in representation['prefLabel'].values()]
        #qry_labels = ["\"" + label + "\"" for label in all_labels]
        #TODO limit the number of pref labels and ensure usage fo default label
        qry_labels = ["\"" + label + "\"" for label in all_labels]
        term_hits_query = self.build_term_hits_query(qry_labels)
        
        try:
            res = requests.get(term_hits_query)
            return res.json()['response']['numFound']
        #except:
        #    print("Term hits computation failed for request: " + qry)
        #    return 0
        except (ValueError, KeyError):
            #response parsing or retrieval errors
            #TODO: fix too long queries issue
            #print("cannot parse response for query: ")
            #print(term_hits_query)
            if(len(qry_labels) > 10):
                try:
                    term_hits_query = self.build_term_hits_query(qry_labels[0:10])
                    th_as_json = requests.get(term_hits_query).json()
                    term_hits = th_as_json['response']['numFound']
                except (ValueError, KeyError):
                    term_hits = 0    
            else:    
                print("cannot get term hits with query: " + term_hits_query)
                term_hits = 0
        
        return term_hits    

    #generic method for building term hit query, may be overwritten in subclasses
    def build_term_hits_query(self, lbls):
        qs = " OR ".join(lbls)
        qry = self.config.get_relevance_solr() + "&q=" + quote_plus(qs)
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
        normalized_pr = self.calculate_normalized_metric_value(entity_type, self.METRIC_PAGERANK, pagerank)
        normalized_eh = self.calculate_normalized_metric_value(entity_type, self.METRIC_ENRICHMENT_HITS, eu_enrichment_count)
        normalized_th = self.calculate_normalized_metric_value(entity_type, self.METRIC_TERM_HITS, eu_hit_count)
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
        max_of_metric = max(self.METRIC_MAX_VALS[metric].values()) 
        max__of_metric_for_type = self.METRIC_MAX_VALS[metric][entity_type]   
        #enforce result as float
        return max_of_metric / float(max__of_metric_for_type);
    
    def trust(self, entity_type):
        return self.METRIC_TRUST[entity_type]
    
class AgentRelevanceCounter(RelevanceCounter):

    def __init__(self):
        RelevanceCounter.__init__(self, self.AGENT)

class ConceptRelevanceCounter(RelevanceCounter):

    def __init__(self):
        RelevanceCounter.__init__(self, self.CONCEPT)

class PlaceRelevanceCounter(RelevanceCounter):

    def __init__(self):
        RelevanceCounter.__init__(self, self.PLACE)

class OrganizationRelevanceCounter(RelevanceCounter):

    def __init__(self):
        RelevanceCounter.__init__(self, self.ORGANIZATION)

    def get_enrichment_count(self, uri):
        #TODO add proper implementation of counting items for organizations
        print("return default enrichment count 1 for organization: " + uri)
        return 1
    
    def build_term_hits_query(self, lbls):
        solr_term_hit_query = self.config.get_relevance_solr() + "&q=XXXXX"
        #labels are already quoted
        fielded_query = "PROVIDER:XXXXX OR DATA_PROVIDER:XXXXX OR provider_aggregation_edm_intermediateProvider: XXXXX"
        
        qrs = []
        for lbl in lbls:
            fq = fielded_query.replace('XXXXX', lbl)
            qrs.append(fq)
        fielded_query = "(" + " OR ".join(qrs) + ")"
        fielded_query = quote_plus(fielded_query)
        term_hits_query = solr_term_hit_query.replace('XXXXX', fielded_query)
        #print(term_hits_query)
        return term_hits_query
    
    #def get_label_count(self, representation):
    #    #TODO add proper implementation of counting enrichments with organizations
    #    print("return default value for label count: 1")
    #   return 1

