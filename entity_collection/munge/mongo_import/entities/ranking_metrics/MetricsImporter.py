import requests
#import json
import sqlite3
#import urllib3
import os
from pymongo import MongoClient
from HarvesterConfig import HarvesterConfig
#from entities.ContextClassHarvesters import ContextClassHarvester, ConceptHarvester, AgentHarvester, PlaceHarvester, OrganizationHarvester
##from RelevanceCounter import RelevanceCounter
from MetricsRecord import MetricsRecord
from EnrichmentEntity import EnrichmentEntity

#import MetricsRecord
#from entities.ranking_metrics.RelevanceCounter import RelevanceCounter 
#from urllib.parse import quote
#from _ast import If
#from symbol import for_stmt

#class MetricsRecord:

#    def __init__(self, entity_id, label, wikidata_id = None, uri_hits=0, term_hits=0, wpd_hits=0, pagerank=0):
#        self.id = entity_id
#        self.def_label = label
#        self.wikidata_id = wikidata_id
#        self.uri_hits = uri_hits
#        self.term_hits = term_hits
#        self.wpd_hits = wpd_hits
#        self.pagerank = pagerank
#        self.all_labels = []

class MetricsImporter:

    DB_CONCEPT = "./db/concept.db"     
    DB_PLACE = "./db/place.db"     
    DB_TIMESPAN = "./db/timespan.db"     
    DB_AGENT = "./db/agent.db"
    DB_ORGANIZATION = "./db/organization.db"     

    #TYPE_CONCEPT = "CONCEPT"     
    #TYPE_PLACE = "PLACE"     
    #TYPE_PLACE = "TYPESPAN"     
    #TYPE_AGENT = "AGENT"
    #TYPE_ORGANIZATION = "ORGANIZATION"
     

    PR_URI_PREFIX = "http://wikidata.dbpedia.entity/resource/"        
    wikidata_endpoint_url = "https://query.wikidata.entity/bigdata/namespace/wdq/sparql?format=json&query="
    wikidata_query = "SELECT ?item WHERE { ?item rdfs:label|skos:altLabel 'XXXXX'@en. } limit 1"
    WKDT_PAGE_RANK = './resources/wd_pr_ultimate.tsv' 
    #WKDT_PAGE_RANK = './resources/wd_pr_test.tsv' 

    wikidata_europeana_mapping = None
    
    def __init__(self, harvester, database, entity_type):
        self.config = HarvesterConfig()
        self.mongo = MongoClient(self.config.get_mongo_host(), self.config.get_mongo_port())
        self.database = database
        self.entity_type = entity_type
        self.harvester = harvester
        self.wkdt_uris = []
        self.pageranks = {}
        self.BATCH_SIZE = 1000
        self.entity_count = -1
        self.metric_records = []
        
        
    def import_metrics(self, entity=None):
        print("start importing metrics for entity type:" + self.entity_type) 
        
        entity_id = entity['codeUri']
        wikidata_id = self.extract_wikidata_uri(entity)
        
        #ensure database is initialized
        #database should be initialized in constructors
        #self.init_database()
        
        #get entity count
        if entity_id is not None:
            self.entity_count = 1

            #process pagerank from Solr, grab relevant items
            pr=self.get_pagerank_from_solr(wikidata_id)
            if (pr is not None):
                self.pageranks[wikidata_id] = pr 
        else:
            self.entity_count = self.get_entity_count()
        
            #load page ranks
            self.load_wikidata_uris()
            
            #load page ranks
            self.load_page_ranks()
            
        #store metrics to db
        self.store_metrics()
        return MetricsRecord(entity_id, 'fake label', wikidata_id, -1, -1, -1, pr) 
                        
        
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
    
        
    def extract_wikidata_uri(self, entity):
        wikidata_uri = None
        #places do not have the wikidata id in same as
        if EnrichmentEntity.TYPE_PLACE == self.entity_type:
            wikidata_uri = self.get_wikidata_uri_for_place(entity)        
        else: 
            representation = entity[EnrichmentEntity.REPRESENTATION]
            if('owlSameAs' in representation.keys()):
                for uri in representation['owlSameAs']:
                    if (uri.startswith(MetricsRecord.WIKIDATA_PREFFIX)):
                        wikidata_uri = uri
                        #print("has wikidata uri: " + str(wikidata_uri))
                        break
                    elif (uri.startswith(MetricsRecord.WIKIDATA_DBPEDIA_PREFIX)):
                        wikidata_uri= str(wikidata_uri).replace(MetricsRecord.WIKIDATA_DBPEDIA_PREFIX, MetricsRecord.WIKIDATA_PREFFIX)
                        #print("has wikidata uri: " + str(wikidata_uri))
        return wikidata_uri

    
    def import_pagerank(self):
        print("start importing pagerank for entity type:" + self.entity_type) 
        #ensure database is initialized
        self.init_database()
        
        #get entity count
        self.entity_count = self.get_entity_count()
        print("entity count: ", self.entity_count)
        
        #load page ranks
        self.load_wikidata_uris()
        print("entities with wikidata links : ", len(self.wkdt_uris))
        
        #load page ranks
        self.load_page_ranks()
        print("entities with wikidata page rank : ", len(self.pageranks))
        
        #store metrics to db
        self.update_pageranks()
    
            
    def store_metrics(self):
        batch = 0
        start = 0    
        while (start < self.entity_count):
            print("start storing metrics:" + str(start))
            #create OrgRecords
            entities = self.fetch_entity_batch(start)
            
            #build metric records
            for entity in entities:
                record = self.fetch_metrics(entity)
                #add to record list
                self.metric_records.append(record)
            
            #store metric records    
            self.store_metric_records(self.metric_records)
            
            #initialize next batch
            batch += 1
            start = batch * self.BATCH_SIZE
            #clear records list
            self.metric_records = []
    
    def update_pageranks(self):
        batch = 0
        start = 0    
        entity_id = '';
        wikidata_id = '';
        page_rank = 0.0;
        
        while (start < self.entity_count):
            print("start updating pagerank:" + str(start))
            #create OrgRecords
            entities = self.fetch_entity_batch(start)
            
            #build metric records
            for entity in entities:
                #add to record list
                entity_id = entity['codeUri']
                wikidata_id = self.extract_wikidata_uri(entity)
                page_rank = 0.0
                if (wikidata_id in self.pageranks.keys()):
                    page_rank = float(self.pageranks[wikidata_id]) 
                record = MetricsRecord(entity_id, 'fake label', wikidata_id, -1, -1, -1, page_rank)
                self.metric_records.append(record)
            
            #store metric records    
            self.store_page_ranks(self.metric_records)
            
            #initialize next batch
            batch += 1
            start = batch * self.BATCH_SIZE
            #clear records list
            self.metric_records = []
    
    def store_page_ranks(self, metric_records):
        conn = sqlite3.connect(self.database)
        csr = conn.cursor()
        for mr in metric_records: #TODO switch to insert or update
            #vals = [str("\"" + metric_record.id + "\""), str(metric_record.wpd_hits), str(metric_record.uri_hits), str(metric_record.term_hits), str(metric_record.pagerank)]
            #instatement = "INSERT OR REPLACE INTO hits VALUES(" + ",".join(vals) + ")"
            #print(instatement)
            #csr.execute(instatement)
            
            try:
                csr.execute("""UPDATE hits SET wikidata_id=?, pagerank=? WHERE id=?""",  
                    (mr.wikidata_id, mr.pagerank, mr.id))
    
            except sqlite3.IntegrityError:
                # if hit already registered print()
                print("update pagerank failed for : " + str(metric_records))
                pass
        conn.commit()
        print("updates page ranks: ", len(metric_records))
        
        
    def fetch_metrics(self, entity):
        #if( i == 10):
        #    break
        entity_id = entity['codeUri']
        #representation =  entity[ContextClassHarvester.REPRESENTATION]
        #used only for easy record identification
        label = self.extract_def_label(entity)
            
        # id, label, wikidata_id, uri_hits, term_hits, wpd_hits, pagerank
        record = MetricsRecord(entity_id, label)
        record.all_labels = self.extract_all_labels(entity)
            
        #saves values in the database, but not page rank and labels
        metrics = self.harvester.relevance_counter.get_raw_relevance_metrics(entity)
        #page rank
        record.wikidata_id = self.extract_wikidata_uri(entity)
        #wikidata_identifier = harvester.relevance_counter.extract_wikidata_identifier(record.wikidata_id)
        
        record.uri_hits = metrics[MetricsRecord.METRIC_ENRICHMENT_HITS]
        record.term_hits = metrics[MetricsRecord.METRIC_TERM_HITS]
        record.wpd_hits = metrics[MetricsRecord.METRIC_WIKI_HITS]
        record.pagerank = self.get_page_rank(record.wikidata_id)
        return record
        
    def store_metric_records(self, metric_records):
        conn = sqlite3.connect(self.dbpath)
        csr = conn.cursor()
        for mr in metric_records: #TODO switch to insert or update
            #vals = [str("\"" + metric_record.id + "\""), str(metric_record.wpd_hits), str(metric_record.uri_hits), str(metric_record.term_hits), str(metric_record.pagerank)]
            #instatement = "INSERT OR REPLACE INTO hits VALUES(" + ",".join(vals) + ")"
            #print(instatement)
            #csr.execute(instatement)
            
            try:
                csr.execute("INSERT OR REPLACE INTO hits(id, wikidata_id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank) VALUES (?, ?, ?, ?, ?, ?)", 
                    (mr.id, mr.wikidata_id, mr.wpd_hits, mr.uri_hits, mr.term_hits, mr.pagerank))
    
            except sqlite3.IntegrityError:
                # if hit already registered print()
                print("insert or replace failed for : " + str(metric_records))
                pass
        print("stored metrics: ", len(metric_records))
        conn.commit()

    #TODO: same initialization as in Relevance Counter, should be refactored
    def init_database(self):
        #conn = sqlite3.connect(self.database)
        self.dbpath = os.path.join(os.path.dirname(__file__), self.database)
        conn = sqlite3.connect(self.dbpath)
        csr = conn.cursor()
        DB_CREATE_STATEMENT = "CREATE TABLE IF NOT EXISTS hits (id VARCHAR(200) PRIMARY KEY, wikidata_id VARCHAR(400), wikipedia_hits INTEGER, europeana_enrichment_hits INTEGER, europeana_string_hits INTEGER, pagerank DOUBLE)"
        csr.execute(DB_CREATE_STATEMENT)
        conn.commit()
        
    
    def extract_all_labels (self, term_list):
        lbls = []
        #TODO filter to use only labels in European languages (use boolean method param)
        for lv in term_list[EnrichmentEntity.REPRESENTATION]['prefLabel']:
            [lbls.append(lbl) for lbl in term_list[EnrichmentEntity.REPRESENTATION]['prefLabel'][lv]]
        #alt labels are not mandatory
        if('altLabel' in term_list[EnrichmentEntity.REPRESENTATION].keys()):
            for lv in term_list[EnrichmentEntity.REPRESENTATION]['altLabel']:
                try:
                    [lbls.append(lbl) for lbl in term_list[EnrichmentEntity.REPRESENTATION]['altLabel'][lv]]
                except KeyError:
                    pass    
        return lbls    

    def extract_def_label(self, term_list):    
        #en_label = entity['representation']['prefLabel']['en'][0]
        label = 'Not available'
        pref_label = term_list[EnrichmentEntity.REPRESENTATION]['prefLabel']
        country_key = None
        if('edmCountry' in term_list.keys()):
            country_key = term_list['edmCountry'].lower()
            
        if('en' in pref_label.keys()):
            label = pref_label['en'][0]
        elif((country_key is not None) and (country_key in pref_label.keys())):
            label = pref_label[country_key][0]
        else:
            label = next(iter(pref_label.values()))[0]
        return label

    def get_page_rank(self, wikidata_id):
        pagerank = 0.0
        #return default value
        if(wikidata_id is None):
            return pagerank
        try:
            pagerank = float(self.all_pageranks[wikidata_id].strip())
            #print("found wikidata page rank for identifier:" + wikidata_id)            
        except (IndexError, KeyError, ValueError):
            #response parsing or value retrieval errors
            print("No page rank found for wikidata id:" + wikidata_id)

        return pagerank

    def load_page_ranks(self):
        if(len(self.wkdt_uris) == 0):
            return
        
        #TODO: retrieve pageranks with one solr query
        for wkdt_uri in self.wkdt_uris:
            self.pageranks[wkdt_uri] = self.get_pagerank_from_solr(wkdt_uri)                    
        
    # this method extracts pagerank from solr for provided uri
    def get_pagerank_from_solr(self, uri):
        if(uri is None):
            return 0
        qry = self.config.get_pagerank_solr() + "/pagerank/select?q=page_url:\"" + uri + "\""
        res = requests.get(qry)
        try:
            return res.json()['response']['docs'][0]['page_rank']
        except:
            return 0
            
    def load_wikidata_uris(self):
        batch = 0
        start = 0    
        while (start < self.entity_count):
            print("load wikidata uris for batch: ", str(start))
            #create OrgRecords
            entities = self.fetch_entity_batch(start)
            
            #build metric records
            for entity in entities:
                wikidata_uri = self.extract_wikidata_uri(entity) 
                if(wikidata_uri is not None):
                    #wkdt_identifier = self.harvester.relevance_counter.extract_wikidata_identifier(wikidata_uri)
                    self.wkdt_uris.append(wikidata_uri)

            #initialize next batch
            batch += 1
            start = batch * self.BATCH_SIZE
        
    def fetch_entity_batch(self, start):
        entities = self.mongo.get_database(HarvesterConfig.DB_ENRICHMENT).get_collection(HarvesterConfig.COL_ENRICHMENT_TERM).find({ "entityType" : self.entity_type})
        #entities.batch_size(batch_size)
        entities.skip(start);
        entities.limit(self.BATCH_SIZE);
        return entities
        
    def get_entity_count(self):
        return self.mongo.get_database(HarvesterConfig.DB_ENRICHMENT).get_collection(HarvesterConfig.COL_ENRICHMENT_TERM).count({ "entityType" : self.entity_type})

