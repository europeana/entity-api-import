import requests
import json
import sqlite3
import urllib3
from pymongo import MongoClient
from entities import HarvesterConfig
from entities.ContextClassHarvesters import ContextClassHarvester, ConceptHarvester, AgentHarvester, PlaceHarvester
from entities.ranking_metrics import RelevanceCounter 
from urllib.parse import quote
from _ast import If
from symbol import for_stmt

# purpose of script is to populate the ranking metrics for Concepts
# Right now we need URI hits, term hits, and PageRank
# (Wikipedia hits are now deprecated as a ranking sigal)

# first, we need to grab all Organization identifiers from Mongo, as well as their
# @en labels 
DB_CONCEPT = "./db/concept.db" 	
DB_PLACE = "./db/place.db" 	
DB_AGENT = "./db/agent.db" 	

TYPE_CONCEPT = "ConceptImpl" 	
TYPE_PLACE = "PlaceImpl" 	
TYPE_AGENT = "AgentImpl" 	

PR_URI_PREFIX = "http://wikidata.dbpedia.entity/resource/"		
wikidata_endpoint_url = "https://query.wikidata.entity/bigdata/namespace/wdq/sparql?format=json&query="
wikidata_query = "SELECT ?item WHERE { ?item rdfs:label|skos:altLabel 'XXXXX'@en. } limit 1"
#WKDT_PAGE_RANK = './resources/wd_pr_ultimate.tsv' 
WKDT_PAGE_RANK = './resources/wd_pr_test.tsv' 

metric_records = []

class MetricsRecord:

	def __init__(self, entity_id, label, wikidata_id = None, uri_hits=0, term_hits=0, wpd_hits=0, pagerank=0):
		self.id = entity_id
		self.def_label = label
		self.wikidata_id = wikidata_id
		self.uri_hits = uri_hits
		self.term_hits = term_hits
		self.wpd_hits = wpd_hits
		self.pagerank = pagerank
		self.all_labels = []

class MetricsImporter:

	def __init__(self, harvester, database, entity_type):
		self.config = HarvesterConfig.HarvesterConfig()
		self.mongo = MongoClient(self.config.get_mongo_host(), self.config.get_mongo_port())
		self.database = database
		self.entity_type = entity_type
		self.harvester = harvester
		self.wkdt_identifiers = []
		self.pageranks = {}
		self.BATCH_SIZE = 300
		self.entity_count = -1
		self.metric_records = []
		
		
	def import_metrics(self):
		print("start importing metrics for entity type:" + self.entity_type) 
		#ensure database is initialized
		self.init_database()
		
		#get entity count
		self.entity_count = self.get_entity_count()
		
		#load page ranks
		self.load_wikidata_identifiers()
		
		#load page ranks
		self.load_page_ranks()
		
		#store metrics to db
		self.store_metrics()
						
	def store_metrics(self):
		batch = 0
		start = 0	
		while (start < self.entity_count):
			print("start storing metrics:" + str(start))
			#create OrgRecords
			#self.mongo.annocultor_db.TermList.find({ "entityType" : self.entity_type})
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
	
	
	def fetch_metrics(self, entity):
		#if( i == 10):
		#	break
		entity_id = entity['codeUri']
		representation =  entity[ContextClassHarvester.REPRESENTATION]
		#used only for easy record identification
		label = self.extract_def_label(entity)
			
		# id, label, wikidata_id, uri_hits, term_hits, wpd_hits, pagerank
		record = MetricsRecord(entity_id, label)
		record.all_labels = self.extract_all_labels(entity)
			
		#saves values in the database, but not page rank and labels
		metrics = self.harvester.relevance_counter.get_raw_relevance_metrics(entity_id, representation)
		#page rank
		record.wikidata_id = harvester.relevance_counter.extract_wikidata_identifier(entity)
		record.uri_hits = metrics[harvester.relevance_counter.METRIC_ENRICHMENT_HITS]
		record.term_hits = metrics[harvester.relevance_counter.METRIC_TERM_HITS]
		record.wpd_hits = metrics[harvester.relevance_counter.METRIC_WIKI_HITS]
		record.pagerank = self.get_page_rank(record.wikidata_id, self.pageranks)
		return record
		
	def store_metric_records(self, metric_records):
		conn = sqlite3.connect(self.database)
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

	def init_database(self):
		conn = sqlite3.connect(self.database)
		csr = conn.cursor()
		DB_CREATE_STATEMENT = "CREATE TABLE IF NOT EXISTS hits (id VARCHAR(200) PRIMARY KEY, wikidata_id VARCHAR(400), wikipedia_hits INTEGER, europeana_enrichment_hits INTEGER, europeana_string_hits INTEGER, pagerank DOUBLE)"
		csr.execute(DB_CREATE_STATEMENT)
		conn.commit()
		
	
	def extract_all_labels (self, term_list):
		lbls = []
		#TODO filter to use only labels in European languages (use boolean method param)
		for lv in term_list['representation']['prefLabel']:
			[lbls.append(lbl) for lbl in term_list['representation']['prefLabel'][lv]]
		#pref labels are not mandatory
		if('altLabel' in term_list['representation'].keys()):
			for lv in term_list['representation']['altLabel']:
				try:
					[lbls.append(lbl) for lbl in term_list['representation']['altLabel'][lv]]
				except KeyError:
					pass	
		return lbls	

	def	extract_def_label(self, term_list):	
		#en_label = entity['representation']['prefLabel']['en'][0]
		label = 'Not available'
		pref_label = term_list['representation']['prefLabel']
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

	def get_page_rank(self, wikidata_id, all_pageranks):
		pagerank = 0.0
		#return default value
		if(wikidata_id is None):
			return pagerank
		try:
			pagerank = float(all_pageranks[wikidata_id].strip())
			#print("found wikidata page rank for identifier:" + wikidata_id)			
		except (IndexError, KeyError, ValueError):
			#response parsing or value retrieval errors
			print("No page rank found for identifier:" + wikidata_id)

		return pagerank

	def load_page_ranks(self):
		if(len(self.wkdt_identifiers) == 0):
			return
		# process pagerank file, grab relevant items
		with open(WKDT_PAGE_RANK) as ult:
			for line in ult.readlines():
				(identifier, pr) = line.split("\t")
				wkdt_identifier = identifier.replace(PR_URI_PREFIX, '')
				#keep in memory only the EC organizations
				if(wkdt_identifier in self.wkdt_identifiers):
					self.pageranks[wkdt_identifier] = pr		
		
	
	def load_wikidata_identifiers(self):
		batch = 0
		start = 0	
		while (start < self.entity_count):
			print(str(start))
			#create OrgRecords
			#self.mongo.annocultor_db.TermList.find({ "entityType" : self.entity_type})
			entities = self.fetch_entity_batch(start)
			
			#build metric records
			for entity in entities:
				wkdt_identifier = self.harvester.relevance_counter.extract_wikidata_identifier(entity['representation'])
				if(wkdt_identifier is not None):
					self.wkdt_identifiers.append(wkdt_identifier)

			#initialize next batch
			batch += 1
			start = batch * self.BATCH_SIZE
		
	def fetch_entity_batch(self, start):
		entities = self.mongo.annocultor_db.TermList.find({ "entityType" : self.entity_type})
		#entities.batch_size(batch_size)
		entities.skip(start);
		entities.limit(self.BATCH_SIZE);
		return entities
		
	def get_entity_count(self):
		return self.mongo.annocultor_db.TermList.count({ "entityType" : self.entity_type})
		
		
#run import scripts
#read page rank once, this is an expensive information
harvester = ConceptHarvester()
importer = MetricsImporter(harvester, DB_CONCEPT, TYPE_CONCEPT)
importer.import_metrics()

harvester = PlaceHarvester()
importer = MetricsImporter(harvester, DB_PLACE, TYPE_PLACE)
importer.import_metrics()

harvester = AgentHarvester()
importer = MetricsImporter(harvester, DB_AGENT, TYPE_AGENT)
importer.import_metrics()
	



