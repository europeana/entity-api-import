import requests
import json
import sqlite3
import urllib3
from pymongo import MongoClient
from entities import HarvesterConfig
from urllib.parse import quote

# purpose of script is to populate the ranking metrics for Organizations
# Right now we need URI hits, term hits, and PageRank
# (Wikipedia hits are now deprecated as a ranking sigal)

# first, we need to grab all Organization identifiers from Mongo, as well as their
# @en labels 
DB_ORGANIZATION = "./db/organization.db" 	
PR_URI_PREFIX = "http://wikidata.dbpedia.entity/resource/"		
wikidata_endpoint_url = "https://query.wikidata.entity/bigdata/namespace/wdq/sparql?format=json&query="
wikidata_query = "SELECT ?item WHERE { ?item rdfs:label|skos:altLabel 'XXXXX'@en. } limit 1"
WKDT_PAGE_RANK = './resources/wd_pr_ultimate.tsv' 
#WKDT_PAGE_RANK = './resources/wd_pr_test.tsv' 

metric_records = []

class MetricsRecord:

	def __init__(self, uri, label):
		self.id = uri
		self.wikidata_id = None
		self.def_label = label
		self.uri_hits = 0
		self.term_hits = 0
		self.wpd_hits = 0
		self.pagerank = 0.0
		self.all_labels = []


def	extract_def_label(term_list):	
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
		

def extract_all_labels (term_list):
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

def extract_wikidata_identifier(representation):
	wikidata_id = None
	WIKIDATA_PREFFIX = 'http://www.wikidata.entity/entity/'
			
	if('owlSameAs' in representation['representation'].keys()):
		for uri in representation['representation']['owlSameAs']:
			if(uri.startswith(WIKIDATA_PREFFIX)):
				wikidata_id = str(uri).replace(WIKIDATA_PREFFIX, '')
				print("has wikidata identifier: " + wikidata_id)
				break
	return wikidata_id

def search_wikidata_id(org):
	lbl = org.def_label
	#TODO expand to use EU labels 
	now_query = wikidata_query.replace('XXXXX', lbl)
	wikidata_req = wikidata_endpoint_url + now_query
	as_json = requests.get(wikidata_req).json()
	wikidata_id = as_json['results']['bindings'][0]['item']['value'].split("/")[-1]
	return wikidata_id

def get_page_rank(wikidata_id, all_pageranks):
	try:
		pagerank = float(all_pageranks[wikidata_id].strip())
		#print("found wikidata page rank for identifier:" + wikidata_id)			
	except (IndexError, KeyError, ValueError):
		#response parsing or value retrieval errors
		print("No page rank found for identifier:" + wikidata_id)
		pagerank = 0.0
	return pagerank

def build_term_hits_query(lbls):
	solr_term_hit_query = config.get_relevance_solr() + "&q=XXXXX"
	fielded_query = "PROVIDER:\"XXXXX\" OR DATA_PROVIDER:\"XXXXX\" OR provider_aggregation_edm_intermediateProvider: \"XXXXX\""
	
	qrs = []
	for lbl in lbls:
		fq = fielded_query.replace('XXXXX', lbl)
		qrs.append(quote(fq))
	fielded_query = "(" + " OR ".join(qrs) + ")"
	term_hits_query = solr_term_hit_query.replace('XXXXX', fielded_query)
	#print(term_hits_query)
	return term_hits_query

def compute_term_hits(lbls):	
	term_hits_query = build_term_hits_query(lbls)
	try:
		th_as_json = requests.get(term_hits_query).json()
		term_hits = th_as_json['response']['numFound']
	except (ValueError, KeyError):
		#response parsing or retrieval errors
		#TODO: fix too long queries issue
		#print("cannot parse response for query: ")
		#print(term_hits_query)
		if(len(lbls) > 10):
			try:
				term_hits_query = build_term_hits_query(lbls[0:10])
				th_as_json = requests.get(term_hits_query).json()
				term_hits = th_as_json['response']['numFound']
			except (ValueError, KeyError):
				term_hits = 0	
		else:	
			print("cannot get term hits with query: " + term_hits_query)
			term_hits = 0
	
	return term_hits

def compute_enrichment_hits(orgid):
	#enable when orgnaization enrichments will be used 
	#solr_enrichment_hit_query = config.get_relevance_solr() + "&q=\"XXXXX\""
	
	#enrich_hits_query = solr_enrichment_hit_query.replace('XXXXX', orgid)
	#enrich_as_json = requests.get(enrich_hits_query).json()	
	#default value
	enrich_hits = 1	
	
	#try:
	#	enrich_hits = enrich_as_json['response']['numFound']
	#except KeyError:
	#	print(enrich_as_json)			
	return enrich_hits

def store_metrics(metric_records):
	conn = sqlite3.connect(DB_ORGANIZATION)
	csr = conn.cursor()
	csr.execute("""
            CREATE TABLE IF NOT EXISTS hits (id VARCHAR(200) PRIMARY KEY, wikipedia_hits INTEGER, europeana_enrichment_hits INTEGER, europeana_string_hits INTEGER, pagerank DOUBLE)
        """)
	for orgr in metric_records: #TODO switch to insert or update
		#vals = [str("\"" + metric_record.id + "\""), str(metric_record.wpd_hits), str(metric_record.uri_hits), str(metric_record.term_hits), str(metric_record.pagerank)]
		#instatement = "INSERT OR REPLACE INTO hits VALUES(" + ",".join(vals) + ")"
		#print(instatement)
		#csr.execute(instatement)
		
		try:
			csr.execute("INSERT INTO hits(id, wikipedia_hits, europeana_enrichment_hits, europeana_string_hits, pagerank) VALUES (?, ?, ?, ?, ?)", 
				(orgr.id, orgr.wpd_hits, orgr.uri_hits, orgr.term_hits, orgr.pagerank))

		except sqlite3.IntegrityError:
			# if hit already registered print()
			pass
	conn.commit()

# process pagerank file, grab relevant items
def read_wkdt_page_rank():
	with open(WKDT_PAGE_RANK) as ult:
		i = 0;
		for line in ult.readlines():
			print(line)
			i+=1
			#keep in memory only the EC organizations
			if(i==10):
				break
					
####  start processing
#read organizations from enrichment database
config = HarvesterConfig.HarvesterConfig()
mongo = MongoClient(config.get_mongo_host('organizations'), config.get_mongo_port())
entities = mongo.annocultor_db.TermList.find({ "entityType" : "OrganizationImpl"})

#create OrgRecords
wkdt_identifiers = []
for entity in entities:
	org_id = entity['codeUri']
	label = extract_def_label(entity) 		
	record = MetricsRecord(org_id, label)
	record.wikidata_id = extract_wikidata_identifier(entity)
	if(record.wikidata_id is not None):
		wkdt_identifiers.append(record.wikidata_id)
	record.all_labels = extract_all_labels(entity)
	metric_records.append(record)

pageranks = {}
	
# process pagerank file, grab relevant items
with open(WKDT_PAGE_RANK) as ult:
	for line in ult.readlines():
		(identifier, pr) = line.split("\t")
		wkdt_identifier = identifier.replace(PR_URI_PREFIX, '')
		#keep in memory only the EC organizations
		if(wkdt_identifier in wkdt_identifiers):
			pageranks[wkdt_identifier] = pr		

# fetch metrics into MetricsRecord
for metric_record in metric_records:
	if(metric_record.wikidata_id is not None):
		metric_record.pagerank = get_page_rank(metric_record.wikidata_id, pageranks)
	else:
		metric_record.pagerank = 0.0
	#all organizations are known and have at least one record, when enrichment is complete the correct values will be used automatically 
	metric_record.uri_hits = max(compute_enrichment_hits(metric_record.id), 1) 
	metric_record.term_hits = compute_term_hits(metric_record.all_labels)

#finally store metrics to database
store_metrics(metric_records)

