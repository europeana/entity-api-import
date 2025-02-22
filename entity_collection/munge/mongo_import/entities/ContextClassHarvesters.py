import os, sys
from _sqlite3 import connect
import datetime
from preview_builder import PreviewBuilder
from ranking_metrics import RelevanceCounter
# TODO: Refactor to shrink this method
import json
from _struct import error
import DepictionManager
from MetricsImporter import MetricsImporter
from HarvesterConfig import HarvesterConfig
from EnrichmentEntity import EnrichmentEntity
        
class LanguageValidator:

    # TODO: What to do with weird 'def' language tags all over the place?
    LOG_LOCATION = os.path.join(os.path.dirname(__file__), '..', 'logs', 'langlogs')

    def __init__(self):
        self.langmap = {}
        langlistloc = os.path.join(os.path.dirname(__file__), '..', 'all_langs.wkp')
        with open(langlistloc, 'r', encoding="UTF-8") as all_langs:
            for lang in all_langs:
                if(not(lang.startswith("#")) and ("|" in lang)):
                    (name, code) = lang.split('|')
                    self.langmap[code.strip()] = name

    def validate_lang_code(self, entity_id, code):
        if(code in self.langmap.keys()):
            return True
        elif(code == EnrichmentEntity.LANG_DEF):
            # TODO: sort out the 'def' mess at some point
            self.log_invalid_lang_code(entity_id, EnrichmentEntity.LANG_DEF)
            return True
        elif(code == ''):
            self.log_invalid_lang_code(entity_id, 'Empty string')
            return True
        else:
            self.log_invalid_lang_code(entity_id, code)
            return False

    def pure_validate_lang_code(self, code):
        if(code in self.langmap.keys()):
            return True
        elif(code == EnrichmentEntity.LANG_DEF):
            return True
        else:
            return False

    def log_invalid_lang_code(self, entity_id, code):
        # TODO: differentiate logfiles by date
        filename = "logs.txt"
        filepath = LanguageValidator.LOG_LOCATION + filename
        with open(filepath, 'a') as lgout:
            msg = "Invalid language code found on entity " + str(entity_id) + ": " + str(code)
            lgout.write(msg)
            lgout.write("\n")

    def print_langs(self):
        print(self.langmap)


class ContextClassHarvester:

    #DEFAULT_CONFIG_SECTION = 'CONFIG'
    #HARVESTER_MONGO_HOST = 'harvester.mongo.host'
    #HARVESTER_MONGO_PORT = 'harvester.mongo.port'
    
    #ORGHARVESTER_MONGO_HOST = 'organization.harvester.mongo.host'
    #ORGHARVESTER_MONGO_PORT = 'organization.harvester.mongo.port'
    
    LOG_LOCATION = 'logs/entlogs/'
    
    CHUNK_SIZE = 250   # each file will consist of 250 entities
    WRITEDIR = os.path.join(os.path.dirname(__file__), '..', 'entities_out')
    CONFIG_DIR = os.path.join(os.path.dirname(__file__), '..', 'config')
    LANG_VALIDATOR = LanguageValidator()
    
    LABEL = 'label'
    TYPE = 'type'
    TYPE_STRING = 'string'
    TYPE_OBJECT = 'obj'
    TYPE_REF = 'ref'
    PROP_OWL_SAMEAS = 'owlSameAs'
    
    #TODO remove when whole code is switched to use the EnrichmentEntity language constants
    LANG_DEF = EnrichmentEntity.LANG_DEF
    LANG_EN = EnrichmentEntity.LANG_EN
    
    
    IGNORED_PROPS = ['about', '_id', "className", "edmOrganizationSector"]
        
    FIELD_MAP = {
        # maps mongo fields to their solr equivalents
        # TODO: there are numerous fields defined in the schema but not 
        # found in the actual data. They are accordingly not represented here.
        # For a list of all fields that might conceivably exist in accordance
        # with the data model, see https://docs.google.com/spreadsheets/d/
        #           1b1UN27M2eCia0L54di0KQY7KcndTq8-wxzwM4wN-8DU/edit#gid=340708208
        'prefLabel' : { LABEL : 'skos_prefLabel' , TYPE : TYPE_STRING },
        'altLabel' : { LABEL: 'skos_altLabel' , TYPE : TYPE_STRING },
        'hiddenLabel' : { LABEL : 'skos_hiddenLabel', TYPE : TYPE_STRING},
        'edmAcronym' : { LABEL : 'edm_acronym', TYPE : TYPE_STRING},
        'note' : { LABEL: 'skos_note' , TYPE : TYPE_STRING },
        'begin' : { LABEL : 'edm_begin', TYPE : TYPE_STRING},
        'end' : { LABEL : 'edm_end', TYPE : TYPE_STRING}, 
        'owlSameAs' : { LABEL: 'owl_sameAs' , TYPE : TYPE_REF },
        'edmIsRelatedTo' : { LABEL: 'edm_isRelatedTo' , TYPE : TYPE_REF },
        'dcIdentifier' : { LABEL: EnrichmentEntity.DC_IDENTIFIER , TYPE : TYPE_STRING },
        'dcDescription' : { LABEL: 'dc_description' , TYPE : TYPE_STRING },
        'rdaGr2DateOfBirth' : { LABEL: 'rdagr2_dateOfBirth' , TYPE : TYPE_STRING },
        #not used yet
        #'rdaGr2DateOfEstablishment' : { 'label': 'rdagr2_dateOfEstablishment' , TYPE : TYPE_STRING },
        'rdaGr2DateOfDeath' : { LABEL: 'rdagr2_dateOfDeath' , TYPE : TYPE_STRING },
        #not used yet
        #'rdaGr2DateOfTermination' : { 'label': 'rdagr2_dateOfTermination' , TYPE : TYPE_STRING },
        'rdaGr2PlaceOfBirth' : { LABEL: 'rdagr2_placeOfBirth' , TYPE : TYPE_STRING },
        'placeOfBirth' : { LABEL: 'rdagr2_placeOfBirth' , TYPE : TYPE_STRING },
        #not used yet
        #'placeOfBirth_uri' : { 'label': 'rdagr2_placeOfBirth.uri' , TYPE : TYPE_STRING },
        'rdaGr2PlaceOfDeath' : { LABEL: 'rdagr2_placeOfDeath' , TYPE : TYPE_STRING },
        #not used yet
        #'placeOfDeath_uri' : { 'label': 'rdagr2_placeOfDeath.uri' , TYPE : TYPE_STRING },
        'rdaGr2PlaceOfDeath' : { LABEL: 'rdagr2_placeOfDeath' , TYPE : TYPE_STRING },
        #not used yet
        #'professionOrOccupation_uri' : { 'label': 'professionOrOccupation.uri' , TYPE : TYPE_STRING },
        'rdaGr2ProfessionOrOccupation' :  { LABEL: 'rdagr2_professionOrOccupation' , TYPE : TYPE_STRING },
        #not used yet
        #'gender' : { 'label': 'gender' , TYPE : TYPE_STRING },
        'rdaGr2Gender' : { LABEL: 'rdagr2_gender' , TYPE : TYPE_STRING },
        'rdaGr2BiographicalInformation' : { LABEL: 'rdagr2_biographicalInformation' , TYPE : TYPE_STRING },
        'latitude' : { LABEL: 'wgs84_pos_lat' , TYPE : TYPE_STRING },
        'longitude' : { LABEL: 'wgs84_pos_long' , TYPE : TYPE_STRING },
        #not used yet
        #'beginDate' : { 'label': 'edm_beginDate' , TYPE : TYPE_STRING },
        #not used yet
        #'endDate' : { 'label': 'edm_endDate' , TYPE : TYPE_STRING },
        'isPartOf' : { LABEL: 'dcterms_isPartOf' , TYPE : TYPE_REF },
        #edm_isNextInSequence
        'isNextInSequence' : { LABEL: 'edm_isNextInSequence' , TYPE : TYPE_REF },
        'hasPart' : { LABEL : 'dcterms_hasPart', TYPE : TYPE_REF},
        'hasMet' : { LABEL : 'edm_hasMet', TYPE : TYPE_REF },
        'date' : { LABEL : 'dc_date', TYPE : TYPE_STRING },
        'exactMatch': { LABEL :  'skos_exactMatch', TYPE : TYPE_STRING },
        'related' : { LABEL : 'skos_related', TYPE : TYPE_REF  },
        'broader' : { LABEL : 'skos_broader', TYPE : TYPE_REF},
        'narrower' : { LABEL : 'skos_narrower', TYPE : TYPE_REF},
        'related' : { LABEL : 'skos_related', TYPE : TYPE_REF},
        'broadMatch' : { LABEL : 'skos_broadMatch', TYPE : TYPE_REF},
        'narrowMatch' : { LABEL : 'skos_narrowMatch', TYPE : TYPE_REF },
        'relatedMatch' : { LABEL : 'skos_relatedMatch', TYPE : TYPE_REF },
        'exactMatch' : { LABEL : 'skos_exactMatch', TYPE : TYPE_REF },
        'closeMatch' : { LABEL : 'skos_closeMatch', TYPE : TYPE_REF },
        'notation' : { LABEL : 'skos_notation', TYPE : TYPE_REF },
        'inScheme' : { LABEL : 'skos_inScheme', TYPE : TYPE_REF },
        'note' : { LABEL : 'skos_note', TYPE : TYPE_STRING },
        'foafLogo' : { LABEL : 'foaf_logo', TYPE : TYPE_REF },
        'foafDepiction' : { LABEL : 'foaf_depiction', TYPE : TYPE_REF },
        # not used yet
        #name' : { 'label' : 'foaf_name', TYPE : TYPE_STRING },
        'foafHomepage' : { LABEL : 'foaf_homepage', TYPE : TYPE_REF},
        'foafPhone' : { LABEL : 'foaf_phone', TYPE : TYPE_STRING},
        'foafMbox' : { LABEL : 'foaf_mbox', TYPE : TYPE_STRING},
        'edmCountry' : { LABEL : EnrichmentEntity.COUNTRY, TYPE : TYPE_STRING},
        'edmEuropeanaRole' : { LABEL : EnrichmentEntity.EUROPEANA_ROLE, TYPE : TYPE_STRING},
        'edmOrganizationDomain' : { LABEL : EnrichmentEntity.ORGANIZATION_DOMAIN, TYPE : TYPE_STRING},
        #TODO: remove, not supported anymore
        #'edmOrganizationSector' : { 'label' : 'edm_organizationSector', TYPE : TYPE_STRING},
        #'edmOrganizationScope' : { 'label' : 'edm_organizationScope', TYPE : TYPE_STRING},
        'edmGeographicLevel' : { LABEL : EnrichmentEntity.GEOGRAPHIC_LEVEL, TYPE : TYPE_STRING},
        'address' : { LABEL : 'vcard_hasAddress', TYPE : TYPE_OBJECT},
        #not sure if used anymore
        'address_about' : { LABEL : 'vcard_hasAddress', TYPE : TYPE_STRING},
        'vcardStreetAddress' : { LABEL : 'vcard_streetAddress', TYPE : TYPE_STRING},
        'vcardLocality' : { LABEL : 'vcard_locality', TYPE : TYPE_STRING },
        #not used yet
        #'vcardRegion' : { LABEL : 'vcard_region', TYPE : TYPE_STRING },
        'vcardPostalCode' : { LABEL : 'vcard_postalCode', TYPE : TYPE_STRING},
        'vcardCountryName' : { LABEL : 'vcard_countryName', TYPE : TYPE_STRING },
        'vcardPostOfficeBox' : { LABEL : 'vcard_postOfficeBox', TYPE : TYPE_STRING},
        'vcardHasGeo' : { LABEL : 'hasGeo', TYPE : TYPE_STRING}
        
    }

    def log_warm_message(self, entity_id, message):
        # TODO: differentiate logfiles by date
        filename = "warn.txt"
        filepath = LanguageValidator.LOG_LOCATION + filename
        with open(filepath, 'a') as lgout:
            msg = "Warning info on processing entity " + str(entity_id) + ": " + str(message)
            lgout.write(msg)
            lgout.write("\n")

    # TODO: add address processing

    def __init__(self, entity_type):
        sys.path.append(os.path.join(os.path.dirname(__file__)))
        sys.path.append(os.path.join(os.path.dirname(__file__), 'ranking_metrics'))
        sys.path.append(os.path.join(os.path.dirname(__file__), 'preview_builder'))
        
        from pymongo import MongoClient
        #import PreviewBuilder
        #import HarvesterConfig
        
        self.config = HarvesterConfig()
        #TODO: remove field name and use entity type
        self.name = entity_type + 's'
        self.client = MongoClient(self.get_mongo_host())
        self.ranking_model = self.config.get_relevance_ranking_model()
        self.write_dir = ContextClassHarvester.WRITEDIR + "/" + self.ranking_model
        #TODO create working dir here, including folders for individual entities and organization type
        self.entity_type = entity_type
        self.preview_builder = PreviewBuilder.PreviewBuilder(self.client, entity_type)
        self.depiction_manager = DepictionManager.DepictionManager(self.config)
        
    def get_mongo_host (self):
        #return default mongo host, the subclasses may use the type based config (e.g. see organizations)
        return self.config.get_mongo_host() 
        
    #def get_mongo_port (self):
        #return default mongo port, the subclasses may use the type based config (e.g. see also organizations host)
        #return self.config.get_mongo_port()
    
    def get_entity_count(self):
        entities = self.client.get_database(HarvesterConfig.DB_ENRICHMENT).get_collection(HarvesterConfig.COL_ENRICHMENT_TERM).find({'entityType': self.entity_type.upper(), EnrichmentEntity.ENTITY_ID: { '$regex': 'http://data.europeana.eu/.*' }}).count()
        return entities
    
    def build_entity_chunk(self, start):
        #TODO rename variables, places-> entity
        entities = self.client.get_database(HarvesterConfig.DB_ENRICHMENT).get_collection(HarvesterConfig.COL_ENRICHMENT_TERM).find( {'entityType': self.entity_type.upper(), EnrichmentEntity.ENTITY_ID: { '$regex': 'http://data.europeana.eu/.*' }}, {EnrichmentEntity.ENTITY_ID:1, '_id': 0})[start:start + ContextClassHarvester.CHUNK_SIZE]
        
        entities_chunk = {}
        for entity in entities:
            entity_id = entity[EnrichmentEntity.ENTITY][EnrichmentEntity.ABOUT]
            entities_chunk[entity_id] = self.client.get_database(HarvesterConfig.DB_ENRICHMENT).get_collection(HarvesterConfig.COL_ENRICHMENT_TERM).find_one({ EnrichmentEntity.ENTITY_ID : entity_id })
        return entities_chunk
    
    def extract_numeric_id(self, entity_id):
        parts = entity_id.split("/")
        #numeric id is the last part of the URL 
        return parts[len(parts) - 1]
    
    def build_solr_doc(self, entities, start, one_entity = False):
        from xml.etree import ElementTree as ET

        docroot = ET.Element('add')
        for entity_id, values  in entities.items():
            print("processing entity:" + entity_id)
            self.build_entity_doc(docroot, entity_id, values)
        self.client.close()
        return self.write_to_file(docroot, start, one_entity)
        
    def build_entity_doc(self, docroot, entity_id, entity_rows):
        #sys.path.append('ranking_metrics')
        from xml.etree import ElementTree as ET
        doc = ET.SubElement(docroot, 'doc')
        self.add_field(doc, 'id', entity_id)
        #self.add_field(doc, 'internal_type', 'Place')
        self.add_field(doc, 'internal_type', self.entity_type.capitalize())
        self.process_created_modified_timestamps(doc, entity_rows)
        self.process_representation(doc, entity_id, entity_rows)
    

    def add_field_list(self, docroot, field_name, values):
        if(values is None):
            return
        for value in values:
            self.add_field(docroot, field_name, value)
        
    def add_field(self, docroot, field_name, field_value):
        from xml.etree import ElementTree as ET

        f = ET.SubElement(docroot, 'field')
        f.set('name', field_name)
        try:
            f.text = self.sanitize_field(field_value)
        except Exception as ex:
            print(str(field_name) + "!" + str(field_value) + str(ex))

    def sanitize_field(self, field_value):
        field_value = field_value.replace("\n", " ")
        field_value = field_value.replace("\\n", " ")
        field_value = field_value.replace("\t", " ")
        return field_value

    def write_to_file(self, doc, start, one_entity):
        from xml.etree import ElementTree as ET
        from xml.dom import minidom
        import io
        writepath = self.get_writepath(start, one_entity)
        roughstring = ET.tostring(doc, encoding='utf-8')
        reparsed = minidom.parseString(roughstring)
        reparsed = reparsed.toprettyxml(encoding='utf-8', indent="     ").decode('utf-8')
        with io.open(writepath, 'w', encoding='utf-8') as writefile:
            writefile.write(reparsed)
            writefile.close()
        return writepath

    def get_writepath(self, start, one_entity):
        if(one_entity):
            return self.write_dir + "/individual_entities/"+ self.name + "/" + str(start) +  ".xml"
        else:
            return self.write_dir + "/" + self.name + "/" + self.name + "_" + str(start) + "_" + str(start + ContextClassHarvester.CHUNK_SIZE) +  ".xml"

    def grab_relevance_ratings(self, docroot, entity_id, entity):
        metrics_record = self.relevance_counter.get_raw_relevance_metrics(entity)
        eu_enrichments = metrics_record.uri_hits
        eu_terms = metrics_record.term_hits
        pagerank = metrics_record.pagerank
        if(self.ranking_model == self.config.HARVESTER_RELEVANCE_RANKING_MODEL_DEFAULT):
            ds = self.relevance_counter.calculate_relevance_score(entity_id, pagerank, eu_enrichments, eu_terms)
        elif(self.ranking_model == self.config.HARVESTER_RELEVANCE_RANKING_MODEL_NORMALIZED):
            ds = self.relevance_counter.calculate_normalized_score(pagerank, eu_enrichments, eu_terms)
        else:
            raise ValueError("Must set property harvester.relevance.ranking.model to one of the values <default> or <normalized>")    
        self.add_field(docroot, 'europeana_doc_count', str(eu_enrichments))
        self.add_field(docroot, 'europeana_term_hits', str(eu_terms))
        self.add_field(docroot, 'pagerank', str(pagerank))
        self.add_field(docroot, 'derived_score', str(ds))
        self.add_suggest_filters(docroot, eu_enrichments)
        return True

    def grab_isshownby(self, docroot, web_resource):
        if(web_resource is not None):
            self.add_field(docroot, 'isShownBy', web_resource.media_url)
            self.add_field(docroot, 'isShownBy.source', web_resource.europeana_item_id)
            self.add_field(docroot, 'isShownBy.thumbnail', web_resource.thumbnail_url)   
    
    def process_address(self, docroot, entity_id, address):
        #TODO check if the full address is needed
        #address_components = []
        for k, v in address.items():
            key = k	
            value = v
            #about is not an ignored property for address
            if ("about" == k):
                key = "address_" + k
            elif ("vcardHasGeo" == k):
                #remove geo:, keep just lat,long 
                value = v.split(":")[-1]
                    
            if(self.is_ignored_property(key)):
                #ignored properties are not mapped to solr document
                continue
        
            if(key not in ContextClassHarvester.FIELD_MAP.keys()):
                self.log_warm_message(entity_id, "unmapped field: " + key)
                continue
        
            field_name = ContextClassHarvester.FIELD_MAP[key][self.LABEL]
            if("vcardHasGeo" != k):
                field_name = field_name + ".1"
            
            self.add_field(docroot, field_name, value)
            #address_components.append(v)

    def process_created_modified_timestamps(self, docroot, entity_rows):
        # Solr time format YYYY-MM-DDThh:mm:ssZ
        if "created" in entity_rows:
            self.add_field(docroot, 'created', entity_rows["created"].isoformat()+"Z")
        #"modified" changed to updated in the database
        if "updated" in entity_rows:
            self.add_field(docroot, "modified", entity_rows["updated"].isoformat()+"Z")

    def is_ignored_property(self, characteristic):
        return str(characteristic) in self.IGNORED_PROPS
        
    def process_representation(self, docroot, entity_id, entity):
        #all pref labels
        all_preflabels = []
        for characteristic in entity[EnrichmentEntity.REPRESENTATION]:
            if(self.is_ignored_property(characteristic)):
                continue
            elif (str(characteristic) not in ContextClassHarvester.FIELD_MAP.keys()):
                # TODO: log this?
                print("unmapped property: " + str(characteristic))
                continue
            elif(characteristic == "address"):
                self.process_address(docroot, entity_id, entity[EnrichmentEntity.REPRESENTATION]['address'])
            # TODO: Refactor horrible conditional
            elif(str(characteristic) == "dcIdentifier"):
                self.add_field_list(docroot, EnrichmentEntity.DC_IDENTIFIER, entity[EnrichmentEntity.REPRESENTATION]['dcIdentifier'][EnrichmentEntity.LANG_DEF])
            elif(str(characteristic) == "edmOrganizationDomain"):
                #TODO: create method to add solr field for .en fields
                self.add_field(docroot, EnrichmentEntity.ORGANIZATION_DOMAIN + "." + EnrichmentEntity.LANG_EN, entity[EnrichmentEntity.REPRESENTATION]['edmOrganizationDomain'][EnrichmentEntity.LANG_EN])
            elif(str(characteristic) == "edmEuropeanaRole"): 
                #multivalued
                roles = entity[EnrichmentEntity.REPRESENTATION]['edmEuropeanaRole'][EnrichmentEntity.LANG_EN]
                self.add_field_list(docroot, EnrichmentEntity.EUROPEANA_ROLE + "." + EnrichmentEntity.LANG_EN, roles)
            elif(str(characteristic) == "edmGeographicLevel"):
                self.add_field(docroot, EnrichmentEntity.GEOGRAPHIC_LEVEL + "." + EnrichmentEntity.LANG_EN, entity[EnrichmentEntity.REPRESENTATION]['edmGeographicLevel'][EnrichmentEntity.LANG_EN])
            elif(str(characteristic) == "edmCountry"):
                self.add_field(docroot, EnrichmentEntity.COUNTRY, entity[EnrichmentEntity.REPRESENTATION]['edmCountry'][EnrichmentEntity.LANG_EN])
            elif(str(characteristic) == "begin"):
                #pick first value from default language for timestamps, need to check for agents
                self.add_field(docroot, EnrichmentEntity.EDM_BEGIN, entity[EnrichmentEntity.REPRESENTATION]['begin'][EnrichmentEntity.LANG_DEF][0])
            elif(str(characteristic) == "end"):
                #pick first value from default language for timestamps, need to check for agents
                self.add_field(docroot, EnrichmentEntity.EDM_END, entity[EnrichmentEntity.REPRESENTATION]['end'][EnrichmentEntity.LANG_DEF][0])
            elif(type(entity[EnrichmentEntity.REPRESENTATION][characteristic]) is dict):
                # hiddenLabels are currenlty used only for Timespans
                if(str(characteristic) == "hiddenLabel" and self.ignore_hidden_label()):
                    continue
            
                #for each entry in the language map
                for lang in entity[EnrichmentEntity.REPRESENTATION][characteristic]:
                    pref_label_count = 0
                    #avoid duplicates when adding values from prefLabel
                    prev_alts = []
                    if(ContextClassHarvester.LANG_VALIDATOR.validate_lang_code(entity_id, lang)):
                        field_name = ContextClassHarvester.FIELD_MAP[characteristic][self.LABEL]
                        field_values = entity[EnrichmentEntity.REPRESENTATION][characteristic][lang]
                        #property is language map of strings
                        if(type(field_values) == str):
                            lang_code = lang if lang != EnrichmentEntity.LANG_DEF else ''
                            q_field_name = field_name + "."+ lang_code
                            #field value = field_values
                            self.add_field(docroot, q_field_name, field_values) 
                        else:
                            #for each value in the list
                            for field_value in field_values:
                                q_field_name = field_name
                                lang_code = lang if lang != EnrichmentEntity.LANG_DEF else ''
                                if(ContextClassHarvester.FIELD_MAP[characteristic][self.TYPE] == self.TYPE_STRING):
                                    q_field_name = field_name + "."+ lang_code
                                # Code snarl: we often have more than one prefLabel per language in the data
                                # We can also have altLabels
                                # We want to shunt all but the first-encountered prefLabel into the altLabel field
                                # while ensuring the altLabels are individually unique
                                # TODO: Refactor (though note that this is a non-trivial refactoring)
                                # NOTE: prev_alts are for one language, all_preflabels include labels in any language
                                if(characteristic == 'prefLabel' and pref_label_count > 0):
                                    #move all additional labels to alt label
                                    q_field_name = "skos_altLabel." + lang_code
                                    #SG - TODO: add dropped pref labels to prev_alts??
                                    #prev_alts.append(field_value)
                                if('altLabel' in q_field_name):
                                    #TODO: SG why this? we skip alt labels here, but we don't add the gained entries from prefLabels
                                    
                                    if(field_value in prev_alts):
                                        continue
                                    prev_alts.append(field_value)
                                    #suggester uses alt labels for some entity types (organizations) 
                                    #disables until altLabels are added to payload 
                                    #self.add_alt_label_to_suggest(field_value, all_preflabels)
                                if(str(characteristic) == "edmAcronym"):
                                    #suggester uses alt labels for some entity types (organizations) 
                                    self.add_acronym_to_suggest(field_value, all_preflabels)
                                    
                                if(characteristic == 'prefLabel' and pref_label_count == 0):
                                    pref_label_count = 1
                                    #TODO: SG - the suggester could actually make use of all pref labels, but the hightlighter might crash
                                    all_preflabels.append(field_value)
                                
                                #add field to solr doc
                                self.add_field(docroot, q_field_name, field_value)                                                          
            #property is list
            elif(type(entity[EnrichmentEntity.REPRESENTATION][characteristic]) is list):
                field_name = ContextClassHarvester.FIELD_MAP[characteristic][self.LABEL]
                for entry in entity[EnrichmentEntity.REPRESENTATION][characteristic]:
                    self.add_field(docroot, field_name, entry)
            # property is a single value
            else: 
                try:
                    field_name = ContextClassHarvester.FIELD_MAP[characteristic][self.LABEL]
                    field_value = entity[EnrichmentEntity.REPRESENTATION][characteristic]
                    self.add_field(docroot, field_name, str(field_value))
                except KeyError as error:
                    print('Attribute found in source but undefined in schema.' + str(error))
                    
        #add suggester payload
        web_resource = self.depiction_manager.get_depiction(entity_id)
        self.grab_isshownby(docroot, web_resource)
        payload = self.build_payload(entity_id, entity, web_resource)
        self.add_field(docroot, 'payload', json.dumps(payload))
        #add suggester field
        all_preflabels = self.shingle_preflabels(all_preflabels)
        # SG: values in the same language are joined using space separator. values in different languages are joined using underscore as it is used as tokenization pattern. see schema.xml  
        self.add_field(docroot, 'skos_prefLabel', "_".join(sorted(set(all_preflabels))))
        depiction = self.preview_builder.get_depiction(entity_id)
        if(depiction):
            self.add_field(docroot, 'foaf_depiction', depiction)
        
        self.grab_relevance_ratings(docroot, entity_id, entity)

    def shingle_preflabels(self, preflabels):
        shingled_labels = []
        for label in preflabels:
            all_terms = label.split()
            for i in range(len(all_terms)):
                shingle = " ".join(all_terms[i:len(all_terms)])
                shingled_labels.append(shingle)
        return shingled_labels

    def build_payload(self, entity_id, entity_rows, web_resource):
        payload = self.preview_builder.build_preview(self.entity_type, entity_id, entity_rows[EnrichmentEntity.REPRESENTATION], web_resource)  
        return payload

    def add_suggest_filters(self, docroot, enrichment_count):
        self.add_field(docroot, 'suggest_filters', self.entity_type.capitalize())
        if(enrichment_count > 0):
            self.add_field(docroot, 'suggest_filters', 'in_europeana')
    
    def suggest_by_alt_label(self):
        #this functionality can be activated by individual harvesters
        return False
    
    def suggest_by_acronym(self):
        #this functionality can be activated by individual harvesters
        return False
        
    def add_alt_label_to_suggest(self, value, suggester_values):
        if(self.suggest_by_alt_label() and (value not in suggester_values)):
            suggester_values.append(value)
            
    def add_acronym_to_suggest(self, value, suggester_values):
        if(self.suggest_by_acronym() and (value not in suggester_values)):
            suggester_values.append(value)
    
    def ignore_hidden_label(self):
        return True
    
class ConceptHarvester(ContextClassHarvester):

    def __init__(self):
        ContextClassHarvester.__init__(self, EnrichmentEntity.TYPE_CONCEPT)
        #sys.path.append(os.path.join(os.path.dirname(__file__), 'ranking_metrics'))
        self.importer = MetricsImporter(self, MetricsImporter.DB_CONCEPT, EnrichmentEntity.TYPE_CONCEPT)
        self.relevance_counter = RelevanceCounter.ConceptRelevanceCounter(self.importer)
    
    def is_ignored_property(self, characteristic):
        #ignore sameAs for Concepts
        return str(characteristic) in self.IGNORED_PROPS or ContextClassHarvester.PROP_OWL_SAMEAS == str(characteristic)


class AgentHarvester(ContextClassHarvester):

    def __init__(self):
        #sys.path.append(os.path.join(os.path.dirname(__file__), 'ranking_metrics'))
        # TODO check if 'eu.europeana.corelib.solr.entity.AgentImpl' is correct and needed (see entityType column in the database)
        #ContextClassHarvester.__init__(self, 'agents', 'eu.europeana.corelib.solr.entity.AgentImpl')
        ContextClassHarvester.__init__(self, EnrichmentEntity.TYPE_AGENT)
        self.importer = MetricsImporter(self, MetricsImporter.DB_AGENT, EnrichmentEntity.TYPE_AGENT)
        self.relevance_counter = RelevanceCounter.AgentRelevanceCounter(self.importer)

    def log_missing_entry(self, entity_id):
        msg = "Entity found in Agents but not get_collection(COL_ENRICHMENT_TERM) collection: " + entity_id
        logfile = "missing_agents.txt"
        logpath = ContextClassHarvester.LOG_LOCATION + logfile
        with open(logpath, 'a') as lgout:
            lgout.write(msg)
            lgout.write("\n")

class PlaceHarvester(ContextClassHarvester):

    def __init__(self):
        #sys.path.append(os.path.join(os.path.dirname(__file__), 'ranking_metrics'))
        #TODO: check if 'eu.europeana.corelib.solr.entity.PlaceImpl' still needed/used
        #ContextClassHarvester.__init__(self, 'places', 'eu.europeana.corelib.solr.entity.PlaceImpl')
        ContextClassHarvester.__init__(self, EnrichmentEntity.TYPE_PLACE)
        self.importer = MetricsImporter(self, MetricsImporter.DB_PLACE, EnrichmentEntity.TYPE_PLACE)
        self.relevance_counter = RelevanceCounter.PlaceRelevanceCounter(self.importer)

    def grab_isshownby(self, docroot, web_resource):
        #isShownBy not supported for places
        return

class TimespanHarvester(ContextClassHarvester):

    def __init__(self):
        #sys.path.append(os.path.join(os.path.dirname(__file__), 'ranking_metrics'))
        #TODO: check if 'eu.europeana.corelib.solr.entity.PlaceImpl' still needed/used
        #ContextClassHarvester.__init__(self, 'places', 'eu.europeana.corelib.solr.entity.PlaceImpl')
        ContextClassHarvester.__init__(self, EnrichmentEntity.TYPE_TIMESPAN)
        self.importer = MetricsImporter(self, MetricsImporter.DB_TIMESPAN, EnrichmentEntity.TYPE_TIMESPAN)
        self.relevance_counter = RelevanceCounter.TimespanRelevanceCounter(self.importer)
    
    def ignore_hidden_label(self):
        return False

class OrganizationHarvester(ContextClassHarvester):

    def __init__(self):
        ContextClassHarvester.__init__(self, EnrichmentEntity.TYPE_ORGANIZATION)
        #sys.path.append(os.path.join(os.path.dirname(__file__), 'ranking_metrics'))
        self.importer = MetricsImporter(self, MetricsImporter.DB_ORGANIZATION, EnrichmentEntity.TYPE_ORGANIZATION)        
        self.relevance_counter = RelevanceCounter.OrganizationRelevanceCounter(self.importer)

    #def get_mongo_host (self):
    #    return self.config.get_mongo_host(self.name)
     
    def suggest_by_alt_label(self):
        return True
    
    def suggest_by_acronym(self):
        return True
    
    def grab_isshownby(self, docroot, web_resource):
        #isShownBy not supported for organizations
        return 
    
        
class IndividualEntityBuilder:
    
    OUTDIR = os.path.join(os.path.dirname(__file__), '..', 'tests', 'testfiles', 'dynamic')

    def build_individual_entity(self, entity_id):
        from pymongo import MongoClient
        if(entity_id.find("/place/") > 0):
            harvester = PlaceHarvester()
        elif(entity_id.find("/agent/") > 0):
            harvester = AgentHarvester()
        elif(entity_id.find("/organization/") > 0):
            harvester = OrganizationHarvester()
        elif(entity_id.find("/concept/") > 0):
            harvester = ConceptHarvester()
        elif(entity_id.find("/time") > 0):
            harvester = TimespanHarvester()
        else:
            print("unrecognized entity type for uri:" + entity_id)
        
        self.client = MongoClient(harvester.get_mongo_host())
        entity_rows = self.client.get_database(HarvesterConfig.DB_ENRICHMENT).get_collection(HarvesterConfig.COL_ENRICHMENT_TERM).find_one({ EnrichmentEntity.ENTITY_ID : entity_id })
        entity_chunk = {}
        entity_chunk[entity_id] = entity_rows
        #used only for filename generation 
        if('semium' in entity_id):
            ident = entity_id.split("/")[-1];
            start_id = int(ident.replace('x', '0'))
            print(start_id)
        else:
            start_id = int(entity_id.split("/")[-1])    
        
        #one_entity
        solrDocFile = harvester.build_solr_doc(entity_chunk, start_id, True)
        return solrDocFile
    
class ChunkBuilder:

    def __init__(self, entity_type, start):
        self.entity_type = entity_type
        self.start = start

    def build_chunk(self):
        #TODO 
        #if(self.entity_type == "concept"):
        if(self.entity_type == EnrichmentEntity.TYPE_CONCEPT):
            harvester = ConceptHarvester()
        #elif(self.entity_type == "agent"):
        elif(self.entity_type == EnrichmentEntity.TYPE_AGENT):
            harvester = AgentHarvester()
        #elif(self.entity_type == "place"):
        elif(self.entity_type == EnrichmentEntity.TYPE_PLACE):
            harvester = PlaceHarvester()
        #elif(self.entity_type == "organization"):
        elif(self.entity_type == EnrichmentEntity.TYPE_ORGANIZATION):
            harvester = OrganizationHarvester()
        elif(self.entity_type == EnrichmentEntity.TYPE_TIMESPAN):
            harvester = TimespanHarvester()    
        ec = harvester.build_entity_chunk(self.start)
        harvester.build_solr_doc(ec, self.start)
