import xml.etree.ElementTree as ET
import os, re
from EnrichmentEntity import EnrichmentEntity
from HarvesterConfig import HarvesterConfig

class PreviewBuilder:

    jobtree = ET.parse(os.path.join(os.path.dirname(__file__), 'professions.rdf'))
    PROFESSIONS = jobtree.getroot()
    ns = {'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', 'skos':'http://www.w3.org/2004/02/skos/core#', 'xml':'http://www.w3.org/XML/1998/namespace'}

    def __init__(self, mongo_client, entity_type):
        from pymongo import MongoClient
        # note fixed import path
        self.mongoclient = mongo_client
        self.depictions = {}
        # temporarily disable until depictions list will be created
        must_load_depictions = False 
        if(must_load_depictions):
            self.load_depictions()
        
    def build_preview(self, entity_type, entity_id, entity_rows, web_resource):
        preview_fields = {}
        preview_fields['id'] = entity_id
        preview_fields['type'] = entity_type.capitalize()
        preview_fields['prefLabel'] = self.build_pref_label(entity_rows)
        altLabel = self.build_alt_label(entity_rows)
        if(altLabel is not None):
            preview_fields['altLabel'] = altLabel
        
        # removed hidden label #EA-2260
        #preview_fields['hiddenLabel'] = self.build_max_recall(entity_type, entity_rows)
            
        #depiction
        depiction = self.build_depiction(entity_id, entity_rows)
        if(depiction):
            preview_fields['depiction'] = depiction 
        if(web_resource):
            preview_fields['isShownBy'] = self.build_isshownby_label(web_resource)
            
        #if(entity_type == "Agent"):
        if(entity_type == EnrichmentEntity.TYPE_AGENT):
            birth_date = self.build_birthdate(entity_rows) 
            if(birth_date): 
                preview_fields['dateOfBirth'] = birth_date
            death_date = self.build_deathdate(entity_rows) 
            if(death_date): 
                preview_fields['dateOfDeath'] = death_date
            role = self.build_role(entity_rows) 
            if(role): 
                preview_fields['professionOrOccupation'] = role
        #elif(entity_type == "Place"):
        elif(entity_type == EnrichmentEntity.TYPE_PLACE):
            country_label = self.build_country_label(entity_rows)    
            if(country_label): 
                preview_fields['isPartOf'] = country_label
        elif(entity_type == EnrichmentEntity.TYPE_TIMESPAN):
            if('begin' in entity_rows): 
                preview_fields['begin'] = entity_rows['begin'][EnrichmentEntity.LANG_DEF][0]
            if('end' in entity_rows): 
                preview_fields['end'] = entity_rows['end'][EnrichmentEntity.LANG_DEF][0]    
        elif(entity_type == EnrichmentEntity.TYPE_ORGANIZATION):
            # for some reason the preview data model for multilingual 
            # Organization fields is different from the mulitilingual
            # model elsewhere
            preview_fields['acronym'] = self.build_acronym(entity_rows)
            #build_org_preview_field('acronym', preview_fields, entity_rows, "edmAcronym")
            if(self.get_org_field_en(entity_rows, "edmCountry")):
                preview_fields['country'] = self.get_org_field_en(entity_rows, "edmCountry")
            if(self.get_org_field_en(entity_rows, "edmOrganizationDomain")):
                preview_fields['organizationDomain'] = self.get_org_field_en(entity_rows, "edmOrganizationDomain")
        return preview_fields

    def build_isshownby_label(self, web_resource):
        isshownby_fields = {}
        isshownby_fields['id'] = web_resource.media_url
        isshownby_fields['type'] = 'WebResource'
        isshownby_fields['source'] = web_resource.europeana_item_id
        isshownby_fields['thumbnail'] = web_resource.thumbnail_url
        return isshownby_fields
            
    def build_pref_label(self, entity_rows):
        all_langs = {}
        for lang in entity_rows['prefLabel']:
            lang_code = lang if lang != EnrichmentEntity.LANG_DEF else ''
            all_langs[lang_code] = entity_rows['prefLabel'][lang][0]
        return all_langs

    def build_alt_label(self, entity_rows):
        all_langs = {}
        if('altLabel' in entity_rows.keys()):
            for lang in entity_rows['altLabel']:
                lang_code = lang if lang != EnrichmentEntity.LANG_DEF else ''
                all_langs[lang_code] = entity_rows['altLabel'][lang]
        else:
            return None        
        return all_langs
    
    #TODO refactor and remove dupplication in labe methods 
    def build_acronym(self, entity_rows):
        all_langs = {}
        if('edmAcronym' in entity_rows.keys()):
            for lang in entity_rows['edmAcronym']:
                lang_code = lang if lang != EnrichmentEntity.LANG_DEF else ''
                all_langs[lang_code] = entity_rows['edmAcronym'][lang]
        else:
            return None        
        return all_langs

    def build_depiction(self, entity_id, entity_rows):
        #temporarily disabled
        ignore_depiction = True
        if(ignore_depiction):
            return None 
        
        #if available in database
        if('foafDepiction' in entity_rows.keys()):
            return entity_rows['foafDepiction'];
        #if available in csv files    
        elif(self.get_depiction(entity_id)):
            return self.get_depiction(entity_id)
        else:
            return None


    def get_org_field_en(self, entity_rows, entity_key):
        if(entity_key in entity_rows.keys()):
            #only english values are available for now and need to be converted to string literals    
            if "en" in entity_rows[entity_key].keys():
                return entity_rows[entity_key]["en"]
        return None
    
    def build_max_recall(self, entity_type, entity_rows):
        all_langs = {}
        for lang in entity_rows['prefLabel']:
            lang_code = lang if lang != EnrichmentEntity.LANG_DEF else ''
            all_langs[lang_code] = self.transpose_terms(entity_type, entity_rows['prefLabel'][lang][0])
        return all_langs

    def transpose_terms(self, entity_type, term):
        # reimplements (with trim_term())
        # https://github.com/europeana/uim-europeana/blob/master/workflow_plugins/
        # europeana-uim-plugin-enrichment/src/main/java/eu/europeana/uim/enrichment/
        # normalizer/AgentNormalizer.java
        term = self.trim_term(term)
        all_terms = [term]
        #if(entity_type != 'Agent'): # only agents need bibliographic inversion
        if(entity_type != EnrichmentEntity.TYPE_AGENT): # only agents need bibliographic inversion
            return all_terms
        elif(' ' not in term): # not possible to invert a single term
            return all_terms
        elif(',' in term):
            term_bits = term.strip().split(',')
            term_bits.reverse()
            reversed_term = " ".join(term_bits)
        else:
            term_bits = term.split()
            term_bits.insert(0, term_bits.pop())
            term_bits[0] = term_bits[0] + ","
            reversed_term = " ".join(term_bits)
        reversed_term = re.sub("\s+", " ", reversed_term.strip())
        all_terms.append(reversed_term)
        return all_terms

    def trim_term(self, term):
        term = term.strip()
        if("(" in term):
            term = term.split("(")[0]
        elif("[" in term):
            term = term.split("[")[0]
        elif("<" in term):
            term = term.split("<")[0]
        elif(";" in term):
            term = term.split(";")[0]
        return term.strip()

    def build_birthdate(self, entity_rows):
        # TODO: Validation routines to ensure agents have only one birthdate and deathdate apiece
        if('rdaGr2DateOfBirth' in entity_rows.keys()):
            for lang in entity_rows['rdaGr2DateOfBirth'].keys():
                dob = entity_rows['rdaGr2DateOfBirth'][lang][0]
                break
            return dob
        else:
            return None

    def build_deathdate(self, entity_rows):
        if('rdaGr2DateOfDeath' in entity_rows.keys()):
            for lang in entity_rows['rdaGr2DateOfDeath'].keys():
                dod = entity_rows['rdaGr2DateOfDeath'][lang][0]
                break
            return dod
        else:
            return None

    def build_role(self, entity_rows):
        roles = {}
        uris = []
        if('rdaGr2ProfessionOrOccupation' in entity_rows.keys()):
            for language in entity_rows['rdaGr2ProfessionOrOccupation']:
                for role in entity_rows['rdaGr2ProfessionOrOccupation'][language]:
                    if role.startswith('http'):
                        uris.append(role)
                    else:
                        try:
                            roles[language].append(role)
                        except KeyError:
                            roles[language] = [role]
            for uri in uris:
                role = PreviewBuilder.PROFESSIONS.find('./rdf:Description[@rdf:about="' + uri + '"]', PreviewBuilder.ns)
                if(role):
                    for role_label in role.findall("skos:prefLabel"):
                        label_contents = role_label.text
                        language = role_label.attrib["xml:lang"]
                        try:
                            roles[language].append(label_contents)
                        except KeyError:
                            roles[language] = [label_contents]
            return roles
        else:
            return None

    def build_country_label(self, entity_rows):
        if 'isPartOf' in entity_rows.keys():
            parent_uri = entity_rows['isPartOf']
            #parents = set([parent_uri for k in entity_rows['isPartOf'].keys() for parent_uri in entity_rows['isPartOf'][k]])
            upper_geos = {}
            while (parent_uri):
                parent = self.mongoclient.get_database(HarvesterConfig.DB_ENRICHMENT).get_collection(HarvesterConfig.COL_ENRICHMENT_TERM).find_one({ EnrichmentEntity.ENTITY_ID : parent_uri})
                if(parent is None):
                    #parent not found, break loop condition
                    raise Exception('Parent not found in database with id:' + parent_uri)
                elif('isPartOf' in parent[EnrichmentEntity.REPRESENTATION].keys()):
                    #not top parent (country), move to next parent
                    parent_uri = parent[EnrichmentEntity.REPRESENTATION]['isPartOf']
                else:    
                    upper_geos[parent_uri] = {}
                    for lang in parent[EnrichmentEntity.REPRESENTATION]['prefLabel']:
                        lang_code = lang if lang != EnrichmentEntity.LANG_DEF else ''                
                        label = parent[EnrichmentEntity.REPRESENTATION]['prefLabel'][lang][0]
                        upper_geos[parent_uri][lang_code] = label
                    #top parent, break loop
                    break
                    
            #for parent_uri in parents:
            #    parent = self.mongoclient.annocultor_db.TermList.find_one({ EnrichmentEntity.ENTITY_ID : parent_uri})
            #    if(parent is not None):
            #        upper_geos[parent_uri] = {}
            #        for lang in parent[EnrichmentEntity.REPRESENTATION]['prefLabel']:
            #            lang_code = lang if lang != EnrichmentEntity.LANG_DEF else ''                
            #            label = parent[EnrichmentEntity.REPRESENTATION]['prefLabel'][lang][0]
            #            upper_geos[parent_uri][lang_code] = label
            if(len(upper_geos.keys()) > 0): 
                return upper_geos
            else:
                return None

    def build_topConcept(self, entity_rows, language):
        # TODO: update this method once top entities dereferenceable
        entities = {}
        entities['def'] = 'Concept'
        return entities


    def build_dateRange(self, entity_rows, language):
        pass

    # temporary (?!) hack - right now Agents are the only entity type with images
    # and they are pulled in ad hoc from a static file

    def load_depictions(self):
        image_files = ['agents.wikidata.images.csv', 'concepts.merge.images.csv']
        current_dir=os.getcwd()
            
        for image_file in image_files:
            #temporary hack, the folder for csv files needs to be added to .properties file
            if(str(current_dir).endswith('ranking_metrics')):
                csv_file = os.path.join(current_dir, '../', 'resources', image_file)
            else:
                csv_file = os.path.join(current_dir, 'entities', 'resources', image_file)
            
            with open(csv_file, encoding="utf-8") as imgs:
                for line in imgs.readlines():
                    (agent_id, image_id) = line.split(sep=",", maxsplit=1)
                    agent_id = agent_id.strip()
                    image_id = image_id.strip()
                    self.depictions[agent_id] = image_id


    def get_depiction(self, entity_key):
        entity_key = entity_key.strip()
        try:
            raw_loc = self.depictions[entity_key]
            loc = re.sub(r"^\"", "", raw_loc)
            loc = re.sub(r"\"$", "", loc)
            print(loc)
            return loc
        except KeyError:
            None
