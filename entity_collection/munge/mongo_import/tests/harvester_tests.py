# ========================================================================#
#
# Tests to ensure import process has run correctly.
#
# Note the scope of these tests: they test only that the import process
# is yielding the expected output. They do nothing to validate the
# data of the imported entities itself.
#
#=========================================================================#
import sys
import unittest
import entities.ContextClassHarvesters
import entities.preview_builder.PreviewBuilder

class HarvesterTest(unittest.TestCase):
   
    # tests on a couple of entities of each type
    def test_transform(self):
        ieb = entities.ContextClassHarvesters.IndividualEntityBuilder()
        test_entities = [
            "http://data.europeana.eu/agent/base/11241",   # Paris Hilton
            "http://data.europeana.eu/agent/base/146741",  # Leonardo da Vinci
            "http://data.europeana.eu/place/base/40360",   # Den Haag
            "http://data.europeana.eu/place/base/143914",  # Ferrara
            #"http://data.europeana.eu/concept/base/214",   # Neoclassicism
            "http://data.europeana.eu/concept/base/207",    # Byzantine art
            "http://data.europeana.eu/organization/1482250000002112001",    # BnF 
            "http://data.europeana.eu/organization/1482250000004375509", #Deutsches film institute
            "http://data.europeana.eu/agent/base/178", #agent max page rank: Aristotel
            "http://data.europeana.eu/place/base/216254", #place max page rank: United States
            "http://data.europeana.eu/concept/base/83", #concept max page rank: World War I
            "http://data.europeana.eu/organization/1482250000004505021", #organization max page rank: Internet Archive
            "http://data.europeana.eu/organization/1482250000004503580"
        ]
        for test_entity in test_entities:
            print("building entity: " + test_entity)
            ieb.build_individual_entity(test_entity)
        #errors = test_files_against_mongo('dynamic')
        #errors.extend(test_json_formation('dynamic'))
        #return errors
        
    # tests on a couple of entities of each type
    def test_build_individual_entity(self):
        #bnf
        #entity_id = "http://data.europeana.eu/organization/1482250000002112001"
        #sofia japanese band 
        #entity_id = "http://data.europeana.eu/agent/base/6376"
        #Centraal Museum 
        entity_id = "http://data.europeana.eu/organization/1482250000004500796"
        #government of catalunia
        #entity_id = "http://data.europeana.eu/organization/1482250000004503580"
        ieb = entities.ContextClassHarvesters.IndividualEntityBuilder()
        ieb.build_individual_entity(entity_id)
    
    def test_OrganizationHarvester(self):
        ol = entities.ContextClassHarvesters.OrganizationHarvester()
        entity_list = ol.build_entity_chunk(0)
        print("First chunk: 0" + len(entity_list))
          
        