# ========================================================================#
#
# Tests to ensure import process has run correctly.
#
# Note the scope of these tests: they test only that the import process
# is yielding the expected output. They do nothing to validate the
# data of the imported entities itself.
#
#=========================================================================#
#import sys
import unittest
import entities.ContextClassHarvesters
#import entities.preview_builder.PreviewBuilder

class HarvesterTest(unittest.TestCase):
   
    # tests on a couple of entities of each type
    def test_build_entity_list(self):
        ieb = entities.ContextClassHarvesters.IndividualEntityBuilder()
        test_entities = [
            #"http://data.europeana.eu/agent/base/11241",   # Paris Hilton
            #"http://data.europeana.eu/agent/base/146741",  # Leonardo da Vinci
            #"http://data.europeana.eu/place/base/40360",   # Den Haag
            #"http://data.europeana.eu/place/base/143914",  # Ferrara
            #"http://data.europeana.eu/concept/base/214",   # Neoclassicism
            #"http://data.europeana.eu/concept/base/207",    # Byzantine art
            #"http://data.europeana.eu/organization/1482250000002112001",    # BnF 
            #"http://data.europeana.eu/organization/1482250000004375509", #Deutsches film institute
            #"http://data.europeana.eu/agent/base/178", #agent max page rank: Aristotel
            #"http://data.europeana.eu/place/base/216254", #place max page rank: United States
            #"http://data.europeana.eu/concept/base/83", #concept max page rank: World War I
            #"http://data.europeana.eu/organization/1482250000004505021", #organization max page rank: Internet Archive
            #"http://data.europeana.eu/organization/1482250000004503580",
            #"http://data.europeana.eu/concept/base/1326",#with isShownBy, Minimal-Techno
            #"http://data.europeana.eu/concept/base/1337",#with isShownBy, Pastorale
            #"http://data.europeana.eu/agent/base/100013",#J.R. (music)
            #"http://data.europeana.eu/place/base/41948"#paris
            "http://data.europeana.eu/timespan/1",
            "http://data.europeana.eu/timespan/2",
            "http://data.europeana.eu/timespan/3",
            "http://data.europeana.eu/timespan/4",
            "http://data.europeana.eu/timespan/5",
            "http://data.europeana.eu/timespan/6",
            "http://data.europeana.eu/timespan/7",
            "http://data.europeana.eu/timespan/8",
            "http://data.europeana.eu/timespan/9",
            "http://data.europeana.eu/timespan/10",
            "http://data.europeana.eu/timespan/11",
            "http://data.europeana.eu/timespan/12",
            "http://data.europeana.eu/timespan/13",
            "http://data.europeana.eu/timespan/14",
            "http://data.europeana.eu/timespan/15",
            "http://data.europeana.eu/timespan/16",
            "http://data.europeana.eu/timespan/17",
            "http://data.europeana.eu/timespan/18",
            "http://data.europeana.eu/timespan/19",
            "http://data.europeana.eu/timespan/20",
            "http://data.europeana.eu/timespan/21"
            
            
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
        #entity_id = "http://data.europeana.eu/organization/1482250000004500796"
        #Birger Sjöberg
        #entity_id = "http://data.europeana.eu/agent/base/54407"
        # J.R. (musician)
        #entity_id = "http://data.europeana.eu/agent/base/100013"
        # Leonardo Da Vinci
        #entity_id = "http://data.europeana.eu/agent/base/146741"
        #government of catalunia
        #entity_id = "http://data.europeana.eu/organization/1482250000004503580"
        # Timespan 
        entity_id = "http://semium.org/time/0079"
                
        ieb = entities.ContextClassHarvesters.IndividualEntityBuilder()
        ieb.build_individual_entity(entity_id)
    
    def test_OrganizationHarvester(self):
        ol = entities.ContextClassHarvesters.OrganizationHarvester()
        entity_list = ol.build_entity_chunk(0)
        print("First chunk: 0" + str(len(entity_list)))
          
    def test_TimespanHarvester(self):
        ts = entities.ContextClassHarvesters.TimespanHarvester()
        entity_list = ts.build_entity_chunk(20)
        print("First chunk: 0" + str(len(entity_list)))
            