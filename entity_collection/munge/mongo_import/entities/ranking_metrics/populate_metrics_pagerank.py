from entities.ranking_metrics.MetricsImporter import MetricsImporter
from entities.ContextClassHarvesters import ConceptHarvester, AgentHarvester, PlaceHarvester, OrganizationHarvester
		
#run import scripts
#read page rank once, this is an expensive information
harvester = ConceptHarvester()
importer = MetricsImporter(harvester, MetricsImporter.DB_CONCEPT, MetricsImporter.TYPE_CONCEPT)
importer.import_pagerank()

#harvester = OrganizationHarvester()
#importer = MetricsImporter(harvester, DB_ORGANIZATION, TYPE_ORGANIZATION)
#importer.import_pagerank()

#harvester = PlaceHarvester()
#importer = MetricsImporter(harvester, DB_PLACE, TYPE_PLACE)
#importer.import_pagerank()


#harvester = AgentHarvester()
#importer = MetricsImporter(harvester, DB_AGENT, TYPE_AGENT)
#importer.import_pagerank()


