
# purpose of script is to populate the ranking metrics for Concepts
# Right now we need URI hits, term hits, and PageRank
# (Wikipedia hits are now deprecated as a ranking sigal)

# first, we need to grab all Organization identifiers from Mongo, as well as their
# @en labels 
from entities.ranking_metrics.MetricsImporter import MetricsImporter 
from entities.ContextClassHarvesters import ContextClassHarvester, ConceptHarvester, AgentHarvester, PlaceHarvester, OrganizationHarvester

#run import scripts
#read page rank once, this is an expensive information
harvester = OrganizationHarvester()
importer = MetricsImporter(harvester, MetricsImporter.DB_ORGANIZATION, MetricsImporter.TYPE_ORGANIZATION)
importer.import_metrics()


harvester = ConceptHarvester()
importer = MetricsImporter(harvester, MetricsImporter.DB_CONCEPT, MetricsImporter.TYPE_CONCEPT)
importer.import_metrics()

harvester = PlaceHarvester()
importer = MetricsImporter(harvester, MetricsImporter.DB_PLACE, MetricsImporter.TYPE_PLACE)
importer.import_metrics()

harvester = AgentHarvester()
importer = MetricsImporter(harvester, MetricsImporter.DB_AGENT, MetricsImporter.TYPE_AGENT)
importer.import_metrics()

