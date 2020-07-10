class DepictionManager:
	
	DEPICTIONS_FILE = "depictions_all.csv"
	
	def __init__(self, harvester_config):
		self.depictions = {}
		self.harvester_config = harvester_config
	
	def load_depictions(self):
		import os
		import csv
		
		current_dir=os.getcwd()
		csv_file = os.path.join(current_dir, 'entities', 'resources', DepictionManager.DEPICTIONS_FILE)
		with open(csv_file, encoding="utf-8") as resources_file:
			csv_reader = csv.reader(resources_file, delimiter=',')
			for row in csv_reader:
				entity_id = row[0]
				#eliminate pending space
				thumbnail_url = row[3].strip()	
				#(entity_id, item_id, media_url, thumbnail_url) = line.split(sep=",", maxsplit=3)
				#thumbnail_url = thumbnail_url.rstrip()
				self.depictions[entity_id] = WebResource(entity_id, row[1], row[2], thumbnail_url)

	def get_depiction(self, entity_id):
		if len(self.depictions) == 0:
			self.load_depictions()
		
		if self.depictions.__contains__(entity_id):
			return self.depictions[entity_id]
		else:
			return None
			
class WebResource:
	def __init__(self, entity_id, item_id, media_url, thumbnail_url):
		self.entity_id = entity_id
		self.europeana_item_id = item_id
		self.media_url = media_url
		self.thumbnail_url = thumbnail_url      