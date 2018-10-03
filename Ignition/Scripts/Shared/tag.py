# --------------------------------------------------------------------------
# Look in a specific folder for a tag whose name matches the pattern.
# If more than one, return the n-th result (where n is specified by idx).
# If none are found, or less than <idx> are found, return None.
# --------------------------------------------------------------------------
def findTag(folder, pattern, idx=0, provider="default"):
	import system
	if provider != None and provider != "":
		provider = "[%s]" % (provider)
	parentPath = "%s%s" % (provider, folder)
	tagPath = "*%s" % (pattern)
	tags=system.tag.browseTags(parentPath=parentPath,tagPath=tagPath,recursive=False,sort="ASC")
	if len(tags) <= idx:
		return "None"#None # Changed to avoid 'Tag properties can only occur after a tag name' error
	return tags[idx].fullPath


# --------------------------------------------------------------------------
# Squeeze the given tag path get something that looks a bit like
# an asset description.
# --------------------------------------------------------------------------
def getTagDescription(tagPath, skipFirstLevel=True):
	if tagPath == None: return None
	if len(tagPath) > 0:
		import system
		ttip = system.tag.read("%s.Tooltip" % tagPath).value
		if ttip <> "": return ttip
	## Make a description from the folder/tag names in the tag path.
	ignoredFolders = ('Device', 'Devices', 'Process')
	foldersAndTags = tagPath.split("/")
	try:
		code = ""
		desc = ""
		lastWords = []
		firstLevel = True
		## Loop through the individual parts of the tag path. A part is a folder/tag in the
		## the tag path.
		for folderOrTag in foldersAndTags: 
			## Check if the top level folder should be excluded.
			if firstLevel:
				firstLevel = False
				if skipFirstLevel: continue
			## Check if the current folder/name is in the list of ignored folders.
			if folderOrTag in ignoredFolders: continue
			words = folderOrTag.split(" ")
			lw = len(words)
			## Check if the current folder/tag starts with a part of the asset code.
			if lw <= 1:
				try:
					tryAsInt = int(words[0])
					hasCodePart = True
				except: hasCodePart = False
			else: hasCodePart = (words[0] == words[0].upper())
			###
			## Some tag names are multiple words written as a single word with mixed-case. e.g. TagName.
			## Split names like this into multiple words.
			for w in range(lw):
				lttrs = []
				lastLower = False
				for l in words[w]:
					#TotalTimeTTL
					if lastLower and l.isupper():
						lttrs.append(' ')
					lttrs.append(l)
					lastLower = l.islower()
				words[w] = ''.join(lttrs)
			###
			i1 = 1 if hasCodePart else 0
			## If current tag/directory contains the description from the parent level,
			## then that substring will not be added to the description again.
			llw = len(lastWords)
			if lw >= llw and llw > 1:
				match = True
				for i in range(i1, llw):
					if lastWords[i] != words[i]:
						match = False
						break
				if match: i1 = llw
			lastWords = words
			## Add to the asset code part of the description
			if hasCodePart: code += words[0]
			## Add to the text part of the description
			if i1 < lw: desc = "%s %s" % (desc, ' '.join(words[i1:]))
		return code + desc
	except:
		return foldersAndTags[-1]


# --------------------------------------------------------------------------
# Get the path of the tag that a component's property is bound to.
# --------------------------------------------------------------------------
def getTagForProperty(component, property="value"):
	# Get the window for this component
	window = component
	while window != None and not window.class.simpleName in ("FPMIWindow", "VisionTemplate"):
		# Keep checking the parent until we find a window (or template)
		window = window.parent
		
	if window != None:		
		# Get the binding adapter on the window for the given component and property.
		ic = window.getInteractionController()
		adapter = ic.getPropertyAdapter(component, property)
		
		if adapter != None:
			# Simple Tag Binding
			if adapter.class.simpleName == "SimpleBoundTagAdapter":
				return adapter.getTagPathString()
			# Indirect Tag Binding
			elif adapter.class.simpleName == "IndirectTagBindingAdapter":
				# Parse the indirect tag path
				interactions = adapter.getInteractions()
				ss = [] # List of parts of the string that will be returned
				for p in adapter.getPathParts():
					idx = p.getRefIdx()
					if idx > 0:
						# Get the current value of indirect bindings
						ss.append(interactions[idx - 1].getQValue().value)
					else:
						# Get static strings
						ss.append(p.getString())
				return "".join(ss)
			# Property Binding
			elif adapter.class.simpleName == "SimpleBoundPropertyAdapter":
				interaction = adapter.getInteraction()
				# Recursively check if source property is bound to a tag
				return getTagForProperty(interaction.getSource(), interaction.getSourceProperty())
		
	# Return None if no tag binding was found
	return None


# --------------------------------------------------------------------------
# Check if any of the tags in the given areas (tagpaths) have active alarms.
# --------------------------------------------------------------------------
def checkAreaAlarms(areas, excluded=[]):
	import system
#	start = system.date.now().getTime()##########################################################
	if type(areas) is list:	areas = system.dataset.toDataSet(['Area'], [[a] for a in areas])
	areasList = []
	sources = []
	for r in range(areas.rowCount):
		area = areas.getValueAt(r, 0)
		if area == "": continue
		sources.append("*%s*" % area)
	areaAlarms = system.alarm.queryStatus(state=["ActiveUnacked", "ActiveAcked"], source=sources)
	for alarm in areaAlarms:
		tp = "/".join(alarm.getSource().toStringSimple().split("/")[1:-1]).lower()
		inc = True
		for ex in excluded:
			if ex != "" and tp.startswith(ex.lower()):
				inc = False
				break
		if inc:
#			print "checkAreaAlarms:",system.date.now().getTime()-start###########################
			return True
#	print "checkAreaAlarms:",system.date.now().getTime()-start###################################
	return False


# --------------------------------------------------------------------------
# Check if any device in the given areas (tagpaths) is in manual mode.
# --------------------------------------------------------------------------
def checkAreaMaintenance(areas, excluded=[]):
	import system
#	start = system.date.now().getTime()##########################################################
	if type(areas) is list:	areas = system.dataset.toDataSet(['Area'], [[a] for a in areas])
	tagPaths = []
	tagPath = "*/Device*/Control/M*"#"*/Control/Manual"
	# Find manual control tags
	for r in range(areas.rowCount):
		area = areas.getValueAt(r, 0)
		if area == "": continue
		btags = system.tag.browseTags(parentPath=area, tagPath=tagPath, recursive=True, sort="ASC")
		for tag in btags:
			if not tag.isFolder():
				if tag.name in ("Manual", "Mode", "ManSetpointCmd"):
					inc = True
					tp = tag.path.lower()
					for ex in excluded:
						if ex != "" and tp.startswith(ex.lower()):
							inc = False
							break
					if inc: tagPaths.append(tag.fullPath)
	# Get the values of the tags
	tags = system.tag.readAll(tagPaths)
	typeBool = type(False)
	typeInt = type(0)
	# Check the value of each tag to see if a device is in a manual mode.
	for tag in tags:
		tagType = type(tag.value)
		if (tagType == typeBool and tag.value) or (tagType == typeInt and tag.value < 2):
#			print "checkAreaMaintenance:",system.date.now().getTime()-start############################
			return True
#	print "checkAreaMaintenance:",system.date.now().getTime()-start####################################
	return False


# --------------------------------------------------------------------------
# Check if any device in the given areas (tagpaths) is faulted.
# --------------------------------------------------------------------------
def checkAreaFaults(areas, excluded=[]):
	import system
#	start = system.date.now().getTime()##########################################################
	if type(areas) is list:	areas = system.dataset.toDataSet(['Area'], [[a] for a in areas])
	tagPaths = []
	tagPath = "*/Device*/*Fault*"#Faults/*"
	# Find fault tags
	for r in range(areas.rowCount):
		area = areas.getValueAt(r, 0)
		if area == "": continue
		btags = system.tag.browseTags(parentPath=area, tagPath=tagPath, recursive=True, sort="ASC")
		for tag in btags:
			if not tag.isFolder():
				folder = tag.path.split("/")[-2]
				if folder in ("Fault", "Faults"): inc = (not tag.name in ("EmergencyStop", "Emergency Stop", "EStop", "E Stop", "Fault Reset", "FaultReset"))
				else: inc = str(tag.name).endswith("Fault")
				if inc:
					tp = tag.path.lower()
					for ex in excluded:
						if ex != "" and tp.startswith(ex.lower()):
							inc = False
							break
					if inc: tagPaths.append(tag.fullPath)
	# Get the values of the tags
	tags = system.tag.readAll(tagPaths)
	# Check the value of each tag to see if a device is faulted.
	for tag in tags:
		if tag.value:
#			print "checkAreaFaults:",system.date.now().getTime()-start#########################################
			return True
#	print "checkAreaFaults:",system.date.now().getTime()-start#################################################
	return False
	

# --------------------------------------------------------------------------
# Manual Recursive Tag Browse. 
# Iterates through several browseTags() calls in a single script.
# This helps prevent server timeouts by browsing individual folders at a time,
# spreading the workload over multiple calls. 
#
# This function was designed to run from a Gateway Scoped call.
# Client based calls (such as those triggered by a button press) should
# search in an asynchronous thread.
#
# A dictionary of UDT definitions is stored to speed up the search. When a 
# particular UDT type is found for the first time, the instance is stored
# in the dictionary. When a subsequent instance of the same UDT is found,
# the same structure from the dictionary will be used, instead of scanning
# the UDT tag contents each time.
# --------------------------------------------------------------------------
_udtDefs_={}
def browseTags(parentPath, tagPath="*", sort="NATIVE", recursive=False):
	from array import array
	from com.inductiveautomation.ignition.common.script.builtin.ialabs import BrowseTag
	## Create a result set of just tags.
	tagsInFolder = system.tag.browseTags(parentPath = parentPath, tagPath = tagPath, recursive=False, sort=sort)
	## Split the results into separate lists for basic tags and for UDT instances
	tagSet = array(BrowseTag)
	udtSet = array(BrowseTag)
	udtPaths = []
	for tag in tagsInFolder:
		if tag.isUDT():
			udtSet.append(tag)
			udtPaths.append("%s.UdtParentType" % tag.fullPath)
		#else:
		tagSet.append(tag) 
	if recursive:
		## Find out the UDT type of each UDT instance
		if len(udtPaths) > 0: udtTypes = system.tag.readAll(udtPaths)
		## Iterate through the UDTs...
		for t in range(len(udtSet)):
			tag = udtSet[t]
			udtType = udtTypes[t].value
			if not udtType in _udtDefs_:
				## Browse the UDT to get its structure. This becomes the definition for other instances of this UDT.
				udtTagSet = browseTags(tag.fullPath, tagPath, sort, recursive)
				_udtDefs_[udtType] = [udtTagSet, tag]
			else:
				## Get the structure definition we've already found and apply it to this tag.
				udtDef = _udtDefs_[udtType]
				defTagSet = udtDef[0]
				defTag = udtDef[1]
				udtTagSet = array(BrowseTag)
				## Create a new BrowseTag with parameters updated from the definition structure.
				for utag in defTagSet:
					udtTagSet.append(BrowseTag(utag.name, 
						utag.path.replace(defTag.path, tag.path), 
						utag.fullPath.replace(defTag.fullPath, tag.fullPath), 
						utag.type,
						0,#(utag.QUERY_TAG if utag.isDB() else 0) or (utag.EXPRESSION_TAG if utag.isExpression() else 0) or (utag.MEMORY_TAG if utag.isMemory() else 0),
						utag.dataType)
					)
			tagSet+=udtTagSet
		# Create a result set of just folders. We'll iterate over this set and call browseTags() again for the results.                                              
		folderSet = system.tag.browseTags(parentPath = parentPath, tagPath = '*', tagType = 'Folder', recursive=False, sort=sort)
		# Iterate through the folders...                                   
		for folder in folderSet:
			tagSet+=browseTags(folder.fullPath, tagPath, sort, recursive)
	# Return the list of tags.
	return tagSet
	
	
# --------------------------------------------------------------------------
# Browse Tag Config, with recursion into folders and UDTs.
# If not all the tags are being returned, set ignoreSubTagsConfig=True.
# This will take longer to execute though.
# --------------------------------------------------------------------------
class BrowseTag():
	def __init__(self, path, tagType, config, parameters): self.path = path; self.tagType = tagType; self.config = config; self.parameters = parameters
	def isFolder(self): return self.tagType == self.tagType.Folder
	def isUDT(self): return self.tagType == self.tagType.UDT_INST
def browseTagsConfig(parentPath, recursive=False, sort="NATIVE", ignoreSubTags=False):
	tags = []
	def __scanTag__(tag, path, params=None):
		tagPath = "%s/%s" % (path, tag.name)
		tagType = tag.tagType
		subTags = tag.getSubTags()
		if tagType == tagType.UDT_INST: params = tag.parameters
		tags = [BrowseTag(tagPath, tagType, tag, params)]
		if tagType in (tagType.Folder, tagType.UDT_INST) and (len(subTags) <= 0 or ignoreSubTags):
			subTags = system.tag.browseConfiguration(tagPath, recursive)
		for subTag in subTags: tags += __scanTag__(subTag, tagPath, params)
		return tags
	config = system.tag.browseConfiguration(parentPath, recursive)
	for tag in config: tags += __scanTag__(tag, parentPath)
	if sort == "ASC": tags.sort(key=lambda x:x.path)
	elif sort == "DES": tags.sort(key=lambda x:x.path, reverse=True)
	return tags

