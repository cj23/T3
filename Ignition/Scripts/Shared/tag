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
def getTagDescription(tagPath, skipFirst=True):
	if tagPath == None:
		return None
	if len(tagPath) > 0:
		import system
		ttip = system.tag.read("%s.Tooltip" % tagPath).value
		if ttip <> "":
			return ttip
	skippedParts = ('Device','Devices','Process') 
	desc = None
	parts = tagPath.split("/")
	try:
		code = ""
		desc = ""
		lastWords = []
		bracketLevel = 0
		firstPart = True
		for p in parts:
			if firstPart:
				firstPart = False
				if skipFirst:
					continue
			if p in skippedParts:
				continue
			words = p.split(" ")
			lw = len(words)
			
			hasCodePart = None
			if lw <= 1:
				hasCodePart = False
			else:
				hasCodePart = valid = (words[0] == words[0].upper())
				#try:
				#	intVal = int(words[0])
				#	hasCodePart = True
				#except:
				#	hasCodePart = False
				
			llw = len(lastWords)
			i1 = 1 if hasCodePart else 0
			## If current tag/directory contains the description from the parent level,
			## then that substring will not be added to the description again.
			if lw >= llw and llw > 1:
				match = True
				for i in range(i1, llw):
					if lastWords[i] != words[i]:
						match = False
						break
				if match:
					i1 = llw
			## Add to the code part of the description
			if hasCodePart:
				code += words[0]
			## Add to the text part of the description
			if i1 < lw:
				desc = "%s %s" % (desc, ' '.join(words[i1:]))
			lastWords = words
				
		desc = code + desc
	except:
		desc = parts[-1]
	return desc


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
