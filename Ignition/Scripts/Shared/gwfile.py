
__dbName = "LocalDB" # Set this to the name of your database connection.

__skipAudit = True # Set this to exclude sql queries from audits.

# Nothing below here should need to change.
__tableName = "Gateway_File_System"
__pathColumn = "FilePath"
__nameColumn = "FileName"
__blobColumn = "Contents"
__typeColumn = "Type"
__createdColumn = "Created"
__modifiedColumn = "Modified"
__uniqueColumn = "Unique"
__sizeColumn = "Size"


def __init():
	__checkDB()

def __checkDB():
	# If not exists:
	__setupDB()	

def __setupDB():
	sql = "CREATE TABLE IF NOT EXISTS `%s` (`%s` VARCHAR(512), `%s` VARCHAR(128), `%s` BLOB, `%s` SMALLINT DEFAULT 1, `%s` DATETIME DEFAULT CURRENT_TIMESTAMP, `%s` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, `%s` BINARY(32), PRIMARY KEY(`%s`))" % ( 
		__tableName, __pathColumn, __nameColumn, __blobColumn, __typeColumn, __createdColumn, __modifiedColumn, __uniqueColumn, __uniqueColumn)
	result = system.db.runUpdateQuery(query=sql, database=__dbName, skipAudit=__skipAudit)

def __printQuery(sql, args=[]):
	for a in args:
		sql = sql.replace("?", "'%s'" % (a), 1)
	#print sql

def __getPathAndName(filepath):
	if len(filepath) > 512:
		raise ValueError('File path is too long.')
	if len(filepath) < 1:
		filepath = "/"
	elif filepath[0] <> "/":
		filepath = "/%s" % (filepath)
	if filepath[-1] == "/":
		filepath = filepath[:-1]
	fp = filepath.split("/")
	path = "/".join(fp[:-1]) + "/"	
	name = fp[-1]
	return (path, name)

# Split a filepath into the actual path and item name.
#
# String filepath: full path to file.
# return (String, String): (path, name) tuplet.
def getPathAndName(filepath):
	return __getPathAndName(filepath)
	

# Writes a (binary) file to the Gateway database.
# If the file already exists its contents will be updated.
#
# String filepath: full path of file to write.
# String / byte[] data: file contents to write.
# return boolean: if file was successfully written.
def writeFile(filepath, data, append=False):
	(path, name) = __getPathAndName(filepath)
	if type(data) in ('str', 'java.lang.String'):
		from java.lang import String
		data = String(data).getBytes()
	makeDirectory(path, True)
	tx = system.db.beginTransaction()
	sql = "SET @data=?"
	result = system.db.runPrepUpdate(sql, [data], database=__dbName, tx=tx, skipAudit=__skipAudit)
	sql = "INSERT INTO `%s` (`%s`, `%s`, `%s`, `%s`, `%s`) VALUES(?, ?, @data, 1, MD5(CONCAT(`%s`, `%s`))) ON DUPLICATE KEY UPDATE `%s`=CONCAT(IF(NOT %s OR ISNULL(`%s`),'',`%s`),@data)" % (
		__tableName, __pathColumn, __nameColumn, __blobColumn, __typeColumn, __uniqueColumn, __pathColumn, __nameColumn, __blobColumn, bool(append), __blobColumn, __blobColumn)
	#try:
	result = system.db.runPrepUpdate(sql, [path, name], database=__dbName, tx=tx, skipAudit=__skipAudit)
	system.db.commitTransaction(tx)
	system.db.closeTransaction(tx)
	return result > 0
	#except PacketTooBigException:
	#	raise ValueError('File size is too large.')


# Load a (binary) file from the Gateway database.
#
# String filepath: full path of file to read.
# return byte[]: file contents.
def readFileAsBytes(filepath):	
	(path, name) = __getPathAndName(filepath)
	sql = "SELECT `%s` FROM `%s` WHERE `%s`=? AND `%s`=?" % (__blobColumn, __tableName, __pathColumn, __nameColumn)
	result = system.db.runPrepQuery(sql, [path, name], database=__dbName)
	if result == None:
		return None
	if len(result) < 1:
		return None
	return result[0][__blobColumn]

	
# Load a (text) file from the Gateway database.
#
# String filepath: full path of file to read.
# return String: file contents.
def readFileAsString(filepath, encoding="UTF-8"):	
	blob = readFileAsBytes(filepath)
	if blob == None:
		return ""
	from org.apache.commons.io import IOUtils
	return IOUtils.toString(blob, encoding)#StandardCharsets.UTF_8)


# Download a file from the Gateway database to the client machine.
#
# String filepath: full path of file to download.
# String dlPath: full path to downloaded file.
# return boolean: if file was successfully downloaded.
def downloadFile(filepath, dlPath, overwrite=False):
	data = loadFile(filepath)
	if data <> None:
		if overwrite or not system.file.fileExists(dlPath):
			system.file.writeFile(dlPath, data.tolist())
			return True
	return False


# Make a directory/folder. 
#
# String filepath: full path of directory to create.
# return boolean: if directory was successfully made.
def makeDirectory(filepath, recursive=True):
	if recursive:
		return makeDirectoryRecursive(filepath)
	else:
		(path, name) = __getPathAndName(filepath)
		sql = "INSERT INTO `%s` (`%s`, `%s`, `%s`, `%s`) VALUES(?, ?, 2, MD5(CONCAT(`%s`, `%s`))) ON DUPLICATE KEY UPDATE `%s`=`%s`" % (
			__tableName, __pathColumn, __nameColumn, __typeColumn, __uniqueColumn, __pathColumn, __nameColumn, __nameColumn, __nameColumn)
		result = system.db.runPrepUpdate(sql, [path, name], database=__dbName, tx=tx, skipAudit=__skipAudit)
		return result > 0
	

# Make a directory/folder. 
# Each directory of the path will be added if non-existant.
#
# String filepath: full path of directory to create.
# return boolean: if directory was successfully made.
def makeDirectoryRecursive(filepath):
	if len(filepath) > 512:
		raise ValueError('File path is too long.')
	if len(filepath) < 1:
		return False
	fp = filepath.split("/")#[1:-1]
	path = "/"
	tx = system.db.beginTransaction(__dbName)
	result = 0
	#print filepath
	#print fp
	for name in fp:
		if len(name) > 0:
			sql = "INSERT INTO `%s` (`%s`, `%s`, `%s`, `%s`) VALUES(?, ?, 2, MD5(CONCAT(`%s`, `%s`))) ON DUPLICATE KEY UPDATE `%s`=`%s`" % (
				__tableName, __pathColumn, __nameColumn, __typeColumn, __uniqueColumn, __pathColumn, __nameColumn, __nameColumn, __nameColumn)
			#print sql, path, name
			result += system.db.runPrepUpdate(sql, [path, name], database=__dbName, tx=tx, skipAudit=__skipAudit)
			path = "%s%s/" % (path, name)
	system.db.commitTransaction(tx)
	system.db.closeTransaction(tx)
	return result > 0


# Check if a file exists on the Gateway database. 
#
# String filepath: full path of file to check.
# return boolean: if file exists.
def fileExists(filepath):
	(path, name) = __getPathAndName(filepath)
	sql = "SELECT 1 FROM `%s` WHERE `%s`=? AND `%s`=?" % (__tableName, __pathColumn, __nameColumn)
	result = system.db.runPrepQuery(sql, [path, name], database=__dbName)
	if result == None:
		return None
	return len(result) > 0


# Move a file to a new location. 
#
# String src: full path of file to move.
# String dest: full path of destination file.
# boolean makeDir: build directory path to destination file.
# return boolean: if file was successfully moved.
def move(src, dest, makeDir=True):
	(srcpath, srcname) = __getPathAndName(src)
	(destpath, destname) = __getPathAndName(dest)
	if makeDir:
		makeDirectory(destpath, True)
	tx = system.db.beginTransaction(__dbName)
	sql = "UPDATE `%s` SET `%s`=?, `%s`=?, `%s`=MD5(CONCAT(`%s`, `%s`)) WHERE `%s`=? AND `%s`=?" % (
		__tableName, __pathColumn, __nameColumn, __uniqueColumn, __pathColumn, __nameColumn, __pathColumn, __nameColumn)
	args = [destpath, destname, srcpath, srcname]
	__printQuery(sql, args)
	result = system.db.runPrepUpdate(sql, args, database=__dbName, tx=tx, skipAudit=__skipAudit)
	sql = "UPDATE `%s` SET `%s`=CONCAT(?, SUBSTRING(`%s`, LENGTH(?) + 1)), `%s`=MD5(CONCAT(`%s`, `%s`)) WHERE `%s` LIKE ?" % (
		__tableName, __pathColumn, __pathColumn, __uniqueColumn, __pathColumn, __nameColumn, __pathColumn)
	args = ["%s%s/" % (destpath, destname), "%s%s/" % (srcpath, srcname), "%s%s/%s" % (srcpath, srcname, '%')]
	__printQuery(sql, args)
	result += system.db.runPrepUpdate(sql, args, database=__dbName, tx=tx, skipAudit=__skipAudit)
	system.db.commitTransaction(tx)
	system.db.closeTransaction(tx)
	return result > 0


# Rename a file.
#
# String src: full path of file to move.
# String newname: new name to give file.
# return boolean: if file was successfully renamed.
def rename(src, newname):
	if len(src) < 1 or len(newname) < 1:
		return False
	(srcpath, srcname) = __getPathAndName(src)
	sql = "UPDATE `%s` SET `%s`=?, `%s`=MD5(CONCAT(`%s`, `%s`)) WHERE `%s`=? AND %s=?" % (
		__tableName, __nameColumn, __uniqueColumn, __pathColumn, __nameColumn, __pathColumn, __nameColumn)
	result = system.db.runPrepUpdate(sql, [newname, srcpath, srcname], database=__dbName, skipAudit=__skipAudit)
	return result > 0


# Permanently delete a file on the Gateway database. 
# Deleting a directory will also delete its files and subdirectories.
#
# String filepath: full path of file to delete.
# return boolean: if file was successfully deleted.
def deletePermanent(filepath):
	(path, name) = __getPathAndName(filepath)
	sql = "DELETE FROM `%s` WHERE ((`%s`=? AND `%s`=?) OR (`%s` LIKE ?))" % (__tableName, __pathColumn, __nameColumn, __pathColumn)
	args = [path, name, "%s%s/%s" % (path, name, "%")]
	__printQuery(sql, args)
	result = system.db.runPrepUpdate(sql, args, database=__dbName, skipAudit=__skipAudit)
	return result > 0


# Copy a file on the Gateway database. 
#
# String src: full path of file to copy.
# String dest: full path to pasted file.
# boolean recursive: copy subdirectories and files.
# boolean makeDir: build directory path to destination file.
# return boolean: if file was successfully copied.
def copy(src, dest, recursive=True, makeDir=True):
	if recursive:
		return copyRecursive(src, dest, makeDir)
	(srcpath, srcname) = __getPathAndName(src)
	(destpath, destname) = __getPathAndName(dest)
	if makeDir:
		makeDirectory(destpath, True)
	sql = "INSERT INTO `%s`(`%s`, `%s`, `%s`, `%s`, `%s`) SELECT ?, ?, `%s`, `%s`, MD5(CONCAT(`%s`, `%s`)) FROM `%s` WHERE `%s`=? AND `%s`=?" % (
		__tableName, __pathColumn, __nameColumn, __typeColumn, __blobColumn, __uniqueColumn, __typeColumn, __blobColumn, __pathColumn, __nameColumn, __pathColumn, __nameColumn)
	result = system.db.runPrepUpdate(sql, [destpath, destname, srcpath, srcname], database=__dbName, skipAudit=__skipAudit)
	return result > 0


# Copy a file on the Gateway database.
# Copying a directory will also copy its files and subdirectories.
#
# String src: full path of file to copy.
# String dest: full path to pasted file.
# return boolean: if file was successfully copied.
def copyRecursive(src, dest, makeDir=True):
	(srcpath, srcname) = __getPathAndName(src)
	(destpath, destname) = __getPathAndName(dest)
	#print srcpath, srcname
	#print destpath, destname
	#if len(destname) < 1:
		#destname = srcname	
	if makeDir:
		makeDirectory(destpath, True)	
	sql = "INSERT INTO `%s` (`%s`, `%s`, `%s`, `%s`, `%s`) " % (__tableName, __pathColumn, __nameColumn, __typeColumn, __blobColumn, __uniqueColumn)
	sql += "SELECT `%s`, `%s`, `%s`, `%s`, MD5(CONCAT(`%s`,`%s`)) AS `%s` FROM (" % (__pathColumn, __nameColumn, __typeColumn, __blobColumn, __pathColumn, __nameColumn, __uniqueColumn)
	sql += "SELECT ? AS `%s`, ? AS `%s`, `%s`, `%s` FROM `%s` WHERE `%s`=? AND `%s`=? UNION SELECT " % (__pathColumn, __nameColumn, __typeColumn, __blobColumn, __tableName, __pathColumn, __nameColumn)
	sql += "CONCAT(?, SUBSTRING(`%s`, LENGTH(?) + 1)) " % (__pathColumn)
	#sql += "CONCAT(?, SUBSTRING(`%s`, INSTR(`%s`, ?)+1)) " % (__pathColumn, __pathColumn)
	sql += "AS `%s`, `%s`, `%s`, `%s` FROM `%s` WHERE `%s` LIKE ? ) aaagwfilezzz" % (__pathColumn, __nameColumn, __typeColumn, __blobColumn, __tableName, __pathColumn)
	args = [destpath, destname, srcpath, srcname,
			#destpath, "/%s/" % (srcname),
			"%s%s/" % (destpath, destname), "%s%s/" % (srcpath, srcname),
			"%s%s/%s" % (srcpath, srcname, '%')]
	__printQuery(sql, args)
	result = system.db.runPrepUpdate(sql, args, database=__dbName, skipAudit=__skipAudit)
	#for a in args:
	#	sql = sql.replace('?', "'%s'" % a, 1)
	#print sql
	return result > 0


class File():
	path = None
	name = None
	type = None
	created = None
	modified = None
	
	def __init__(self, path, name, type=1, created=None, modified=None):
		self.path = path
		self.name = name
		self.type = type
		self.created = created
		self.modified = modified
	
	def getFullPath(self):
		return "%s/%s" % (self.path, self.name)
	
	def getPath(self):
		return self.path
	
	def getName(self):
		return self.name
		
	def isDirectory(self):
		return self.type == 2
	
	def getCreated(self):
		return self.created
	
	def getModifed(self):
		return self.modified	
		

# Get a list of file items in the given path. 
#
# String path: full path to directory to search.
# return File[]: list of file items in the given path.
def getFiles(path, recursive=False):
	sql = "SELECT `%s`,`%s`,`%s`,`%s`,`%s` FROM `%s` WHERE `%s` LIKE ?" % (
		__pathColumn, __nameColumn, __typeColumn, __createdColumn, __modifiedColumn, __tableName, __pathColumn)
	result = system.db.runPrepQuery(sql, ["%s%s" % (path, '%' if recursive else '')], database=__dbName)
	files = []
	for r in result:
		file = File(r[__pathColumn], r[__nameColumn], r[__typeColumn], r[__createdColumn], r[__modifiedColumn])
		files.append(file)
	return files


# Get a simple dataset of file items in the given path. 
#
# String path: full path to directory to search.
# return PyDataset: dataset of file items in the given path.
def getFilesDataset(path, recursive=True):
	sql = "SELECT `%s`,`%s`,`%s`,`%s`,`%s` FROM `%s` WHERE `%s` LIKE ? ORDER BY `%s` DESC, `%s` ASC" % (
		__pathColumn, __nameColumn, __typeColumn, __createdColumn, __modifiedColumn, __tableName, __pathColumn, __typeColumn, __nameColumn)
	result = system.db.runPrepQuery(sql, ["%s%s" % (path, '%' if recursive else '')], database=__dbName)
	return result


# Get a dataset of file items in the given path that is useful
# for creating a file browser in a Table component. 
#
# String path: full path to directory to search.
# return PyDataset: dataset of file items in the given path.
def getTableViewDataset(path, filter=""):
	#files = getFilesDataset(path)
	
	sql = "SELECT `%s`,`%s`,`%s`, LENGTH(`%s`) AS `%s` FROM `%s` WHERE `%s` LIKE ? AND (`%s`=2 OR LOWER(`%s`) LIKE ?) ORDER BY `%s` DESC, `%s` ASC" % (
		__nameColumn, __typeColumn, __modifiedColumn, __blobColumn, __sizeColumn, __tableName, __pathColumn, __typeColumn, __nameColumn, __typeColumn, __nameColumn)
	files = system.db.runPrepQuery(sql, [path, filter.lower()], database=__dbName)
	
	ds = []
	for f in files:
		row = [f[__typeColumn], f[__nameColumn], f[__sizeColumn], f[__modifiedColumn]]
		ds.append(row)
	return system.dataset.toDataSet(['Type', 'Name', 'Size', 'Date Modified'], ds)
	

# Get a dataset of file items in the given path that is useful
# for creating a file browser in a List component. 
#
# String path: full path to directory to search.
# return PyDataset: dataset of file items in the given path.
def getListViewDataset(path):
	#files = getFilesDataset(path)
	
	sql = "SELECT `%s` FROM `%s` WHERE `%s` LIKE ? ORDER BY `%s` DESC, `%s` ASC" % (__nameColumn, __tableName, __pathColumn, __typeColumn, __nameColumn)
	files = system.db.runPrepQuery(sql, [path], database=__dbName)
	
	ds = []
	for f in files:
		row = [f[__nameColumn]]
		ds.append(row)
	return system.dataset.toDataSet(['Name'], ds)


# Get a dataset of file items in the given path that is useful
# for creating a file browser in a Tree View component. 
#
# String path: full path to directory to search.
# return PyDataset: dataset of file items in the given path.
def getTreeViewDataset(path):
	files = getFilesDataset(path)
	ds = []
	for f in files:
		fp = "%s/%s" % (f[__pathColumn], f[__nameColumn])
		row = [f[__pathColumn], f[__nameColumn], "default","color(255,255,255,255)","color(0,0,0,255)","",None,"","default","color(250,214,138,255)","color(0,0,0,255)","",None]
		ds.append(row)
	headers = ["path","text","icon","background","foreground","tooltip","border","selectedText","selectedIcon","selectedBackground","selectedForeground","selectedTooltip","selectedBorder"]
	return system.dataset.toDataSet(headers, ds)


# Show the window as a modal dialog.
def __showModal(params=None):
	from javax.swing import JDialog
	windowName = 'GWFileBrowser'
	if windowName in system.gui.getWindowNames():
		window = system.nav.openWindowInstance(windowName, params)
		system.nav.centerWindow(window)
		rc = window.getRootContainer()
		#rc.load()
		cp = window.getContentPane()
		window.setVisible(False)
		dlg = JDialog(None, True)
		dlg.setContentPane(cp)
		dlg.setSize(window.getWidth(), window.getHeight())
		dlg.setMinimumSize(window.getMinimumSize())
		dlg.setMaximumSize(window.getMaximumSize())
		dlg.setLocation(window.getX(), window.getY())
		#dlg.setLocationRelativeTo(None)
		dlg.setTitle(window.getTitle())
		dlg.setVisible(True)
		system.nav.closeWindow(window)
		return rc.Result
	return None


# Dialog modifiers
DLG_DB_ICON = 1
DLG_DARK_BG = 2

# Get the scope that the module is called from.
def __getGlobalScope():
	from com.inductiveautomation.ignition.common.model import ApplicationScope
	scope = ApplicationScope.getGlobalScope()	
	if (ApplicationScope.isGateway(scope)):
		return 0
	if (ApplicationScope.isClient(scope)):
		return 1
	if (ApplicationScope.isDesigner(scope)):
		return 2
	return -1

# Open File dialog
#
# String extension: not implemented.
# String filename: default file location to select.
# return String: selected filepath, or None if cancelled.
def openFile(extension="", defaultLocation="/", modifiers=0):
	if __getGlobalScope() == 1:
		params = {'Title' : 'Open', 'InitialPath': defaultLocation, 'Modifiers': modifiers, 'ExtensionFilter':extension, 'ExtensionFilterName': ""}
		return __showModal(params)
	else:
		# The dialog can only be shown in the client scope.
		return None
	

# Save File dialog
#
# String filename: default file location to select.
# String extension: not implemented.
# String typeDesc: not implemented.
# return String: selected filepath, or None if cancelled.
def saveFile(filename="/", extension="", typeDesc="", modifiers=0):
	if __getGlobalScope() == 1:
		params = {'Title' : 'Save', 'InitialPath': filename, 'Modifiers': modifiers, 'ExtensionFilter':extension, 'ExtensionFilterName': typeDesc}
		return __showModal(params)
	else:
		# The dialog can only be shown in the client scope.
		return None

	
# Get path and name of exisiting location from user input.
#
# String filepath: user inputted filepath.
# return (String, String): (path,name) tuplet.
def getUserPathAndName(filepath):
	(path, name) = __getPathAndName(filepath)
	filepath = "%s%s" % (path, name)
	if fileExists(path):
		return (path, name)
	return ("/", "")



# Initialise. Called each time the module is loaded.
__init()
