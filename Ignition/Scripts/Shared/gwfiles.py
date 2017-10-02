__dbName = "LocalDB"
__tableName = "Gateway_File_System"
#__idColumn = "ID"
#__parentIdColumn = "ParentID"
__pathColumn = "FilePath"
__nameColumn = "FileName"
__blobColumn = "Contents"
__typeColumn = "Type"
__createdColumn = "Created"
__modifiedColumn = "Modified"
__sizeColumn = "Size"

__skipAudit = True


def __init():
	__checkDB()
	

def __setupDB():
	#sql = "CREATE TABLE IF NOT EXISTS `%s` (`%s` INT AUTO_INCREMENT PRIMARY KEY, `%s` INT, `%s` VARCHAR(1024), `%s` VARCHAR(128), `%s` BLOB, `%s` SMALLINT DEFAULT 1, `%s` DATETIME DEFAULT CURRENT_TIMESTAMP, `%s` DATETIME ON UPDATE CURRENT_TIMESTAMP, UNIQUE KEY(`%s`,`%s`))" % ( 
	#	__tableName, __idColumn, __parentIdColumn, __pathColumn, __nameColumn, __blobColumn, __typeColumn, __createdColumn, __modifiedColumn, __parentIdColumn, __nameColumn)
	sql = "CREATE TABLE IF NOT EXISTS `%s` (`%s` VARCHAR(512), `%s` VARCHAR(128), `%s` BLOB, `%s` SMALLINT DEFAULT 1, `%s` DATETIME DEFAULT CURRENT_TIMESTAMP, `%s` DATETIME DEFAULT ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(`%s`,`%s`))" % ( 
		__tableName, __pathColumn, __nameColumn, __blobColumn, __typeColumn, __createdColumn, __modifiedColumn, __pathColumn, __nameColumn)
	#print sql
	#result = system.db.runUpdateQuery(query=sql, database=__dbName, skipAudit=__skipAudit)


def __checkDB():
	# If not exists:
	__setupDB()

	
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
	
def getPathAndName(filepath):
	return __getPathAndName(filepath)
		

# Writes a (binary) file to the Gateway database.
# If the file already exists its contents will be updated.
#
# String filepath: full path of file to write.
# byte[] data: file contents to write.
# return boolean: if file was successfully written.
def writeFile(filepath, data):
	(path, name) = __getPathAndName(filepath)
	makeDirectory(path, True)
	sql = "INSERT INTO `%s` (`%s`, `%s`, `%s`, `%s`) VALUES(?, ?, ?, 1) ON DUPLICATE KEY UPDATE `%s`=?" % (__tableName, __pathColumn, __nameColumn, __blobColumn, __typeColumn, __blobColumn)
	#try:
	result = system.db.runPrepUpdate(sql, [path, name, data, data], database=__dbName, skipAudit=__skipAudit)
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


## TODO: #########################################
# Make a directory/folder. 
#
# String filepath: full path of directory to create.
# return boolean: if directory was successfully made.
def makeDirectory(filepath, recursive=True):
	if recursive:
		return makeDirectoryRecursive(filepath)
	else:
		(path, name) = __getPathAndName(filepath)
		sql = "INSERT INTO `%s` (`%s`, `%s`, `%s`) VALUES(?, ?, 2) ON DUPLICATE KEY UPDATE `%s`=`%s`" % (__tableName, __pathColumn, __nameColumn, __typeColumn, __nameColumn, __nameColumn)
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
			sql = "INSERT INTO `%s` (`%s`, `%s`, `%s`) VALUES(?, ?, 2) ON DUPLICATE KEY UPDATE `%s`=`%s`" % (__tableName, __pathColumn, __nameColumn, __typeColumn, __nameColumn, __nameColumn)
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
	sql = "UPDATE `%s` SET `%s`=?, `%s`=? WHERE `%s`=? AND %s=?" % (__tableName, __pathColumn, __nameColumn, __pathColumn, __nameColumn)
	result = system.db.runPrepUpdate(sql, [destpath, destname, srcpath, srcname], database=__dbName, skipAudit=__skipAudit)
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
	sql = "UPDATE `%s` SET `%s`=? WHERE `%s`=? AND %s=?" % (__tableName, __nameColumn, __pathColumn, __nameColumn)
	result = system.db.runPrepUpdate(sql, [newname, srcpath, srcname], database=__dbName, skipAudit=__skipAudit)
	return result > 0


# Permanently delete a file on the Gateway database. 
# Deleting a directory will also delete its files and subdirectories.
#
# String filepath: full path of file to delete.
# return boolean: if file was successfully deleted.
def deletePermanent(filepath):
	if len(filepath) < 1:
		return None
	(path, name) = __getPathAndName(filepath)
	sql = "DELETE FROM `%s` WHERE ((`%s`=? AND `%s`=?) OR (`%s` LIKE ?))" % (__tableName, __pathColumn, __nameColumn, __pathColumn)
	result = system.db.runPrepUpdate(sql, [path, name, "%s%s" % (filepath, "%")], database=__dbName, skipAudit=__skipAudit)
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
	sql = "INSERT INTO `%s`(`%s`, `%s`, `%s`, `%s`) SELECT ?, ?, `%s`, `%s` FROM `%s` WHERE `%s`=? AND `%s`=?" % (__tableName, __pathColumn, __nameColumn, __typeColumn, __blobColumn, __typeColumn, __blobColumn, __pathColumn, __nameColumn)
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
	sql = "INSERT INTO `%s` (`%s`, `%s`, `%s`, `%s`) " % (__tableName, __pathColumn, __nameColumn, __typeColumn, __blobColumn)
	sql += "SELECT ? AS `%s`, ? AS `%s`, `%s`, `%s` FROM `%s` WHERE `%s`=? AND `%s`=? UNION SELECT " % (__pathColumn, __nameColumn, __typeColumn, __blobColumn, __tableName, __pathColumn, __nameColumn)
	sql += "CONCAT(?, SUBSTRING(`%s`, INSTR(`%s`, ?)+1)) " % (__pathColumn, __pathColumn)
	sql += "AS `%s`, `%s`, `%s`, `%s` FROM `%s` WHERE `%s` LIKE ?" % (__pathColumn, __nameColumn, __typeColumn, __blobColumn, __tableName, __pathColumn)
	args = [destpath, destname, srcpath, srcname,
			destpath, "/%s/" % (srcname),
			"%s%s/%s" % (srcpath, srcname, '%')]
	#result = system.db.runPrepUpdate(sql, args, database=__dbName, skipAudit=__skipAudit)
	for a in args:
		sql = sql.replace('?', "'%s'" % a, 1)
	return sql
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
	sql = "SELECT `%s`,`%s`,`%s`,`%s`,`%s` FROM `%s` WHERE `%s` LIKE ?" % (__pathColumn, __nameColumn, __typeColumn, __createdColumn, __modifiedColumn, __tableName, __pathColumn)
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
	sql = "SELECT TRIM(BOTH '/' FROM `%s`) AS `%s`,`%s`,`%s`,`%s`,`%s` FROM `%s` WHERE `%s` LIKE ? ORDER BY `%s` DESC, `%s` ASC" % (__pathColumn, __pathColumn, __nameColumn, __typeColumn, __createdColumn, __modifiedColumn, __tableName, __pathColumn, __typeColumn, __nameColumn)
	result = system.db.runPrepQuery(sql, ["%s%s" % (path, '%' if recursive else '')], database=__dbName)
	return result


# Get a dataset of file items in the given path that is useful
# for creating a file browser in a Table component. 
#
# String path: full path to directory to search.
# return PyDataset: dataset of file items in the given path.
def getTableViewDataset(path):
	#files = getFilesDataset(path)
	
	sql = "SELECT TRIM(BOTH '/' FROM `%s`) AS `%s`,`%s`,`%s`,`%s`,`%s`, LENGTH(`%s`) AS `%s` FROM `%s` WHERE `%s` LIKE ? ORDER BY `%s` DESC, `%s` ASC" % (__pathColumn, __pathColumn, __nameColumn, __typeColumn, __createdColumn, __modifiedColumn, __blobColumn, __sizeColumn, __tableName, __pathColumn, __typeColumn, __nameColumn)
	files = system.db.runPrepQuery(sql, [path], database=__dbName)
	
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
	headers = "path","text","icon","background","foreground","tooltip","border","selectedText","selectedIcon","selectedBackground","selectedForeground","selectedTooltip","selectedBorder"
	return system.dataset.toDataSet(headers, ds)


def __showModal(title, defaultPath):
	from javax.swing import JDialog
	window = system.nav.openWindowInstance('GWFileBrowser', {'Title' : title, 'InitialPath': defaultPath})
	system.nav.centerWindow(window)
	rc = window.getRootContainer()
	cp = window.getContentPane()
	window.setVisible(False)
	dlg = JDialog(None, True)
	dlg.setContentPane(cp)
	dlg.setSize(window.getWidth(), window.getHeight())
	dlg.setMinimumSize(window.getMinimumSize())
	dlg.setMaximumSize(window.getMaximumSize())
	dlg.setLocation(window.getX(), window.getY())
	dlg.setLocationRelativeTo(None)
	dlg.setTitle(window.getTitle())
	dlg.setVisible(True)
	system.nav.closeWindow(window)
	return rc.Result
	
#
def openFile(extension=None, defaultLocation="/"):
	return __showModal('Open', defaultLocation)
	
#
def saveFile(filename="/", extension=None, typeDesc=None):
	return __showModal('Save', filename)
	
def getUserPathAndName(filepath):
	(path, name) = __getPathAndName(filepath)
	filepath = "%s%s" % (path, name)
	if fileExists(path):
		return (path, name)
	return ("/", "")
	
__init()
