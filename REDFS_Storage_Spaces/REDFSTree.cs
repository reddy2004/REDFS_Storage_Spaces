using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using DokanNet;
using Newtonsoft.Json;

namespace REDFS_ClusterMode
{
    /*
     * Notice how we keep the information needed to display the tree structure of the filesystem
     * and in each object of REDFSInode, we have the object RedFS_Inode which has the actual dbns and layout
     * of the file in the dbn space.
     */
    public class REDFSTree
    {
        IDictionary inodes = new Dictionary<string, REDFSInode>();
        int thisFsid = 0;

        REDFSCore   redfsCoreLocalCopy = null;                        /* If null, then just operate without storing any real data*/

        public REDFSTree()
        {
            //Root of the file system
            inodes["\\"] = new REDFSInode(true, null, "\\");
        }

        public REDFSTree(RedFS_FSID fsid, REDFSCore rfscore)
        {
            //Root of the file system
            inodes["\\"] = new REDFSInode(true, null, "\\");
            thisFsid = fsid.get_fsid();

            //fsidLocalCopy = fsid;
            redfsCoreLocalCopy = rfscore;

            RedFS_Inode inowip = fsid.get_inode_file_wip("load");
            DEFS.ASSERT(inowip.get_filesize() == (long)128 * 1024 * 1024 * 1024, "inowip size mismatch " + inowip.get_filesize());
        }

        public void CreateRootDirectoryWip(RedFS_FSID fsid)
        {
            RedFS_Inode wip = new RedFS_Inode(WIP_TYPE.DIRECTORY_FILE, 2, -1);
            wip.set_filesize(0);
            wip.is_dirty = true;

            RedFS_Inode inowip = fsid.get_inode_file_wip("CTreeinit");
            DEFS.ASSERT(inowip.get_filesize() == (long)128 * 1024 * 1024 * 1024, "inowip size mismatch " + inowip.get_filesize());
            redfsCoreLocalCopy.redfs_checkin_wip(inowip, wip, wip.get_ino()); //just commit some basic wip 

            //Write out the oddi data after loading the wip we just wrote

            ((REDFSInode)inodes["\\"]).LoadWipForExistingInode(redfsCoreLocalCopy, inowip, 2, -1);

            //should write out the dir json
            ((REDFSInode)inodes["\\"]).isDirty = true;
            ((REDFSInode)inodes["\\"]).SyncInternal(inowip, redfsCoreLocalCopy, inodes);

            redfsCoreLocalCopy.sync(inowip);
            redfsCoreLocalCopy.flush_cache(inowip, false);

            long rootDirSize = ((REDFSInode)inodes["\\"]).myWIP.get_filesize();
            DEFS.ASSERT(0 != rootDirSize, "Created a wip for a new rootdir in new fsid, but size is ro zero!");

            //test
            byte[] buffer = new byte[rootDirSize];
            redfsCoreLocalCopy.redfs_read(((REDFSInode)inodes["\\"]).myWIP, 0, buffer, 0, buffer.Length);
            string result = System.Text.Encoding.UTF8.GetString(buffer);
        }

        public void LoadRootDirectory(RedFS_FSID fsid)
        {
            REDFSInode rootDir = ((REDFSInode)inodes["\\"]);
            RedFS_Inode inowip = fsid.get_inode_file_wip("load root dir");

            rootDir.LoadWipForExistingInode(redfsCoreLocalCopy, inowip, 2, -1);

            long inoFileSize = inowip.get_filesize();

            byte[] buffer = new byte[rootDir.myWIP.get_filesize()];
            redfsCoreLocalCopy.redfs_read(rootDir.myWIP, 0, buffer, 0, buffer.Length);

            string result = System.Text.Encoding.UTF8.GetString(buffer);
            OnDiskDirectoryInfo oddi = JsonConvert.DeserializeObject<OnDiskDirectoryInfo>(result);

            foreach (OnDiskInodeInfo item in oddi.inodes)
            {
                rootDir.items.Add(item.fileInfo.FileName);
            }
        }

        private Boolean isRootOfTree(string path)
        {
            return (path == "\\");
        }

        private string GetParentPath(string fullPath, out string finalComponent)
        {
            if (fullPath == "\\")
            {
                finalComponent = "";
                return null;
            }
            else
            {
                string[] components = fullPath.Split("\\");
                //string.Join(",", components);
                string[] parent = new string[components.Length - 2];
                for (var i = 1; i < components.Length - 1; i++)
                {
                    parent[i - 1] = components[i];
                }
                if (parent.Length == 0)
                {
                    finalComponent = components[1];
                    return "\\";
                }
                else
                {
                    finalComponent = components[components.Length - 1];
                    return "\\" + string.Join("\\", parent);
                }
            }
        }

        /*
         * Load the directory does the following,
         * IF directory in not incore - read it from the filesystem and populate the dictionary.
         * Load all the file name and meta info of this directory and populate the dictionlary
         */
        private Boolean LoadDirectory(string path)
        {
            if (inodes.Contains(path) || path == "\\")
            {
                //Load all the files/dir inside this directory in the dictionary and return
                //XX It could be the case that directory is present but not its children.
                return true;
            }
            else
            {
                string firstComponent;
                //Now this directory in not incore. Lets load the parent first.
                if (LoadDirectory(GetParentPath(path, out firstComponent)))
                {
                    //We dont know if this directory is present at all in the parent path
                    return inodes.Contains(path);
                }
                else
                {
                    return false;
                }
            }
        }

        public REDFSInode GetInode(string fullPath)
        {
            if (inodes.Contains(fullPath))
            {
                return (REDFSInode)inodes[fullPath];
            }
            else
            {
                string firstComponent;
                //Load directory should suceed provided that its in the filesystem.
                if (LoadDirectory(GetParentPath(fullPath, out firstComponent)))
                {
                    return GetInode(fullPath);
                }
                else
                {
                    //Parent directory does not exist in the system, so unlikely that this file exists
                    return null;
                }
            }
        }

        private Boolean CreateFileInternal(RedFS_FSID fsid, string inFolder, string fileName)
        {
            REDFSInode newFile = new REDFSInode(false, inFolder, fileName);
            REDFSInode directory = (REDFSInode)inodes[inFolder];

            RedFS_Inode inowip = fsid.get_inode_file_wip("CreateFileInternal");

            int newInodeNum = redfsCoreLocalCopy.NEXT_INODE_NUMBER(fsid);

            newFile.CreateWipForNewlyCreatedInode(fsid.get_fsid(), newInodeNum, directory.myWIP.get_ino());
            newFile.isDirty = true;

            DEFS.ASSERT(newFile.isDirty == true, "New file should be marked dirty");
            DEFS.ASSERT(newFile.myWIP.is_dirty == true, "New file wip should also be dirty!");

            if (inFolder == "\\")
            {
                inodes.Add("\\" + fileName, newFile);
            }
            else
            {
                inodes.Add(inFolder + "\\" + fileName, newFile);
            }
            if (!directory.items.Contains(fileName))
            {
                directory.AddNewInode(fileName);
                directory.isDirty = true;
            }

            //It will be marked dirty at time of syncTree also when we checkin the wip
            //at this point, ino wip is not actually modified. During syncTree, the tree item (object)
            //REDFSInode is cleaned by writing out the content into the FS and pointers are updated in
            //RedFS_Inode, and the object (RedFS_Inode), i.e wip, is marked dirty. We write out this
            //RedFS_Inode and then checkin the wip (RedFS_Inode) to inode file and inode files is marked
            //dirty thereafter.
            inowip.is_dirty = true;
            return true;
        }

        private Boolean CreateDirectoryInternal(RedFS_FSID fsid, string inFolder, string dirName)
        {
            REDFSInode newDir = new REDFSInode(true, inFolder, dirName);
            REDFSInode directory = (REDFSInode)inodes[inFolder];

            RedFS_Inode inowip = fsid.get_inode_file_wip("CreateDirectoryInternal");

            int newInodeNum = redfsCoreLocalCopy.NEXT_INODE_NUMBER(fsid);

            newDir.CreateWipForNewlyCreatedInode(fsid.get_fsid(), newInodeNum, directory.myWIP.get_ino());
            newDir.isDirty = true;

            directory.AddNewInode(dirName);

            if (inFolder == "\\")
            {
                inodes.Add("\\" + dirName, newDir);
            }
            else
            {
                inodes.Add(inFolder + "\\" + dirName, newDir);
            }

            if (!directory.items.Contains(dirName))
            {
                directory.AddNewInode(dirName);
                directory.isDirty = true;
            }

            //It will be marked dirty at time of syncTree also when we checkin the wip
            //at this point, ino wip is not actually modified. During syncTree, the tree item (object)
            //REDFSInode is cleaned by writing out the content into the FS and pointers are updated in
            //RedFS_Inode, and the object (RedFS_Inode), i.e wip, is marked dirty. We write out this
            //RedFS_Inode and then checkin the wip (RedFS_Inode) to inode file and inode files is marked
            //dirty thereafter.
            inowip.is_dirty = true;
            return true;
        }

        public Boolean CreateFile(RedFS_FSID fsid, string filePath)
        {
            string firstComponent;
            string inDir = GetParentPath(filePath, out firstComponent);
            Console.WriteLine("CreateFile: " + filePath + " Log: Attempting to create new file [" + firstComponent + "] in " + inDir);
            return CreateFile(fsid, inDir, firstComponent);
        }

        public Boolean MoveInode(RedFS_FSID fsid, string srcPath, string destPath, bool replace, bool isDirectory)
        {
            if (!replace && FileExists(destPath))
            {
                return false;
            }
            else if (FileExists(srcPath) && !isDirectory)
            {
                //src file exists, so copy this over to the destFile
                string srcFileName;
                string destFileName;
                string srcParent = GetParentPath(srcPath, out srcFileName);
                string destParent = GetParentPath(destPath, out destFileName);

                if (((REDFSInode)inodes[srcParent]).RemoveInodeNameFromDirectory(srcFileName))
                {
                    //copy inode we want to move
                    REDFSInode srcInode = (REDFSInode)inodes[srcPath];

                    //edit the inodes parent directory and new name
                    srcInode.parentDirectory = destParent;
                    srcInode.fileInfo.FileName = destFileName;

                    //remove from our main inodes list
                    inodes.Remove(srcPath);

                    //Add this to the destination parent with the new name
                    ((REDFSInode)inodes[destParent]).items.Add(destFileName);

                    //Create a new entry in our main inodes list
                    inodes.Add(destPath, srcInode);
                    return true;
                }

                return false;
            }
            else if (!replace && DirectoryExists(destPath))
            {
                //First create the destination directory. then recursively move each item.


                return false;
            }
            else if (DirectoryExists(destPath) && isDirectory)
            {
                //src file exists, so copy this over to the destFile
                string srcFileName;
                string destFileName;
                string srcParent = GetParentPath(srcPath, out srcFileName);
                string destParent = GetParentPath(destPath, out destFileName);

                if (((REDFSInode)inodes[srcParent]).RemoveInodeNameFromDirectory(srcFileName))
                {
                    //copy inode we want to move
                    REDFSInode srcInode = (REDFSInode)inodes[srcPath];

                    //edit the inodes parent directory and new name
                    srcInode.parentDirectory = destParent;
                    srcInode.fileInfo.FileName = destFileName;

                    //remove from our main inodes list
                    inodes.Remove(srcPath);

                    //Add this to the destination parent with the new name
                    ((REDFSInode)inodes[destParent]).items.Add(destFileName);

                    //Create a new entry in our main inodes list
                    inodes.Add(destPath, srcInode);

                    //XXXX We are not yet done, we have to update parent path for all its children
                    throw new NotImplementedException();
                    return true;
                }
                return false;
            }
            else if (!DirectoryExists(destPath) && isDirectory && !replace)
            {
                //XXX todo, recursive approach
                CreateDirectory(fsid, destPath);
                IList<FileInformation> files = FindFilesWithPattern(srcPath, "*");
                foreach (FileInformation f in files)
                {
                    if (f.Attributes.HasFlag(FileAttributes.Directory))
                    {
                        MoveInode(fsid, srcPath + "\\" + f.FileName, destPath + "\\" + f.FileName, false, true);
                    }
                    else
                    {
                        MoveInode(fsid, srcPath + "\\" + f.FileName, destPath + "\\" + f.FileName, false, false);
                    }
                }
                return false;
            }
            else
            {
                return false;
            }
        }

        public Boolean CreateFile(RedFS_FSID fsid, string inFolder, string fileName)
        {
            if (!inodes.Contains(inFolder))
            {
                if (!LoadDirectory(inFolder))
                {
                    //parent directory where we want to create file is not present
                    return false;
                }
                //Parent directory is present and loaded.
                return CreateFileInternal(fsid, inFolder, fileName);
            }
            else
            {
                //parent folder is incore
                return CreateFileInternal(fsid, inFolder, fileName);
            }
        }
        public Boolean CreateDirectory(RedFS_FSID fsid, string dirPath)
        {
            string firstComponent;
            string inDir = GetParentPath(dirPath, out firstComponent);
            Console.WriteLine("Attempting to create new directory [" + firstComponent + "] in " + inDir);
            return CreateDirectory(fsid, inDir, firstComponent);
        }

        public Boolean CreateDirectory(RedFS_FSID fsid, string inFolder, string directoryName)
        {
            if (!inodes.Contains(inFolder))
            {
                if (!LoadDirectory(inFolder))
                {
                    //parent directory where we want to create file is not present
                    return false;
                }
                //Parent directory is present and loaded.
                return CreateDirectoryInternal(fsid, inFolder, directoryName);
            }
            else
            {
                //parent folder exists incore
                return CreateDirectoryInternal(fsid, inFolder, directoryName);
            }
        }

        public Boolean DeleteFile(string path)
        {
            if (LoadInode(path))
            {
                string finalComponent;
                string parent = GetParentPath(path, out finalComponent);
                REDFSInode rfi = (REDFSInode)inodes[parent];
                rfi.items.Remove(finalComponent);
                inodes.Remove(path);
                return true;
            }
            else
            {
                return false;
            }
        }

        public Boolean DeleteDirectoryInCleanup(string path)
        {
            if (LoadInode(path))
            {
                return DeleteDirectory(path);
            }
            else
            {
                //Already deleted
                return true;
            }
        }

        public Boolean DeleteDirectory(string path)
        {
            if (LoadInode(path))
            {
                string finalComponent;
                string parent = GetParentPath(path, out finalComponent);
                REDFSInode rfi = (REDFSInode)inodes[parent];
                if (!rfi.isDirectory())
                {
                    throw new NotImplementedException();
                }

                REDFSInode rfit = (REDFSInode)inodes[path];
                List<string> clist = (List<string>)rfit.ListFilesWithPattern("*");

                //first copy the list
                List<string> children = new List<string>();
                foreach (var c in clist)
                {
                    children.Add(c);
                }

                foreach (var child in children)
                {
                    string childpath = path + "\\" + child;
                    if (DirectoryExists(childpath))
                    {
                        DeleteDirectory(childpath);
                    }
                    else if (FileExists(childpath))
                    {
                        DeleteFile(childpath);
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                }

                rfi.items.Remove(finalComponent);
                inodes.Remove(path);
                return true;
            }
            else
            {
                return false;
            }
        }

        public Boolean SetAttributes(string path, FileAttributes newAttr)
        {
            if (LoadInode(path))
            {
                REDFSInode rfi = (REDFSInode)inodes[path];
                rfi.SetAttributes(newAttr);
                return true;
            }
            else
            {
                return false;
            }
        }

        public Boolean FlushFileBuffers(string filePath)
        {
            if (LoadInode(filePath))
            {
                REDFSInode rfi = (REDFSInode)inodes[filePath];
                rfi.FlushFileBuffers();
                return true;
            }
            else
            {
                return false;
            }
        }

        public FileInformation getFileInformationOfRootDir()
        {
            return ((REDFSInode)inodes["\\"]).fileInfo;
        }

        public FileInformation GetFileInformationStruct(string path)
        {
            //XX todo, load it first
            if (LoadInode(path))
            {
                return ((REDFSInode)inodes[path]).fileInfo;
            }
            throw new SystemException();
        }

        public Boolean SetAllocationSize(string fileName, long length, Boolean IsDirectory)
        {
            if (IsDirectory)
            {
                return true;
            }
            else
            {
                if (LoadInode(fileName))
                {
                    ((REDFSInode)inodes[fileName]).fileInfo.Length = length;
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public Boolean DirectoryExists(string dirPath)
        {
            if (inodes.Contains(dirPath))
            {
                REDFSInode i = (REDFSInode)inodes[dirPath];
                return i.isDirectory();
            }
            return false;
        }

        public Boolean FileExists(string filePath)
        {
            if (inodes.Contains(filePath))
            {
                REDFSInode i = (REDFSInode)inodes[filePath];
                return (i.isDirectory() == false);
            }
            return false;
        }

        public IList<FileInformation> FindFilesWithPattern(string dirPath, string pattern)
        {
            //In which directory? First load the directory since we want the FI of its children too
            //LoadDirectory(?)
            Boolean isSearchInRootFolder = false;
            if (dirPath == "\\")
            {
                //We are looking at the root directory;
                isSearchInRootFolder = true;
            }

            REDFSInode rfi = (REDFSInode)inodes["\\"];

            if (isSearchInRootFolder)
            {
                IList<FileInformation> lfi = new List<FileInformation>();

                IList<string> names = rfi.ListFilesWithPattern(pattern);

                foreach (var str in names)
                {
                    lfi.Add(((REDFSInode)inodes["\\" + str]).fileInfo);
                }
                return lfi;
            }
            else
            {
                if (LoadInode(dirPath))
                {
                    IList<FileInformation> lfi = new List<FileInformation>();
                    REDFSInode targetDir = (REDFSInode)inodes[dirPath];
                    IList<string> names = targetDir.ListFilesWithPattern(pattern);
                    foreach (var str in names)
                    {
                        lfi.Add(((REDFSInode)inodes[dirPath + "\\" + str]).fileInfo);
                    }
                    return lfi;
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        public Boolean WriteFile(string fileName, byte[] buffer, out int bytesWritten, long offset)
        {
            if (LoadInode(fileName))
            {
                REDFSInode rfi = (REDFSInode)inodes[fileName];
                return rfi.WriteFile(buffer, out bytesWritten, offset);
            }
            else
            {
                bytesWritten = 0;
                return false;
            }
        }

        public Boolean ReadFile(string fileName, byte[] buffer, out int bytesRead, long offset)
        {
            if (LoadInode(fileName))
            {
                return ((REDFSInode)inodes[fileName]).ReadFile(buffer, out bytesRead, offset);
            }
            else
            {
                bytesRead = 0;
                return false;
            }
        }

        private Boolean LoadInode(string fullPath)
        {
            if (inodes[fullPath] == null)
            {
                return false;
            }
            else
            {
                return true;
            }

        }

        public Boolean SetEndOfFile(string fileName, long length, bool preAlloc)
        {
            if (LoadInode(fileName))
            {
                if (redfsCoreLocalCopy != null)
                {
                    return ((REDFSInode)inodes[fileName]).SetEndOfFile(redfsCoreLocalCopy, length, preAlloc);
                }
                else
                {
                    return ((REDFSInode)inodes[fileName]).SetEndOfFile(length);
                }
            }
            return false;
        }

        public Boolean SetFileAttributes(string fileName, FileAttributes attributes)
        {
            return true;
        }

        public bool SetFileTime(string fileName, DateTime? creationTime, DateTime? lastAccessTime, DateTime? lastWriteTime)
        {
            return true;
        }

        public long getNumInodesInTree()
        {
            return inodes.Count;
        }

        public void PrintContents()
        {
            Console.WriteLine("Printing contents of REDFSTree.");
            foreach (DictionaryEntry kvp in inodes)
            {
                string content = ((REDFSInode)kvp.Value).printSelf();
                Console.WriteLine("Key = {0}, Value = {1}", kvp.Key, content);
            }
            Console.WriteLine("Printing contents of REDFSTree. [DONE]");
        }

        public void SyncTree(RedFS_FSID fsid)
        {
            lock (inodes)
            {
                DEFS.ASSERT(fsid.get_fsid() == thisFsid, "Fsid mismatch in synctree");
                RedFS_Inode inowip = fsid.get_inode_file_wip("Sync");
                RedFS_Inode imapwip = fsid.get_inodemap_wip();

                //Instead of walking the dictionary, lets walk the tree, DFS and write out nodes
                REDFSInode rootdir = (REDFSInode)inodes["\\"];

                rootdir.SyncInternal(inowip, redfsCoreLocalCopy, inodes);
                redfsCoreLocalCopy.sync(inowip);
                redfsCoreLocalCopy.sync(imapwip);
                redfsCoreLocalCopy.flush_cache(inowip, false);
                redfsCoreLocalCopy.flush_cache(imapwip, false);

                //very important for imap and inode files to be writtin out correctly
                redfsCoreLocalCopy.redfs_commit_fsid(fsid);
            }
        }

    }
}
