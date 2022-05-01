using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using DokanNet;
using Newtonsoft.Json;

namespace REDFS_ClusterMode
{
    public class OnDiskInodeInfo
    {
        public FileInformation fileInfo;
        public int ino;
    }

    public class OnDiskDirectoryInfo
    {
        public int ino;
        public FileInformation fileInfo;
        public List<OnDiskInodeInfo> inodes;

        public OnDiskDirectoryInfo()
        {
            inodes = new List<OnDiskInodeInfo>();
        }
    }


    public class REDFSInode
    {
        //All file attributes and inode type for both file/dirs.
        public FileInformation fileInfo;
        public string parentDirectory;
        public Boolean isDirty;
        public int BLK_SIZE = OPS.FS_BLOCK_SIZE;

        public string cache_string;

        IDictionary inCoreData = new Dictionary<int, byte[]>();

        //Flags specific for directories
        public List<string> items = new List<string>(); //all files/dir names
        public Boolean isInodeSkeleton = false;

        private long m_creation_time;

        public RedFS_Inode myWIP;

        public REDFSInode(Boolean isDirectory, string parent, string name)
        {
            fileInfo = new FileInformation();
            fileInfo.FileName = name;
            if (isDirectory) {
                fileInfo.Attributes |= System.IO.FileAttributes.Directory;
            } else {
                fileInfo.Attributes |= System.IO.FileAttributes.Normal;
            }

            fileInfo.Length = 0;
           
            fileInfo.CreationTime = DateTime.Now;
            fileInfo.LastAccessTime = DateTime.Now;
            fileInfo.LastWriteTime = DateTime.Now;

            parentDirectory = parent;

            m_creation_time = DateTime.Now.ToUniversalTime().Ticks;
            /*
            if (name.Length >= 2 && name.Length <= 4 && name.IndexOf("d") == 0) { }
            else {
                DEFS.ASSERT(name == "\\" || name.IndexOf("\\") < 0, "Incorrect name in filename: " + name);
            }*/
        }

        public int get_inode_obj_age()
        {
            long elapsed = (DateTime.Now.ToUniversalTime().Ticks - m_creation_time);
            return (int)(elapsed / 10000000);
        }

        public void touch_inode_obj()
        {
            m_creation_time = DateTime.Now.ToUniversalTime().Ticks;
        }

        public void LoadWipForExistingInode(REDFSCore redfsCore, RedFS_Inode inowip, int ino, int pino)
        {
            if (fileInfo.Attributes.HasFlag(FileAttributes.Normal))
            {
                myWIP = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, ino, pino);
                redfsCore.redfs_checkout_wip(inowip, myWIP, ino);

                //mark as not dirty.
                myWIP.is_dirty = false;
                isDirty = false;

                //throw new SystemException("Not yet implimented!");
            }
            else
            {
                myWIP = new RedFS_Inode(WIP_TYPE.DIRECTORY_FILE, ino, pino);
                redfsCore.redfs_checkout_wip(inowip, myWIP, ino);

                //mark as not dirty.
                myWIP.is_dirty = false;
                isDirty = false;

                //Keep the json string in memory for debugging
                byte[] buffer = new byte[myWIP.get_filesize()];
                redfsCore.redfs_read(myWIP, 0, buffer, 0, buffer.Length);
                string jsonString = Encoding.UTF8.GetString(buffer);               
            }
            touch_inode_obj();
        }

        public void CreateWipForNewlyCreatedInode(int fsid, int ino, int pino)
        {
            if (fileInfo.Attributes.HasFlag(FileAttributes.Normal))
            {
                myWIP = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, ino, pino);
                myWIP.setfilefsid_on_dirty(fsid);
            }
            else
            {
                myWIP = new RedFS_Inode(WIP_TYPE.DIRECTORY_FILE, ino, pino);
                myWIP.setfilefsid_on_dirty(fsid);
            }
            myWIP.is_dirty = true;
            isDirty = true;
            touch_inode_obj();
        }
        public void InitializeDirectory(List<string> children)
        {
            if (isDirectory())
            {
                isInodeSkeleton = false;
                touch_inode_obj();
            }
            else
            {
                throw new SystemException();
            }
        }

        public void InitializeFile(byte[] fsInfoBlock)
        {
            if (!isDirectory())
            {
                isInodeSkeleton = false;
                touch_inode_obj();
            }
            else
            {
                throw new SystemException();
            }   
        }

        public string printSelf()
        {
            if (isDirectory())
                 return "[" + fileInfo.FileName + "] inside " + parentDirectory + " & contents=[" + String.Join(",", items) + "]";
            else
                return "[" + fileInfo.FileName + "] inside " + parentDirectory;

        }

        public Boolean isDirectory()
        {
            return fileInfo.Attributes.HasFlag(FileAttributes.Directory);
        }

        public Boolean AddNewInode(string fileName)
        {
            if (isDirectory())
            {
                items.Add(fileName);
                isDirty = true;
                touch_inode_obj();
                return true;
            }
            else
            {
                throw new SystemException();
            }
        }

        public IList<string> ListFilesWithPattern(string pattern)
        {
            if (pattern == "*")
                return items;
            else
            {
                IList<string> si = new List<string>();
                foreach (string item in items)
                {
                    if (item.IndexOf(pattern) == 0)
                        si.Add(item);
                }
                return si;
            }
        }

        public Boolean SetEndOfFile(long length)
        {
            if (isDirectory())
            {
                throw new NotSupportedException();
            }
            else
            {
                fileInfo.Length = length;
                myWIP.set_filesize(length);
                isDirty = true;
                touch_inode_obj();
                return true;
            }
        }

        public void SetAttributes(FileAttributes newAttr)
        {
            touch_inode_obj();
        }

        /*
         * When sync'd, thisdir will loose track of the file in this directory. Its its the responsibility
         * of the called to remove it from the incore inodes as well or we will simply hog up space in memory
         */ 
        public Boolean RemoveInodeNameFromDirectory(string fileName)
        {
            DEFS.ASSERT(isDirectory(), "Should be a directory, if we want to remove a file from it");
            foreach(string f in items)
            {
                if (f == fileName)
                {
                    items.Remove(fileName);
                    touch_inode_obj();
                    return true;
                }
            }
            return false;
        }

        private void ComputeBoundaries(int blocksize, long offset, int size, out int startfbn, out int startoffset, out int endfbn, out int endoffset)
        {
            startfbn = (int)(offset / blocksize);
            endfbn = (int)((offset + size) / blocksize);

            startoffset = (int)(offset % blocksize);
            endoffset = (int)((offset + size) % blocksize);
        }

        public Boolean WriteFile(byte[] buffer, out int bytesWritten, long offset)
        {
            int startfbn, startoffset, endfbn, endoffset;
            ComputeBoundaries(BLK_SIZE, offset, buffer.Length, out startfbn, out startoffset, out endfbn, out endoffset);

            int currentBufferOffset = 0;

            for (int fbn = startfbn; fbn <= endfbn; fbn++)
            {
                if (fbn == startfbn)
                {
                    if (!inCoreData.Contains(startfbn))
                    {
                        inCoreData.Add(startfbn, new byte[BLK_SIZE]);
                    }

                    byte[] data = (byte[])inCoreData[startfbn];
                    int tocopy = ((BLK_SIZE - startoffset) < buffer.Length) ? (BLK_SIZE - startoffset) : buffer.Length;
                    //Console.WriteLine("[currOffsetInInput, tocopy -> fbn, offsetInFBN ]" + currentBufferOffset + " copy -> " + startfbn + " , " + startoffset);
                    for (int k=0;k<tocopy; k++)
                    {
                        data[k + startoffset] = buffer[k + currentBufferOffset];
                    }
                    currentBufferOffset += tocopy;
                }
                else if (fbn == endfbn)
                {
                    if (!inCoreData.Contains(endfbn))
                    {
                        inCoreData.Add(endfbn, new byte[BLK_SIZE]);
                    }

                    byte[] data = (byte[])inCoreData[endfbn];
                    int tocopy = endoffset;
                    //Console.WriteLine("[currOffsetInInput, tocopy -> fbn, offsetInFBN ]" + currentBufferOffset + " copy -> " + endoffset + " , " + 0);

                    for (int k = 0; k < tocopy; k++)
                    {
                        data[k] = buffer[k + currentBufferOffset];
                    }
                    currentBufferOffset += tocopy;
                }
                else
                {
                    //proper BLK_SIZE size copy
                    if (!inCoreData.Contains(fbn))
                    {
                        inCoreData.Add(fbn, new byte[BLK_SIZE]);
                    }

                    byte[] data = (byte[])inCoreData[fbn];
                    //Console.WriteLine("[currOffsetInInput, tocopy -> fbn, offsetInFBN ]" + currentBufferOffset + " copy -> " + fbn + " , " +  0);

                    for (int k = 0; k < BLK_SIZE; k++)
                    {
                        data[k] = buffer[k + currentBufferOffset];
                    }
                    currentBufferOffset += BLK_SIZE;
                }
            }
            bytesWritten = currentBufferOffset;
            touch_inode_obj();
            return true;
        }

        public Boolean ReadFile(byte[] buffer, out int bytesRead, long offset)
        {
            int startfbn, startoffset, endfbn, endoffset;
            ComputeBoundaries(BLK_SIZE, offset, buffer.Length, out startfbn, out startoffset, out endfbn, out endoffset);

            int currentBufferOffset = 0;

            for (int fbn = startfbn; fbn <= endfbn; fbn++)
            {
                if (fbn == startfbn)
                {
                    //return 0's if its not present incore
                    if (!inCoreData.Contains(startfbn))
                    {
                        inCoreData.Add(startfbn, new byte[BLK_SIZE]);
                    }

                    byte[] data = (byte[])inCoreData[startfbn];
                    int tocopy = ((BLK_SIZE - startoffset) < buffer.Length) ? (BLK_SIZE - startoffset) : buffer.Length;
                    //Console.WriteLine("[currOffsetInInput, tocopy -> fbn, offsetInFBN ]" + currentBufferOffset + " copy -> " + startfbn + " , " + startoffset);
                    for (int k = 0; k < tocopy; k++)
                    {
                        buffer[k + currentBufferOffset] = data[k + startoffset];
                    }
                    currentBufferOffset += tocopy;
                }
                else if (fbn == endfbn)
                {
                    if (!inCoreData.Contains(endfbn))
                    {
                        inCoreData.Add(endfbn, new byte[BLK_SIZE]);
                    }

                    byte[] data = (byte[])inCoreData[endfbn];
                    int tocopy = endoffset;
                    //Console.WriteLine("[currOffsetInInput, tocopy -> fbn, offsetInFBN ]" + currentBufferOffset + " copy -> " + endoffset + " , " + 0);

                    for (int k = 0; k < tocopy; k++)
                    {
                        buffer[k + currentBufferOffset] = data[k];
                    }
                    currentBufferOffset += tocopy;
                }
                else
                {
                    //proper 8k size copy
                    if (!inCoreData.Contains(fbn))
                    {
                        inCoreData.Add(fbn, new byte[BLK_SIZE]);
                    }

                    byte[] data = (byte[])inCoreData[fbn];
                    //Console.WriteLine("[currOffsetInInput, tocopy -> fbn, offsetInFBN ]" + currentBufferOffset + " copy -> " + fbn + " , " + 0);

                    for (int k = 0; k < BLK_SIZE; k++)
                    {
                        buffer[k + currentBufferOffset] = data[k];
                    }
                    currentBufferOffset += BLK_SIZE;
                }
            }
            bytesRead = currentBufferOffset;
            touch_inode_obj();
            return true;
        }

        public Boolean FlushFileBuffers(REDFSCore redfsCore)
        {
            if (isDirty)
            {
                Console.WriteLine("Flush file buffers " + fileInfo.FileName);
                redfsCore.sync(myWIP);
                redfsCore.flush_cache(myWIP, false);
            }
            return true;
        }

        //---------------------------------------------------------------------------------------------------------------------------
        //                    Methods called with obj of REDFSCore, it means that we do actual disk io. myWip should also be valid;
        //---------------------------------------------------------------------------------------------------------------------------

        public Boolean SetEndOfFile(REDFSCore redfsCore, long length, bool preAlloc)
        {
            if (isDirectory())
            {
                throw new NotSupportedException();
            }
            else
            {
                redfsCore.redfs_resize_wip(myWIP.get_filefsid(), myWIP, length, preAlloc);
                fileInfo.Length = myWIP.get_filesize();
                isDirty = true;
                touch_inode_obj();
                return true;
            }
        }

        /*
         * Walk the existing tree from root and figure out which directories need to be reloaded and bought back into
         * memory because they are dirty. 
         * Ex. new inode added, moved, modified etc.
         */ 
        public void PreSyncDirectoryLoadList(List<string> dirsToBeloaded, IDictionary allinodes)
        {
            DEFS.ASSERT(isDirectory(), "PreSyncDirectoryLoadList must be called only for a directory");

            if (isDirty)
            {
                string selfdirpath = (parentDirectory == null) ? "\\" : ((parentDirectory == "\\") ? 
                     "\\" + fileInfo.FileName : parentDirectory + "\\" + fileInfo.FileName);
                dirsToBeloaded.Add(selfdirpath);
            }

            foreach (String item in items)
            {
                string childpath = (parentDirectory == null) ? ("\\" + item) :
                    (parentDirectory == "\\") ? ("\\" + fileInfo.FileName + "\\" + item) :
                    (parentDirectory + "\\" + fileInfo.FileName + "\\" + item);

                if (allinodes.Contains(childpath))
                {
                    REDFSInode child = (REDFSInode)allinodes[childpath];
                    if (child.isDirectory())
                    {
                        child.PreSyncDirectoryLoadList(dirsToBeloaded, allinodes);
                    }
                }
            }
        }

        /*
         * Write out all the inmemory data
         * 
         * Steps,
         * For each directory from the root.
         * a. If dir is dirty, reload the contents to write back out correctly if its a skeleton.
         * b. If dir is skeleton and not dirty, dont worry, just sync incore contents.
         * 
         * Once children are cleared, check if its become a skeleton or not.
         * a. If yes, then see if dir can remove itself and mark its parent as skeleton.
         * b. If no, check we if we have any aged files/dir and remove them.
         */ 
        public void SyncInternal(RedFS_Inode inowip, REDFSCore redfsCore, IDictionary allinodes)
        {
            if (fileInfo.FileName == "\\")
            {
                DEFS.ASSERT(parentDirectory == null, "Parent is null for rootdir");
            }
            else
            {
                DEFS.ASSERT(parentDirectory != null, "parent cannot be null for non-root dir");
            }

            if (isDirectory() && allinodes != null)
            {
                OnDiskDirectoryInfo oddi = new OnDiskDirectoryInfo();
                oddi.ino = myWIP.get_ino();
                oddi.fileInfo = fileInfo;

                if (!isDirty)
                {
                  //  While dir1 is loaded, the items are zero while it hsould have 100
                    foreach (String item in items)
                    {
                        lock (allinodes)
                        {
                            string childpath = (parentDirectory == null) ? ("\\" + item) :
                                (parentDirectory == "\\") ? ("\\" + fileInfo.FileName + "\\" + item) :
                                (parentDirectory + "\\" + fileInfo.FileName + "\\" + item);
                            REDFSInode child = (REDFSInode)allinodes[childpath];

                            if (child != null)
                            {
                                child.SyncInternal(inowip, redfsCore, allinodes);
                            }

                            //OnDiskInodeInfo odii = new OnDiskInodeInfo();
                            //odii.fileInfo = child.fileInfo;
                            //odii.ino = child.myWIP.get_ino();

                            //oddi.inodes.Add(odii);
                        }
                    }
                }
                else if (isDirty) 
                {
                    DEFS.ASSERT(!isInodeSkeleton, "Called must have reloaded dir in case of dirty");
                    foreach (String item in items)
                    {
                        lock (allinodes)
                        {
                            string childpath = (parentDirectory == null) ? ("\\" + item) :
                                (parentDirectory == "\\") ? ("\\" + fileInfo.FileName + "\\" + item) :
                                (parentDirectory + "\\" + fileInfo.FileName + "\\" + item);
                            REDFSInode child = (REDFSInode)allinodes[childpath];

                            child.SyncInternal(inowip, redfsCore, allinodes);

                            OnDiskInodeInfo odii = new OnDiskInodeInfo();
                            odii.fileInfo = child.fileInfo;
                            odii.ino = child.myWIP.get_ino();

                            oddi.inodes.Add(odii);
                        }
                    }

                    string json = JsonConvert.SerializeObject(oddi, Formatting.None);
                    byte[] data = Encoding.UTF8.GetBytes(json);

                    cache_string = json;

                    if (myWIP.get_ino() == 64 || myWIP.get_ino() == 65)
                    {
                        Console.WriteLine("Just to stop here for debug!");
                    }

                    redfsCore.redfs_write(myWIP, 0, data, 0, data.Length);
                    redfsCore.redfs_checkin_wip(inowip, myWIP, myWIP.get_ino());
                    redfsCore.sync(myWIP);
                    myWIP.log("syncInteral datalen=" + data.Length);
                }
                redfsCore.flush_cache(myWIP, false);
                isDirty = false;
            }
            if (!isDirectory() && isDirty)
            {
                redfsCore.sync(myWIP);
            }
            else if (isDirectory() && isDirty && allinodes == null)
            {
                //for testing
                OnDiskDirectoryInfo oddi = new OnDiskDirectoryInfo();
                oddi.ino = myWIP.get_ino();
                oddi.fileInfo = fileInfo;

                foreach (String item in items)
                {
                    OnDiskInodeInfo odii = new OnDiskInodeInfo();
                    odii.fileInfo.FileName = item;
                    oddi.inodes.Add(odii);
                }

                string json = JsonConvert.SerializeObject(oddi, Formatting.Indented);
                byte[] data = Encoding.UTF8.GetBytes(json);

                cache_string = json;

                redfsCore.redfs_write(myWIP, 0, data, 0, data.Length);
                redfsCore.redfs_checkin_wip(inowip, myWIP, myWIP.get_ino());
                redfsCore.sync(myWIP);
                redfsCore.flush_cache(myWIP, false);
                isDirty = false;
            }
            else if (isDirty)
            {
                throw new SystemException("Not yet implimented!");
            }

            /*
             * Here comes the GC part, this could be a directory or a file. If we keep loading the entire directory structure,
             * it will lead to bad results. So the idea is that we just load a file and only its parent directories we need.
             * It means that a directory will have entries of its contents, but not all of them are incore. This is called a skeleton
             * directory
             * 
             * For a file, if its old and unused, remove it from the inodes[] map file and mark its parent as skeleton. i.e not all
             * of its parents children (this one) is present incore in inodes[] map file.
             * 
             * For a directory, if non of its children are incore and its skeleton, remove it from inodes[] map file and mark its parent
             * as skeleton
             */

            if (myWIP.get_ino() == 64 || myWIP.get_ino() == 65)
            {
                Console.WriteLine("dor debug");
            }

            int age = get_inode_obj_age();
            if (!isDirectory())
            {
                //File
                if (age > 20 && isDirty == false && myWIP.is_dirty == false)
                {
                    //remove self
                    allinodes.Remove(fileInfo.FileName);
                    REDFSInode parent = (REDFSInode)allinodes[parentDirectory];
                    parent.isInodeSkeleton = true;
                }
            }
            else
            {
                if (!(age > 20 && isDirty == false && myWIP.is_dirty == false))
                {
                    //not a candidate
                    return;
                }

                Boolean isValidNodePresent = false;

                //Now check all its children are either "dir+skeleton" OR "file+notininodes[]".
                foreach (String item in items)
                {
                    lock (allinodes)
                    {
                        string childpath = (parentDirectory == "\\") ? ("\\" + fileInfo.FileName + "\\" + item) :
                            (parentDirectory + "\\" + fileInfo.FileName + "\\" + item);
                        
                        if (allinodes.Contains(childpath))
                        {
                            REDFSInode child = (REDFSInode)allinodes[childpath];
                            if (!child.isDirectory())
                            {
                                isValidNodePresent = true;
                            }
                        }
                        else
                        {
                            DEFS.ASSERT(isInodeSkeleton == true, "We dont have a child incore, so we must be a skeleton!");
                        }
                    }
                }

                lock (allinodes)
                {
                    if (!isValidNodePresent && fileInfo.FileName != "\\")
                    {
                        //remove self
                        string selfpath = (parentDirectory == "\\") ?(fileInfo.FileName) :
                            (parentDirectory + "\\" + fileInfo.FileName);

                        allinodes.Remove(selfpath);
                        REDFSInode parent = (REDFSInode)allinodes[parentDirectory];
                        parent.isInodeSkeleton = true;
                    }
                }
            }
        }
    }
}
