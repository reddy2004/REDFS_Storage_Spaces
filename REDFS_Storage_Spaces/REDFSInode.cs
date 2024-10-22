﻿using System;
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
        }

        private int get_inode_obj_age()
        {
            long elapsed = (DateTime.Now.ToUniversalTime().Ticks - m_creation_time);
            return (int)(elapsed / 10000000);
        }

        private void touch_inode_obj()
        {
            m_creation_time = DateTime.Now.ToUniversalTime().Ticks;
        }

        public void LoadWipForExistingInode(REDFSCore redfsCore, RedFS_Inode inowip, int ino, int pino)
        {
            if (myWIP != null && (myWIP.m_ino != 0  || myWIP.is_dirty == true))
            {
                Console.WriteLine("appears wip is already in memory!" + ino + ", p=" + pino);
                return;
            }

            lock (inowip)
            {
                if (fileInfo.Attributes.HasFlag(FileAttributes.Normal))
                {
                    myWIP = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, ino, pino);
                    redfsCore.redfs_checkout_wip(inowip, myWIP, ino);

                    //mark as not dirty.
                    myWIP.is_dirty = false;
                    isDirty = false;

                    fileInfo.Length = myWIP.get_filesize();
                    //throw new SystemException("Not yet implimented!");
                }
                else
                {
                    myWIP = new RedFS_Inode(WIP_TYPE.DIRECTORY_FILE, ino, pino);
                    redfsCore.redfs_checkout_wip(inowip, myWIP, ino);

                    //mark as not dirty.
                    myWIP.is_dirty = false;
                    isDirty = false;
                    DEFS.ASSERT(myWIP.get_ino() == ino, "ino should match on reading the wip from inofile! ");

                    //Keep the json string in memory for debugging
                    byte[] buffer = new byte[myWIP.get_filesize()];
                    redfsCore.redfs_read(myWIP, 0, buffer, 0, buffer.Length);
                }
                touch_inode_obj();
            }
        }

        public void InsertWipForNewlyCreatedInode(int fsid, int ino, int pino, RedFS_Inode rclone)
        {
            DEFS.ASSERT(fileInfo.Attributes.HasFlag(FileAttributes.Normal), "We can insert wip only for files and not directories!");

            myWIP = rclone; //just point it here.

            //Set the correct attributes
            myWIP.set_ino(ino, pino);
            myWIP.m_ino = ino;
            myWIP.setfilefsid_on_dirty(fsid);
            myWIP.isWipValid = true;
            myWIP.is_dirty = true;
            isDirty = true;
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
            myWIP.isWipValid = true;
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
            lock (this)
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
        }

        public IList<string> ListFilesWithPattern(string pattern)
        {
            lock (this)
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
                    touch_inode_obj();
                    return si;
                }
            }
        }

        public Boolean SetEndOfFile(long length)
        {
            lock (this)
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
            lock (this)
            {
                DEFS.ASSERT(isDirectory(), "Should be a directory, if we want to remove a file from it");
                foreach (string f in items)
                {
                    if (f == fileName)
                    {
                        items.Remove(fileName);
                        touch_inode_obj();
                        return true;
                    }
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

        public Boolean WriteFile(REDFSCore redfsCore, byte[] buffer, out int bytesWritten, long offset)
        {
            lock (this)
            {
                bytesWritten = redfsCore.redfs_write(myWIP, offset, buffer, 0, buffer.Length, WRITE_TYPE.OVERWRITE_IN_PLACE);
                isDirty = true;
                touch_inode_obj();

                if (myWIP.get_ino() == 65)
                {
                    Console.WriteLine("Write: offset:" + offset + ", buffer : " + buffer.Length);
                }

                long current_wip_size = myWIP.get_filesize();
                long end_of_file = buffer.Length + offset;
                if (end_of_file > current_wip_size)
                {
                    DEFS.ASSERT(end_of_file == myWIP.get_filesize(), "File size should've been written out correctly");
                }
                fileInfo.Length = myWIP.get_filesize();
                return true;
            }
        }

        public Boolean ReadFile(REDFSCore redfsCore, byte[] buffer, out int bytesRead, long offset)
        {
            lock (this)
            {
                if (myWIP.get_ino() == 65)
                {
                    Console.WriteLine("Read: offset:" + offset + ", buffer : " + buffer.Length);
                }
                if (offset > myWIP.size)
                {
                    //log error
                    bytesRead = 0;
                    return false;
                }
                DEFS.ASSERT(offset <= myWIP.size, "Trying to read beyond eof!");
                bytesRead = redfsCore.redfs_read(myWIP, offset, buffer, 0, buffer.Length);
                touch_inode_obj();
                return true;
            }
        }

        public Boolean FlushFileBuffers(REDFSCore redfsCore)
        {
            lock (this)
            {
                if (isDirty)
                {
                    redfsCore.sync(myWIP);
                    redfsCore.flush_cache(myWIP, false);
                }
            }
            return true;
        }

        //---------------------------------------------------------------------------------------------------------------------------
        //                    Methods called with obj of REDFSCore, it means that we do actual disk io. myWip should also be valid;
        //---------------------------------------------------------------------------------------------------------------------------

        public Boolean SetEndOfFile(REDFSCore redfsCore, long length, bool preAlloc)
        {
            lock (this)
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
        }

        /*
         * Walk the existing tree from root and figure out which directories need to be reloaded and bought back into
         * memory because they are dirty. 
         * Ex. new inode added, moved, modified etc.
         */ 
        public void PreSyncDirectoryLoadList(List<string> dirsToBeloaded, IDictionary allinodes)
        {
            DEFS.ASSERT(isDirectory(), "PreSyncDirectoryLoadList must be called only for a directory");

            lock (this)
            {
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
                    touch_inode_obj();
                }
            }
        }

        /*
         * Walks the tree and releases all non-dirty L0's that are in memory. Return the count
         * of dirty buffers. Used before shutdown.
         */ 
        public int FlushCacheL0s(REDFSCore redfsCore, IDictionary allinodes)
        {
            lock (this)
            {
                touch_inode_obj();
                if (isDirectory())
                {
                    foreach (String item in items)
                    {
                        string childpath = (parentDirectory == null) ? ("\\" + item) :
                            (parentDirectory == "\\") ? ("\\" + fileInfo.FileName + "\\" + item) :
                            (parentDirectory + "\\" + fileInfo.FileName + "\\" + item);
                        REDFSInode child = (REDFSInode)allinodes[childpath];

                        if (child != null)
                        {
                            child.FlushCacheL0s(redfsCore, allinodes);
                        }
                    }
                    redfsCore.flush_cache(myWIP, true);
                }
                else
                {
                    redfsCore.flush_cache(myWIP, true);
                }
            }
            return 0;
        }

        /*
         * Walks the tree and releases all non-dirty L0's that are in memory. Return the count
         * of dirty buffers. Used during normal copy/read/write so that memory usage is in control
         */
        public int FlushCacheL0s_Garbage_Collection(REDFSCore redfsCore, IDictionary allinodes)
        {
            lock (this)
            {
                touch_inode_obj();
                if (isDirectory())
                {
                    foreach (String item in items)
                    {
                        string childpath = (parentDirectory == null) ? ("\\" + item) :
                            (parentDirectory == "\\") ? ("\\" + fileInfo.FileName + "\\" + item) :
                            (parentDirectory + "\\" + fileInfo.FileName + "\\" + item);
                        REDFSInode child = (REDFSInode)allinodes[childpath];

                        if (child != null)
                        {
                            child.FlushCacheL0s_Garbage_Collection(redfsCore, allinodes);
                        }
                    }
                    redfsCore.flush_cache(myWIP, false);
                }
                else
                {
                    redfsCore.flush_cache(myWIP, false);
                }
            }
            return 0;
        }

        public void PreSummaryDirectoryLoadList(List<string> dirsToBeloaded, IDictionary allinodes)
        {
            DEFS.ASSERT(isDirectory(), "PreSyncDirectoryLoadList must be called only for a directory");

            string selfdirpath = (parentDirectory == null) ? "\\" : ((parentDirectory == "\\") ?
                    "\\" + fileInfo.FileName : parentDirectory + "\\" + fileInfo.FileName);

            lock (this)
            {
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
                            dirsToBeloaded.Add(childpath);
                        }
                        touch_inode_obj();
                    }
                }
            }
        }

        public Boolean GetDebugSummaryOfFSID(DebugSummaryOfFSID dsof, IDictionary allinodes, REDFSCore rfcore)
        {
            DEFS.ASSERT(isInodeSkeleton == false, "Cannot run query on a skeleton, must have preloaded!");

            touch_inode_obj();
            if (isDirectory())
            {
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
                            child.GetDebugSummaryOfFSID(dsof, allinodes, rfcore);
                        }
                        else
                        {
                            child.GetDebugSummaryOfFSID(dsof, allinodes, rfcore);
                        }
                    }
                }

                PrintableWIP pwip = rfcore.redfs_list_tree(myWIP, Array.Empty<long>(), Array.Empty<int>());
                dsof.numDirectories++;
                dsof.numL0s += pwip.ondiskL0Blocks;
                dsof.numL1s += pwip.ondiskL1Blocks;
                dsof.numL2s += pwip.ondiskL2Blocks;
                //dsof.totalLogicalData += myWIP.get_filesize();

                return true;
            }
            else
            {
                PrintableWIP pwip = rfcore.redfs_list_tree(myWIP, Array.Empty<long>(), Array.Empty<int>());
                dsof.numFiles++;
                dsof.numL0s += pwip.ondiskL0Blocks;
                dsof.numL1s += pwip.ondiskL1Blocks;
                dsof.numL2s += pwip.ondiskL2Blocks;
                dsof.totalLogicalData += myWIP.get_filesize();
                return true;
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
            //Dont touch_inode_obj() in this routine

            if (fileInfo.FileName == "\\")
            {
                DEFS.ASSERT(parentDirectory == null, "Parent is null for rootdir");
            }
            else
            {
                DEFS.ASSERT(parentDirectory != null, "parent cannot be null for non-root dir");
            }

            lock (this)
            {
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
                            }
                        }
                    }
                    else if (isDirty)
                    {
                        DEFS.ASSERT(!isInodeSkeleton, "Called must have reloaded dir in case of dirty");
                        foreach (String item in items)
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

                        string json = JsonConvert.SerializeObject(oddi, Formatting.None);
                        byte[] data = Encoding.UTF8.GetBytes(json);

                        //quick inline test
                        try
                        {
                            string testStr1 = System.Text.Encoding.UTF8.GetString(data);
                            OnDiskDirectoryInfo oddiTest = JsonConvert.DeserializeObject<OnDiskDirectoryInfo>(testStr1);
                            DEFS.ASSERT(testStr1 == json, "json encodings should match");
                        }
                        catch (Exception e)
                        {
                            DEFS.ASSERT(false, "some issue with json " + e.Message);
                        }

                        cache_string = json;

                        redfsCore.redfs_write(myWIP, 0, data, 0, data.Length, WRITE_TYPE.TRUNCATE_AND_OVERWRITE);
                        redfsCore.sync(myWIP);
                        bool returnValue = redfsCore.redfs_checkin_wip(inowip, myWIP, myWIP.get_ino());
                        DEFS.ASSERT(returnValue, "Couldnt checkin wip correctly, ino = " + myWIP.get_ino());
                        myWIP.log("syncInteral datalen=" + data.Length);
                    }
                    redfsCore.flush_cache(myWIP, false);
                    isDirty = false;
                }


                if (!isDirectory() && (isDirty || myWIP.is_dirty))
                {
                        fileInfo.Length = myWIP.get_filesize();
                        redfsCore.sync(myWIP);
                        redfsCore.redfs_checkin_wip(inowip, myWIP, myWIP.get_ino());
                        isDirty = false;
                }
                else if (isDirectory() && (isDirty || myWIP.is_dirty) && allinodes == null)
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
                    cache_string = json;
                    byte[] data = Encoding.UTF8.GetBytes(json);

                    redfsCore.redfs_write(myWIP, 0, data, 0, data.Length, WRITE_TYPE.TRUNCATE_AND_OVERWRITE);
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

                int age = get_inode_obj_age();
                if (!isDirectory())
                {
                    //File
                    if (age > 20 && isDirty == false && myWIP.is_dirty == false)
                    {
                        lock (allinodes)
                        {
                            redfsCore.flush_cache(myWIP, false);
                            //remove self
                            allinodes.Remove(fileInfo.FileName);
                            REDFSInode parent = (REDFSInode)allinodes[parentDirectory];
                            parent.isInodeSkeleton = true;
                        }
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
                            string childpath = (parentDirectory == null) ? ("\\" + item) :
                                (parentDirectory == "\\") ? ("\\" + fileInfo.FileName + "\\" + item) :
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

                    redfsCore.flush_cache(myWIP, false);
                    lock (allinodes)
                    {
                        if (!isValidNodePresent && fileInfo.FileName != "\\")
                        {
                            //remove self
                            string selfpath = (parentDirectory == "\\") ? (fileInfo.FileName) :
                                (parentDirectory + "\\" + fileInfo.FileName);

                            allinodes.Remove(selfpath);
                            REDFSInode parent = (REDFSInode)allinodes[parentDirectory];
                            parent.isInodeSkeleton = true;
                        }
                        else if (!isValidNodePresent && fileInfo.FileName != "\\")
                        {
                            isInodeSkeleton = true;
                        }
                    }
                }
            }//end of lock
        }
    }
}
