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

        IDictionary inCoreData = new Dictionary<int, byte[]>();

        //Flags specific for directories
        public List<string> items = new List<string>(); //all files/dir names
        public Boolean isInodeSkeleton = false;

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
        }

        public void LoadWipForExistingInode(REDFSCore redfsCore, RedFS_Inode inowip, int ino, int pino)
        {
            if (fileInfo.Attributes.HasFlag(FileAttributes.Normal))
            {
                myWIP = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, ino, pino);
                //throw new SystemException("Not yet implimented!");
            }
            else
            {
                myWIP = new RedFS_Inode(WIP_TYPE.DIRECTORY_FILE, ino, pino);
                redfsCore.redfs_checkout_wip(inowip, myWIP, ino);

                byte[] buffer = new byte[myWIP.get_filesize()];
                redfsCore.redfs_read(myWIP, 0, buffer, 0, buffer.Length);

                string jsonString = Encoding.UTF8.GetString(buffer);               
            }
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
        }
        public void InitializeDirectory(List<string> children)
        {
            if (isDirectory())
            {
                isInodeSkeleton = false;
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

        private string GetParentPath()
        {
            return parentDirectory;
        }

        public Boolean isDirectory()
        {
            return fileInfo.Attributes.HasFlag(FileAttributes.Directory);
        }

        public Boolean AddNewInode(string fileName)
        {
            if (isDirectory())
            {
                //Dont care if directory or file. we can optimize later.
                items.Add(fileName);
                isDirty = true;
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
                return true;
            }
        }

        public void SetAttributes(FileAttributes newAttr)
        {

        }

        public Boolean RemoveInodeNameFromDirectory(string fileName)
        {
            foreach(string f in items)
            {
                if (f == fileName)
                {
                    items.Remove(fileName);
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
            return true;
        }

        public Boolean FlushFileBuffers()
        {
            if (isDirty)
            {
                Console.WriteLine("Flush file buffers " + fileInfo.FileName);
            }
            return true;
        }

        /*
         * For a directory, remove all the file list and set the flag as skeleton
         * For a file, clear out all the buffers and set the flag. There should 
         * be no dirty/incode data after this call.
         */ 
        public Boolean MakeInodeAsSkeleton()
        {
            if (isDirectory())
            {
                //By now all files are  removed from the main 'inodes' dictionary
                //assert that directory is not dirty
            }
            else
            {
                //assert file does not have dirty buffers.
                //clear out all data in memory, this inode is removed from the 'inode' dictionary
            }
            isInodeSkeleton = true;
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
                return true;
            }
        }

        /*
         * Write out all the inmemory data
         */ 
        public void SyncInternal(RedFS_Inode inowip, REDFSCore redfsCore, IDictionary allinodes)
        {
            if (isDirectory() && allinodes != null)
            {
                if (isDirty)
                {
                    OnDiskDirectoryInfo oddi = new OnDiskDirectoryInfo();
                    oddi.ino = myWIP.get_ino();
                    oddi.fileInfo = fileInfo;

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

                    if (myWIP.m_ino == 2)
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
        }
    }
}