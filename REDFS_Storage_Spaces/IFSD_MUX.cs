using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

/*
 * Contains the list of fsid in the container and pointers to the redfs core.
 * for the sake of simplicity in implimentation, its better to pass redfsCore to each of the REDFSTree objects
 * so that each of these objects can manage its own ondisk data in the client r/w path.
 * 
 * Dokan Callback -> IFSD_Mux -> REDFSTree ->  REdfs_FSID/REDFSCore -> storage/alloc/ref/map etc
 * 
 * Locks are taken from top to bottom in an order so issue of deadlock wont arise.
 */
namespace REDFS_ClusterMode
{
    public class IFSD_MUX
    {
        public RedFS_FSID[] FSIDList = new RedFS_FSID[1024];            //One copy exists inside the REDFSTree object as well. 
        public REDFSTree[]  RedfsVolumeTrees = new REDFSTree[1024];

        public REDFSCore redfsCore; /*One object*/

        public int numValidFsids = 0;

        private bool m_shutdown;
        private bool m_shutdown_done_gc = false;

        public IFSD_MUX(string containerName)
        {
            redfsCore = new REDFSCore(containerName);
            Thread oThread = new Thread(new ThreadStart(W_GCThreadMux));
            oThread.Start();
        }

        public void CreateZeroRootVolume()
        {
            FSIDList[0] = redfsCore.CreateZeroRootVolume();
            numValidFsids++;
        }

        /*
         * Called when a new volume is created by the user from the root volume. This is a vanilla volume
         * with no data
         */
        public int CreateAndInitNewFSIDFromRootVolume()
        {
            int newFsidId = numValidFsids++;
            /*
            RedFS_Inode iMapWip = new RedFS_Inode(WIP_TYPE.PUBLIC_INODE_MAP, 1, -1);
            RedFS_Inode inodeFile = new RedFS_Inode(WIP_TYPE.PUBLIC_INODE_FILE, 0, -1);

            //inodeFile.set_filesize((long)128 * 1024 * 1024 * 1024);
            redfsCore.redfs_resize_wip(newFsidId, inodeFile, (long)128 * 1024 * 1024 * 1024, false);

            //iMapWip.set_filesize( ((long)inodeFile.get_filesize() / OPS.WIP_SIZE) / 8); //67108864
            redfsCore.redfs_resize_wip(newFsidId, iMapWip, ((long)inodeFile.get_filesize() / OPS.WIP_SIZE) / 8, false);

            DEFS.ASSERT(inodeFile.get_filesize() == (long)128 * 1024 * 1024 * 1024, "inowip size mismatch " + inodeFile.get_filesize());
            DEFS.ASSERT(iMapWip.get_filesize() == ((long)inodeFile.get_filesize() / OPS.WIP_SIZE) / 8, "iMapWip size mismatch " + iMapWip.get_filesize());

            RedFS_FSID newFsid = new RedFS_FSID(newFsidId, 0);
            newFsid.set_inodefile_wip(inodeFile);
            newFsid.set_inodemap_wip(iMapWip);
            newFsid.set_logical_data(0);
            newFsid.set_start_inonumber(64);

            redfsCore.sync(inodeFile);
            redfsCore.sync(iMapWip);
            redfsCore.flush_cache(inodeFile, true);
            redfsCore.flush_cache(iMapWip, true);

            DEFS.ASSERT(inodeFile.get_incore_cnt() == 0, "Dont cache blocks during init");
            DEFS.ASSERT(iMapWip.get_incore_cnt() == 0, "Dont cache blocks during init");
            */

            FSIDList[newFsidId] = redfsCore.CreateEmptyFSID(newFsidId);
            FSIDList[newFsidId].set_dirty(true);

            RedfsVolumeTrees[newFsidId] = new REDFSTree(FSIDList[newFsidId], redfsCore);

            RedfsVolumeTrees[newFsidId].CreateRootDirectoryWip(); //for the rootDir, i.e '\\'

            //do_fsid_sync_internal(newFsidId); not required

            return newFsidId;
        }

        public void Sync()
        {
            //redFSPersistantStorage uses Windows Filesystems which have their own caching and buffering mechanism. 
            //redFSPersistantStorage does not have in-memory data to be sync, all write & reads are dont synchronously.

            redfsCore.redfsBlockAllocator.Sync();

            //Write out all dirty fsid blocks
            lock (FSIDList)
            {
                //start with i=0 but dont do anything for fsid 0
                for (int i = 0; i < (numValidFsids); i++)
                {
                    if (i > 0 && FSIDList[i] != null)
                    {
                        try
                        {
                            RedfsVolumeTrees[i].SyncTree();
                            if (FSIDList[i].isDirty())
                            {
                                redfsCore.redFSPersistantStorage.write_fsid(FSIDList[i]);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }
                    }
                    FSIDList[i].set_dirty(false);
                }
            }
            redfsCore.redfsBlockAllocator.Sync();
        }

        public void shut_down()
        {
            Console.WriteLine("IFSDMux", "Initiating shutdown call");

            Sync();

            m_shutdown = true;
            while (m_shutdown_done_gc == false)
            {
                System.Threading.Thread.Sleep(100);
            }

            System.Threading.Thread.Sleep(100);

            redfsCore.ShutDown();
            Console.WriteLine("IFSDMux", "Finished shutdown call");
        }

        /*
         * Long running threads, does GC every 5 seconds approx.
         */
        private void W_GCThreadMux()
        {
            Console.WriteLine("IFSDMux : Starting gc/sync thread (Mux)...");
            int next_wait = 1000;

            while (true)
            {
                bool shutdownloop = false;
                //TimingCounter tctr = new TimingCounter();
                //tctr.start_counter();

                if (m_shutdown)
                {
                    shutdownloop = true;
                }
                else
                {
                    Thread.Sleep(next_wait);
                }

                for (int i = 0; i < (numValidFsids); i++)
                {
                    if (FSIDList[i] == null) {
                        continue;
                    }

                    if (!FSIDList[i].isDirty())
                    {
                        continue;
                    }

                    do_fsid_sync_internal(i);
                }

                if (m_shutdown && shutdownloop)
                {
                    m_shutdown_done_gc = true;
                    break;
                }
                //tctr.stop_counter();
                //next_wait = (m_shutdown) ? 0 : (((5000 - tctr.get_millisecs_avg()) < 0) ? 0 : (5000 - tctr.get_millisecs_avg()));
            }

            //we are exiting now
            Console.WriteLine("IFSDMux", "Leaving gc/sync thread (Mux)...");
        }

        private void do_fsid_sync_internal(int id)
        {
            /*
            lock (FSIDList[id])
            {
                RedFS_Inode inowip = FSIDList[id].get_inode_file_wip("GC1");
                redfsCore.sync(inowip);
                redfsCore.flush_cache(inowip, true);
                redfsCore.redfs_commit_fsid(FSIDList[id]);
            }
            */
            //fix - very important
            //FSIDList[id].rootdir.sync();
            //FSIDList[id].rootdir.gc();

            if (m_shutdown == true)
            {
                //fix
               // ((CInode)REDDY.FSIDList[id].rootdir).unmount(true);
            }

            DEFS.ASSERT(FSIDList != null, "FSID List cannot be null");
            DEFS.ASSERT(FSIDList[id] != null, "FSID List at id: " + id + " cannot be null");
            lock (FSIDList)
            {
                if (FSIDList[id].isDirty())
                {
                    RedFS_Inode inowip = FSIDList[id].get_inode_file_wip("GC2");
                    redfsCore.sync(inowip);
                    redfsCore.flush_cache(inowip, false);
                    redfsCore.redfs_commit_fsid(FSIDList[id]);
                }
            }
        }

        /*
         * Can be call on the fly if the volume graph changes
         */
        public void ReloadAllFSIDs(VolumeManager volumeManager)
        {
            lock (FSIDList)
            {
                for (int i = 0; i < numValidFsids; i++)
                {
                    lock (FSIDList[i])
                    {
                        if (FSIDList[i] != null)
                        {
                            try
                            {
                                if (i != 0)
                                {
                                    RedfsVolumeTrees[i].SyncTree();
                                }
                                //do_fsid_sync_internal(i);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e.Message);
                            }
                        }
                    }
                }

                FSIDList = new RedFS_FSID[1024];
                foreach (VirtualVolume v in volumeManager.volumes)
                {
                    if (!v.isDeleted)
                    {
                        try
                        {
                            FSIDList[v.volumeId] = redfsCore.redFSPersistantStorage.read_fsid(v.volumeId);
                            RedfsVolumeTrees[v.volumeId] = new REDFSTree(FSIDList[v.volumeId], redfsCore);
                            if (v.volumeId != 0)
                            {
                                RedfsVolumeTrees[v.volumeId].LoadRootDirectory();
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                            throw new SystemException("Error in inowip size duing reload!" + e.Message);
                        }
                    }
                    else
                    {
                        FSIDList[v.volumeId] = redfsCore.redFSPersistantStorage.read_fsid(v.volumeId);
                    }

                    if (v.volumeId > numValidFsids)
                    {
                        numValidFsids = v.volumeId;
                    }
                }
                Console.WriteLine("Finished reading all fsids from disk!");
            }
        }


        //-----------------------------------------------------------------------------------------------------------------------------
        //                PUBLIC FUNCTIONS : An USER would use these APIs to create and work with volumes/directories/files
        //                                   This includes the dokan interface, backup jobs, dedupe jobs and the web filebrowser
        //-----------------------------------------------------------------------------------------------------------------------------
        public Boolean SetEndOfFile(int fsid, string path, long length, bool preAlloc)
        {
            return RedfsVolumeTrees[fsid].SetEndOfFile(path, length, preAlloc);
        }

        public Boolean CreateDirectory(int fsid, string path)
        {
            return RedfsVolumeTrees[fsid].CreateDirectory(FSIDList[fsid], path);
        }

        public Boolean CreateFile(int fsid, string path)
        {
            return RedfsVolumeTrees[fsid].CreateFile(FSIDList[fsid], path);
        }
    }
}
