using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using DokanNet;
using DokanNet.Logging;

namespace REDFS_ClusterMode
{

    public static class INIT_COMPONENTS
    {
        public static int Dummy = 0;
        public static int DBNSpanMap = 1;
        public static int RedFSBlockAllocator = 2;
        public static int RedFSPersistantStorage = 4;
        public static int RefCountMap = 8;
        public static int AllocMap = 16;

        public static string[] TypeString = { "Dummy", "DBNSpanMap", "RedFSBlockAllocator", "RedFSPersistantStorage", "RefCountMap", "AllocMap"};
    }

    public class REDFSContainer
    {
        /*
         * A timing counter that is served to the browser. If there is a change, the browser
         * can refresh its data
         */
        public long volumes_ux_version = (new DateTime()).Ticks;
        public long chunks_ux_version = (new DateTime()).Ticks;
        public long backups_ux_version = (new DateTime()).Ticks;
        public long space_usage_ux_version = (new DateTime()).Ticks;

        /*
         * The base path where the containers main meta data is present.
         * We should put this in AppData folder or Program Files
         */
        public string containerFolderPath;
        public string containerName;

        /*
         * Consists of all the virtualVolumes in this container. All volume level data is available
         * in VirtualVolume class
         */ 
        public VolumeManager volumeManager;

        /*
         * Status of container wide operations. Such as 
         * Chunk operations - remove, new, move etc
         * Compression
         * Dedupe
         */ 
        public ContainerOperations containerOperations;

        /*
         * Data of all the chunks (data files) that are part of this container 
         */
        public IDictionary redfsChunks = new Dictionary<int, ChunkInfo>();

        /*
        * These are commands that is coming from the brower, they are just queued until its picked up by a thread.
        * The client (browser) can query the status and then track the progress.
        */
        public LinkedList<VolumeOperation> volumeOperations = new LinkedList<VolumeOperation>();

        /*
         * This contains the list of backup tasks in this container
         */
        public BackupTaskManager backupManager;


        /*
         * Where all the volumes can be accessesed and the files are also read using typical slashed directory heirarchy
         */ 
        public IFSD_MUX ifsd_mux;

        /*
         * Dokan callbacks are handled here
         */
        IncoreFSSkeleton ifs = null;

        public REDFSContainer(string containerName1, string containerPath1)
        {
            containerName = containerName1;
            containerFolderPath = containerPath1;
            containerOperations = new ContainerOperations();
        }

        public void InitContainer()
        {
            ifsd_mux = new IFSD_MUX(containerName);

            LoadContainerDataFromDisk();


            if (volumeManager.newRootVolumeCreated)
            {
                try
                {
                    ifsd_mux.CreateZeroRootVolume();
                    SaveChunkListToDisk("from new root volume");
                } 
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }

            //All chunks must've been inserted into the persistantstorage object by now.
            ifsd_mux.ReloadAllFSIDs(volumeManager);
        }

        public BackupTaskManager GetBackupTaskManager()
        {
            return backupManager;
        }

        public Boolean CreateRootVolumeForNewContainer()
        {
            volumeManager = new VolumeManager(containerFolderPath);
            volumeManager.CreateRootVolumeForNewContainer();

            return true;
        }

        //Static method to create the first basechunkfile
        public static Boolean CreateBaseChunkFileForNewContainer(string containerFolderPath)
        {
            ChunkInfo ci = new ChunkInfo();
            ci.id = 0;
            ci.path = containerFolderPath + "\\primaryAutoCreatedChunk.redfs";

            //using (FileStream sw1 = new FileStream(ci.path, FileMode.OpenOrCreate, FileAccess.Write))
            using (FileStream sw1 = new FileStream(ci.path, FileMode.OpenOrCreate))
            {
                sw1.SetLength((long)2 * 1024 * 1024 * 1024);
            }
            ci.size = 2;
            ci.freeSpace = 2048;

            string chunksFile = containerFolderPath + "\\chunk.files";
            using (StreamWriter sw = new StreamWriter(chunksFile))
            {
                String vstr = JsonConvert.SerializeObject(ci, Formatting.None);
                //Console.WriteLine(vstr);
                sw.WriteLine(vstr);
            }
            return true;
        }

        public Boolean LoadContainerDataFromDisk()
        {
            //First load volumes
            volumeManager = new VolumeManager(containerFolderPath);
            volumeManager.LoadVolumeListFromDisk();

            //Load the chunk info, this also sets the info in persistantStorage class.
            LoadChunkInformationFromDisk();

            //Also load backup task list
            backupManager = new BackupTaskManager(containerFolderPath);

            return true;
        }

        public Boolean LoadChunkInformationFromDisk()
        {
            Boolean hasDefaultChunk = false;
            try
            {
                string chunksFile = containerFolderPath + "\\chunk.files";
                Console.WriteLine(">>>>> Reading chunks for container " + containerFolderPath + " @ location " + chunksFile);
                using (StreamReader sr = new StreamReader(chunksFile))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        ChunkInfo b2 = JsonConvert.DeserializeObject<ChunkInfo>(line);
                        if (b2.id == 0)
                        {
                            hasDefaultChunk = true;
                        }
                        //We prolly crashed while creating. so fail this and let user remove this chunk.
                        if (b2.creationInProgress == true)
                        {
                            b2.status = "Failed";
                            b2.canDeleteChunk = true;
                        }

                        //Now lets check that the chunk files are present and accessible
                        bool pathExists;
                        int numSegments;
                        HostOSFileSystem.ChunkExistsInPath(b2.path, out pathExists, out numSegments);
                        if (!pathExists)
                        {
                            b2.status = "Error: Chunk not found";
                            b2.chunkIsAccessible = false;
                        }
                        else if (numSegments != b2.size)
                        {
                            b2.status = "Error: Size mismatch";
                            b2.chunkIsAccessible = false;
                        }
                        else
                        {
                            b2.status = "Ready";
                            b2.chunkIsAccessible = true;
                            ifsd_mux.redfsCore.InsertChunk((ChunkInfo)b2);
                        }
                        redfsChunks.Add((int)b2.id, b2);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The file (chunk.files) could not be read:");
                Console.WriteLine(e.Message);
            }

            if (!hasDefaultChunk && redfsChunks.Count == 0)
            {
                throw new SystemException();
            }
            return true;
        }

        public Boolean SaveChunkListToDisk(string from)
        {
            try
            {
                string chunksFile = containerFolderPath + "\\chunk.files";
                Console.WriteLine("Saving chunks for container " + containerFolderPath + " @ location " + chunksFile + " whre: " + from );

                lock (redfsChunks)
                {
                    using (StreamWriter sw = new StreamWriter(chunksFile))
                    {
                        foreach (ChunkInfo chunk in redfsChunks.Values)
                        {
                            String vstr = JsonConvert.SerializeObject(chunk, Formatting.None);
                            //Console.WriteLine(vstr);
                            sw.WriteLine(vstr);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("The file could not be read: chunk.files");
                Console.WriteLine(e.Message);
                return false;
            }
            return true;
        }

        public Boolean RemoveChunkFromContainer(int id, bool force)
        {
            string chunkfilepath = ((ChunkInfo)REDFS.redfsContainer.redfsChunks[id]).path;
            
            //XXX abort all the running ops for this file.
            if (((ChunkInfo)REDFS.redfsContainer.redfsChunks[id]).canDeleteChunk || force)
            {
                int opid = containerOperations.FindRunningOperation(ALLOWED_OPERATIONS.CHUNK_CREATE, id);
                if (opid != -1)
                {
                    while(!containerOperations.StopSpecificRunningOperations(opid))
                    {
                        Thread.Sleep(100);
                    }
                }
                redfsChunks.Remove(id);
                File.Delete(chunkfilepath);
                return SaveChunkListToDisk("from remove");
            }
            else
            {
                Console.WriteLine("Cannot delete chunk " + id + " as its not marked for delete");
                return false;
            }
        }

        /*
         * Called from the http server. We have been given a chunk and that chunk will have empty slots
         * so we have to allocate and map them into the redfs addressspace
         */ 
        public Boolean AddNewSegmentsIntoREDFSAddressSpace(SegmentDataInfo sdi)
        {
            int sizeToAllocate = sdi.sizeInGB;

            if (sdi.segmentTypeString == "segmentDefault")
            {
                int useableOffset = ((ChunkInfo)redfsChunks[sdi.chunkIDs[0]]).size - (((ChunkInfo)redfsChunks[sdi.chunkIDs[0]]).freeSpace) / 1024;
                DEFS.ASSERT(sdi.chunkIDs.Length == 1, "Should have only one target chunk passed!");

                if (sizeToAllocate > ((((ChunkInfo)redfsChunks[sdi.chunkIDs[0]]).freeSpace) / 1024))
                {
                    return false;
                }
                int dbnSpaceSegmentOffsetToStartFrom = ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap.GetNumSegmentsUsedInREDFSAddressSpace();

                for (int i=0;i<sizeToAllocate;i++)
                {
                    RAWSegment[] dataDefault1 = new RAWSegment[1];
                    dataDefault1[0] = new RAWSegment(SEGMENT_TYPES.Default, sdi.chunkIDs[0], useableOffset + i);
                    DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, (dbnSpaceSegmentOffsetToStartFrom + i) *OPS.NUM_DBNS_IN_1GB, dataDefault1, null);

                    DBNSegmentSpanMap spanMap = ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
                    spanMap.InsertDBNSegmentSpan(dss1);
                }

                ((ChunkInfo)redfsChunks[sdi.chunkIDs[0]]).freeSpace -= sizeToAllocate * 1024;
                SaveChunkListToDisk("from update");
                return true;
            }
            else
            {
                Console.WriteLine("Mirror and RAID types are not yet supported!");
                return false;
            }
        }

        public Boolean AddNewChunkToContainer(bool isBaseChunk, ChunkInfo ci)
        {
            //Must add after validation.
            int maxChunkId = 0;
            foreach (var chunk in redfsChunks.Values)
            {
                if ((int)((ChunkInfo)chunk).id > maxChunkId)
                {
                    maxChunkId = (int)((ChunkInfo)chunk).id;
                }
            }

            //Create a new operation to allocate a chunk at the given location. XXX todo

            ci.id = isBaseChunk? 0 : maxChunkId + 1;
            ci.freeSpace = ci.size * 1024;
            ci.status = "Creating..";
            ci.creationInProgress = true;
            redfsChunks.Add(ci.id, ci);

            SaveChunkListToDisk("from add new");
            return true;
        }

        /*
         * Actually wait for all chunks to be ready. This is used in test mode to verify tests where we actually
         * do i/o.
         */ 
        public void WaitForChunkCreationToComplete()
        {
            Boolean areAllChunksReady = false;

            while(areAllChunksReady == false)
            {
                areAllChunksReady = true; //assume
                foreach (ChunkInfo ci in redfsChunks.Values)
                {
                    if (!ci.chunkIsAccessible || ci.creationInProgress)
                    {
                        areAllChunksReady = false;
                    }
                }
                Thread.Sleep(1000);
            }
        }


        public List<ChunkInfo> getChunksInContainer()
        {
            List<ChunkInfo> list = new List<ChunkInfo>();
            foreach (var item in redfsChunks.Values)
            {
                list.Add((ChunkInfo)item);
            }
            return list;
        }

        public DBNSegmentSpanMap getSegmentSpanMap()
        {
            return ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
        }

        public VolumeManager GetCurrentVolumeManager()
        {
            return volumeManager;
        }

        public Boolean SaveContainerInfoToDisk()
        {
            return false;
        }

        public Boolean FlushAndWrapUp()
        {
            while(containerOperations.StopAllRunningOperations() == false)
            {
                Thread.Sleep(500);
            }

            //XXX Todo unmount ifs.
            if (REDFS.redfsContainer.containerOperations.currentlyMountedVolume != 0)
            {
                UnMountVolume();
            }

            ifsd_mux.shut_down();
            //To do, drain out all other io

            return true;
        }

        public void ReloadAllFSIDs()
        {
            ifsd_mux.ReloadAllFSIDs(volumeManager);
        }

        public void MountVolume(int volumeId)
        {
            /*
             * This should be called by HTTP server thread
             */
            Thread tc = new Thread(new ThreadStart(tBlockingDokanFSThread));
            tc.Start();

        }

        public void tBlockingDokanFSThread()
        {
            DEFS.ASSERT(REDFS.redfsContainer.containerOperations.currentlyMountedVolume >= 1, "should be greateer than 1");
            ifs = new IncoreFSSkeleton(ifsd_mux.RedfsVolumeTrees[REDFS.redfsContainer.containerOperations.currentlyMountedVolume]);
            ifs.Mount(@"N:\", /*DokanOptions.DebugMode | DokanOptions.EnableNotificationAPI*/ DokanOptions.FixedDrive, /*treadCount=*/5, new NullLogger());
            Console.WriteLine("tBlockingDokanFSThread is done <><><><><><><<>");
        }

        public void UnMountVolume()
        {
            Dokan.Unmount('N');
            Dokan.RemoveMountPoint(@"N:\");
        }
    }
}
