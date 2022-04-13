using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading;
using System.Text;
namespace REDFS_TESTS
{
    /*
     * Test classes will introduce a way to start using REDFS as a filesystem/storage layer
     * for your own application.
     */ 
    [TestClass]
    public class IntroTestWIPCreation
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        /*
         * Create a new test container. Note that test containers is present in a particular folder.
         * You could have the chunks of the filesystem located everywhere provided that they are 
         * accessible from this device.
         * 
         * Currently is designed for usage with Microsoft windows path, but you could modify the
         * FS to be run on unix devices as well.
         */ 
        private void InitNewTestContainer(out string containerName)
        {
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "IntroTestWIPCreation_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\IntroTestWIPCreation_" + id1;

            REDFS.AddNewContainer(co1);

            REDFS.MountContainer(true, co1.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);
            containerName = co1.containerName;
        }

        private void CleanupTestContainer(string containerName)
        {
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(containerName);
        }

        /*
         * A test container is created with a default file of 1GB. We are going to add one more chunk, 2GB to the 
         * file system bringing the total space available in the container to 3GB
         * 
         * Since REDFS has concept of segments of 1GB, we can create two segments of 1GB in the chunk of size 2G
         * Segment is always 1GB at some offset in a chunk.
         * Chunk is a file which is in GBs, and an n-GB chunk has n segments that can be used.
         * 
         * REDFS dbn (disk block number) is spread over segments. Each of these 1GB segment in DBN space could be 
         * DEFAULT, RAID or mirrored. 
         * 
         * In this example, we use the 2GB chunk to map 2 1GB segments into the DBN address space.
         */ 
        private void CreateTestContainer(string containerName)
        {
            ChunkInfo ci = new ChunkInfo();
            ci.allowedSegmentTypes = SEGMENT_TYPES.Default;
            //ci.id is automatically assigned
            ci.size = 2;
            ci.freeSpace = ci.size * 1024;
            ci.path = REDFS.getAbsoluteContainerPath() + "\\cifile1.dat";
            ci.speedClass = "default";
            ci.chunkIsAccessible = true;

            REDFS.redfsContainer.AddNewChunkToContainer(false, ci);

            REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.CHUNK_CREATE, "Create Chunking", "Writing", ci.id + "," + ci.path + "," + ci.size + "," + ci.allowedSegmentTypes);

            //Wait
            REDFS.redfsContainer.WaitForChunkCreationToComplete();

            //We have the default chunk and the new one we just added
            Assert.AreEqual(2, REDFS.redfsContainer.redfsChunks.Count);

            Assert.AreEqual(OPS.NUM_SPAN_MAX_ALLOWED, 128 * 1024); //128 TB
            Assert.AreEqual(OPS.NUM_DBNS_IN_1GB, 131072); //With 8k blocks

            //Now that we have a container, lets try to create two default segments. Our new chunk should have got the id=1,
            //and with that we create two default spans with two segments.
            //As a twist, the first datasegment will be in the 2nd GB and the second datasegment will be in the first GB of the chunkfile
            RAWSegment[] dataDefault1 = new RAWSegment[1];
            dataDefault1[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 0);
            RAWSegment[] dataDefault2 = new RAWSegment[1];
            dataDefault2[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 1);

            DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 0, dataDefault1, null);
            DBNSegmentSpan dss2 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, OPS.NUM_DBNS_IN_1GB, dataDefault2, null);

            //Note that we are creating these outside of 'REDFSBlockAllocator' but since we have loaded REDFSContainer, these will get properly
            //written out into the container files. We can simply load the REDFSBlockAllocator later and verify ondisk infomration is valid and as expected.
            DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            spanMap.InsertDBNSegmentSpan(dss1);
            spanMap.InsertDBNSegmentSpan(dss2);

            REDFSCore rfcc = REDFS.redfsContainer.ifsd_mux.redfsCore;
            REDFS.redfsContainer.ReloadAllFSIDs();
        }

        private void CreateCloneOfZeroVolume()
        {

            Assert.AreEqual(2, REDFS.redfsContainer.redfsChunks.Count);
            //Clone the root volume.
            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(0, "cloneOfRoot", "Testing", "#000000");
            REDFS.redfsContainer.ReloadAllFSIDs();

            Assert.AreEqual(REDFS.redfsContainer.volumeManager.volumes.Count, 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[0] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[1] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1] != null);

            //To recover all allocated buffers
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree(rfsid);
        }

        /*
         * Create a new container and clone the root volume.
         * In the new volume, i.e FSID 1, create a file.
         * Write out random bytes and read it back and verify its right.
         */ 
        [TestMethod]
        public void IntroTest_1()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].CreateFile(rfsid, "\\temp.dat");
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].getNumInodesInTree() == 2);

            //RedFS_Inode rootDirWip = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP;
            //RedFS_Inode inoFile = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");

            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree(rfsid);

            byte[] buffer_in = new byte[99999];
            byte[] buffer_out = new byte[99999];

            int bytesWritten = 0, bytesRead = 0;

            Random r = new Random();
            r.NextBytes(buffer_in);
            r.NextBytes(buffer_out); //just scramble it, it should be overritten with buffer_in's data when we read back.

            /*
             * Notice that we are using the REDFSTree to write and read from wips, the tree is a wrapper for all of
             * the actual wips (REDFS_Inode) objects and provides the user an easy way to read and write to files.
             */
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].WriteFile("\\temp.dat", buffer_in, out bytesWritten, 0);

            Assert.AreEqual(bytesWritten, 99999);

            /*
             * We dont care that data is written out correctly or flushed. A background thread flushes all dirty buffers
             * to disk. The wrapper ensures that we read what we just wrote. it does  not matter to the caller if the data is
             * in memory or disk.
             */ 
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].ReadFile("\\temp.dat", buffer_out, out bytesRead, 0);

            Assert.AreEqual(bytesRead, 99999);

            for (int i=0;i<99999; i++)
            {
                Assert.AreEqual(buffer_in[i], buffer_out[i]);
            }

            /*
             * We also dont need to flush the buffers or clear out alloc'd blocks of the file. Since the tree is automanaged, we
             * can just call cleanup or unmount the container and expect that all data is written out cleanly and we manually
             * dont have to worry about any sort of memory management
             */ 
            CleanupTestContainer(containerName);
        }

        /*
         * In this test, instead of using the tree, we will try to work with the wips (REDFS_Inode objects) directly by reading
         * and writing to the redfsCore object. These wips must be tracked by the caller.
         * 
         * This will be useful if you need to impliment your own abstact layer and use redfs just as a file store. You may create
         * and modify wips directly which are not part of any REDFSTree or any FSID for that matter. (We need fsid to store the inodeFile
         * of a filesystem. If you impliment something else, then instead of inodeFile, you could have something else. For now, the inodeFile
         * is a safe way to store inodes.)
         * 
         * In our implimentation of REDFS, the inodeFile is part of the FSID, The fsid itself is just an 8k block stored at some DBN.
         * Once we read FSID, we get the inodeFile wip and with that we can access all the directories and files of the underlying filesystem.
         * 
         * To impliment something else, say a block store, then you need to create an inodeFile to store inodes in your blockstore. another
         * hashFile to store hash id to inode mapping. Then store both inodeFile and hashFile somewhere to access all the files in the block store.
         */ 
        [TestMethod]
        public void IntroTest_2()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];

            REDFSCore rfcore= REDFS.redfsContainer.ifsd_mux.redfsCore;
            int newInodeNum = rfcore.NEXT_INODE_NUMBER(rfsid);

            RedFS_Inode myWIP = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, newInodeNum, 0);


            byte[] buffer_in = new byte[99999];
            byte[] buffer_out = new byte[99999];

            Random r = new Random();
            r.NextBytes(buffer_in);
            r.NextBytes(buffer_out); //just scramble it, it should be overritten with buffer_in's data when we read back.

            /*
             * Use the wip to write out some data to disk. You could write out any arbitrary data encoded to byte array. 
             * For ex. Im using a simple json to store the directory info and writing out the json->byte array for the dir wip in the project
             */
            rfcore.redfs_write(myWIP, 0, buffer_in, 0, 99999);

            /*
             * Read directly using the REDFSCore.
             */
            rfcore.redfs_read(myWIP, 0, buffer_out, 0, 99999);

            for (int i = 0; i < 99999; i++)
            {
                Assert.AreEqual(buffer_in[i], buffer_out[i]);
            }

            
            /*
             * Since we are working with wips directly, we have to free up the allocated memory.
             */
            rfcore.sync(myWIP);

            PrintableWIP pwip = rfcore.redfs_list_tree(myWIP);

            Assert.AreEqual(pwip.wipIdx.Length, 13); //13 blocks
            Assert.AreEqual(pwip.L0_DBNS, null); //coz all dbns are stored in wip itself
            Assert.AreEqual(pwip.L1_DBNS, null);

            /*
             * Usually using REDFS_Tree to access wips would have taken care of this, but since we are working
             * with direct wips and REDFSCore, we should discard the wip once done.
             * 
             * Also remember, that you 'checkout' a copy of wip. So you must sync() and discard() after that. You cannot
             * checkout the same wip in two different threads and expect consistency as those two wips in two different threads
             * would be two copies. Data will get garbled if you use both.
             * 
             * Always checkout a wip, do i/o, then sync, commit and discard it
             */ 
            rfcore.redfs_discard_wip(myWIP);

            CleanupTestContainer(containerName);
        }
    }
}
