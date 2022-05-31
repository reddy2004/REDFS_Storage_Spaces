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
    [TestClass]
    public class IntroTestWIPRefcountLogic_setA
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
            REDFS.isTestMode = true;

            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "IntroTestWIPRefcntlogic_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\IntroTestWIPRefcntlogic_" + id1;

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
         * Segment is always 1GB at some offset (in GB) in a chunk.
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
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();
        }

        private void CreateAnotherCloneOfZeroVolume()
        {

            Assert.AreEqual(2, REDFS.redfsContainer.redfsChunks.Count);
            //Clone the root volume.
            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(0, "cloneOfRoot2", "Testing", "#000000");
            REDFS.redfsContainer.ReloadAllFSIDs();

            Assert.AreEqual(REDFS.redfsContainer.volumeManager.volumes.Count, 3);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[0] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[1] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[2] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 3);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2] != null);

            //To recover all allocated buffers
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[2];
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].SyncTree();
        }

        private void CreateTestFileAndWriteData(string filepath, byte[] data)
        {
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].CreateFile(filepath);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].getNumInodesInTree() == 2);

            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();

            byte[] buffer_out = new byte[data.Length];

            int bytesWritten = 0, bytesRead = 0;

            Random r = new Random();
            r.NextBytes(buffer_out); //just scramble it, it should be overritten with buffer_in's data when we read back.

            /*
             * Notice that we are using the REDFSTree to write and read from wips, the tree is a wrapper for all of
             * the actual wips (REDFS_Inode) objects and provides the user an easy way to read and write to files.
             */
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].WriteFile(filepath, data, out bytesWritten, 0);

            Assert.AreEqual(bytesWritten, data.Length);
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();
            /*
             * We dont care that data is written out correctly or flushed. A background thread flushes all dirty buffers
             * to disk. The wrapper ensures that we read what we just wrote. it does  not matter to the caller if the data is
             * in memory or disk.
             */
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].ReadFile(filepath, buffer_out, out bytesRead, 0);

            Assert.AreEqual(bytesRead, data.Length);

            for (int i = 0; i < data.Length; i++)
            {
                Assert.AreEqual(data[i], buffer_out[i]);
            }
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();
        }
        /*
         * Create a wip, note its refcounts and dbns, overwrite it, both full overwrites and
         * partial overwrites, Verify data, dbns and refcounts. If something was supposed to
         * get free, verify overwritten dbns are marked free as well.
         */
        [TestMethod]
        public void IntroTest_9()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            byte[] buffer = new byte[90099999];
            byte[] buffer2 = new byte[90099999];

            int bytesRead = 0;
            Random r = new Random();
            r.NextBytes(buffer);

            CreateTestFileAndWriteData("\\tempfile.data", buffer);
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].ReadFile("\\tempfile.data", buffer, out bytesRead, 0);

            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;
            RedFS_Inode wip = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\tempfile.data").myWIP;

            /*
             * Notice we just have a wip, i.e the root pointer of the meta data tree of the inode. Who ever has this wip
             * must be aware of how to use it - depending on weather its a file or directory. Here we know its a file, and
             * lets say we want this clone'd file to be \\tempfile.data.clone and place it in the root of the FSID.
             * We have to create a new file and pass this existing wip. We could also put this cloned wip into another fsid/folder
             */
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();

            RedFS_Inode rclone = rfcore.redfs_clone_wip(wip);

            Assert.IsTrue(rclone.m_ino == -1);
            Assert.IsTrue(rclone.get_parent_ino() == -1);

            //Lets put this wip on another fsid
            CreateAnotherCloneOfZeroVolume();

            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].CreateFileWithdWip("\\tempfile.data.clone", rclone);
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].ReadFile("\\tempfile.data.clone", buffer2, out bytesRead, 0);

            for (int i = 0; i < buffer.Length; i++)
            {
                Assert.AreEqual(buffer[i], buffer2[i]);
            }

            CleanupTestContainer(containerName);
        }
    }
}
