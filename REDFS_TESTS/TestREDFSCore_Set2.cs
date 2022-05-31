using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestREDFSCore_Set2
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        private void InitNewTestContainer(out string containerName)
        {
            REDFS.isTestMode = true;
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "Core_Set2_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\Core_Set2_" + id1;

            REDFS.AddNewContainer(co1);

            REDFS.MountContainer(true, co1.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);
            containerName = co1.containerName;

            ChunkInfo ci = new ChunkInfo();
            ci.allowedSegmentTypes = SEGMENT_TYPES.Default;
            //ci.id is automatically assigned
            ci.size = 2;
            ci.freeSpace = ci.size * 1024;
            ci.path = co1.containerPath + "\\cifile1.dat";
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

            DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 0, dataDefault2, null);
            DBNSegmentSpan dss2 = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, OPS.NUM_DBNS_IN_1GB, dataDefault1, null);

            //Note that we are creating these outside of 'REDFSBlockAllocator' but since we have loaded REDFSContainer, these will get properly
            //written out into the container files. We can simply load the REDFSBlockAllocator later and verify ondisk infomration is valid and as expected.
            DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            spanMap.InsertDBNSegmentSpan(dss1);
            spanMap.InsertDBNSegmentSpan(dss2);
        }

        private void CleanupTestContainer(string containerName)
        {
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(containerName);
        }

        [TestMethod]
        public void TestCloneOfRootVolumeAndFileCreation()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            //Clone the root volume.
            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(0, "cloneOfRoot", "Testing", "#000000");
            REDFS.redfsContainer.ReloadAllFSIDs();

            Assert.AreEqual(REDFS.redfsContainer.volumeManager.volumes.Count, 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[0] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[1] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1] != null);

            //for eas of suse
            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];


            //Lets create a file.
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].CreateFile("\\temp.dat");
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].getNumInodesInTree() == 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FileExists("\\temp.dat"));

            //Lets check the inode which ifsd_mux works on
            REDFSInode tempdat = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\temp.dat");
            Assert.AreEqual("temp.dat", tempdat.fileInfo.FileName);
            Assert.AreEqual(0, tempdat.fileInfo.Length);
            Assert.IsTrue(tempdat.fileInfo.Attributes.HasFlag(FileAttributes.Normal));

            //Lets check the inode that redfscore works on.
            RedFS_Inode wip = tempdat.myWIP;
            Assert.IsTrue(wip.spanType == SPAN_TYPE.DEFAULT);
            Assert.AreEqual(64, wip.get_ino());
            Assert.AreEqual(2, wip.get_parent_ino());
            Assert.AreEqual(0, wip.get_inode_level());
            Assert.AreEqual(1, wip.get_filefsid());
            Assert.AreEqual(0, wip.get_filesize());
            Assert.AreEqual(WIP_TYPE.REGULAR_FILE, wip.get_wiptype());
            Assert.AreEqual(0, wip.get_incore_cnt());

           
            CleanupTestContainer(containerName);
        }

        [TestMethod]
        public void TestHelperFunctions()
        {
            REDFSCore redfsCore = new REDFSCore();

            byte[] buffer = new byte[OPS.FS_BLOCK_SIZE];

            for (int i = 0; i < buffer.Length; i++) buffer[i] = 0xFF;

            buffer[2000] = 0xEF;
            buffer[3000] = 0xFE;

            Assert.AreEqual(2000 * 8 + 4, redfsCore.get_free_bitoffset(0, buffer));
            Assert.AreEqual(3000 * 8, redfsCore.get_free_bitoffset(0, buffer));
        }

        [TestMethod]
        public void TestCloneOfRootVolumeAndCreatingIMapWip()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            REDFSCore rfcc = REDFS.redfsContainer.ifsd_mux.redfsCore;

            //Clone the root volume.
            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(0, "cloneOfRoot", "Testing", "#000000");
            REDFS.redfsContainer.ReloadAllFSIDs();

            Assert.AreEqual(REDFS.redfsContainer.volumeManager.volumes.Count, 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[0] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[1] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1] != null);

            //Lets generate an inode number
            RedFS_FSID myNewFSID = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            
            int ino1 = REDFS.redfsContainer.ifsd_mux.redfsCore.NEXT_INODE_NUMBER(myNewFSID);
            int ino2 = REDFS.redfsContainer.ifsd_mux.redfsCore.NEXT_INODE_NUMBER(myNewFSID);

            //We have inserted segmentspans in order of 2nd gb of file to 1gb of dbn space and 1st gb of chunkfile to 2nd db of dbn space
            //So dbn 0 will be at offset of chunk1 @ 1GB.
            Assert.AreEqual(64, ino1);
            Assert.AreEqual(65, ino2);

            REDFS.redfsContainer.ifsd_mux.CreateFile(myNewFSID.get_fsid(), "\\tempFile.dat");
            REDFSInode tempdat = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[myNewFSID.get_fsid()].GetInode("\\tempFile.dat");

            Assert.AreEqual("tempFile.dat", tempdat.fileInfo.FileName);
            Assert.AreEqual(0, tempdat.fileInfo.Length);
            Assert.IsTrue(tempdat.fileInfo.Attributes.HasFlag(FileAttributes.Normal));

            //Lets check the inode that redfscore works on.
            RedFS_Inode wip = tempdat.myWIP;
            Assert.AreEqual(0, wip.get_filesize());

            //Lets grow the wip.
            REDFS.redfsContainer.ifsd_mux.SetEndOfFile(myNewFSID.get_fsid(), "\\tempFile.dat", 9999999999, false);
            Assert.AreEqual(9999999999, wip.get_filesize());

            PrintableWIP pwip = rfcc.redfs_list_tree(wip, Array.Empty<long>(), Array.Empty<int>());

            /*
             * Notice that we are trying to create a 68GB ~approx file while we have only 2 gb of segements.
             * This will work as the file creation is sparse files
             */ 
            REDFS.redfsContainer.ifsd_mux.SetEndOfFile(myNewFSID.get_fsid(), "\\tempFile.dat", 99999999999, false);
            Assert.AreEqual(wip.get_filesize(), 99999999999);

            PrintableWIP pwip1 = rfcc.redfs_list_tree(wip, Array.Empty<long>(), Array.Empty<int>());
            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;
            
            rfcore.redfs_discard_wip(wip);

            REDFSTree rftree = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[myNewFSID.get_fsid()];
            Thread.Sleep(5000);
            rftree.SyncTree();

            CleanupTestContainer(containerName);
        }

        [TestMethod]
        public void TestCloneOfRootVolumeAndBulkAllocs()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            REDFSCore rfcc = REDFS.redfsContainer.ifsd_mux.redfsCore;

            //Clone the root volume.
            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(0, "cloneOfRoot", "Testing", "#000000");
            REDFS.redfsContainer.ReloadAllFSIDs();

            Assert.AreEqual(REDFS.redfsContainer.volumeManager.volumes.Count, 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[0] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[1] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1] != null);

            //Lets generate an inode number
            RedFS_FSID myNewFSID = REDFS.redfsContainer.ifsd_mux.FSIDList[1];

            int ino1 = REDFS.redfsContainer.ifsd_mux.redfsCore.NEXT_INODE_NUMBER(myNewFSID);
            int ino2 = REDFS.redfsContainer.ifsd_mux.redfsCore.NEXT_INODE_NUMBER(myNewFSID);

            //We have inserted segmentspans in order of 2nd gb of file to 1gb of dbn space and 1st gb of chunkfile to 2nd db of dbn space
            //So dbn 0 will be at offset of chunk1 @ 1GB.
            Assert.AreEqual(64, ino1);
            Assert.AreEqual(65, ino2);

            REDFS.redfsContainer.ifsd_mux.CreateFile(myNewFSID.get_fsid(), "\\tempFile.dat");
            REDFSInode tempdat = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[myNewFSID.get_fsid()].GetInode("\\tempFile.dat");

            Assert.AreEqual("tempFile.dat", tempdat.fileInfo.FileName);
            Assert.AreEqual(0, tempdat.fileInfo.Length);
            Assert.IsTrue(tempdat.fileInfo.Attributes.HasFlag(FileAttributes.Normal));

            //Lets check the inode that redfscore works on.
            RedFS_Inode wip = tempdat.myWIP;
            Assert.AreEqual(0, wip.get_filesize());

            //Lets grow the wip.
            REDFS.redfsContainer.ifsd_mux.SetEndOfFile(myNewFSID.get_fsid(), "\\tempFile.dat", OPS.FS_BLOCK_SIZE * OPS.FS_SPAN_OUT + 1, true);
            Assert.AreEqual(OPS.FS_BLOCK_SIZE * OPS.FS_SPAN_OUT + 1, wip.get_filesize());

            PrintableWIP pwip = rfcc.redfs_list_tree(wip, Array.Empty<long>(), Array.Empty<int>());

            CleanupTestContainer(containerName);
        }
    }
}
