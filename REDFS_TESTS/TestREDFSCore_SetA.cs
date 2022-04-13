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
    public class TestREDFSCore_SetA
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        private void InitNewTestContainer(out string containerName)
        {
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "Core_SetA_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\Core_SetA_" + id1;

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

        private void CreateFewFilesInRootDir(int numFiles)
        {
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];

            //Lets create 1000 files
            for (int f = 0; f < numFiles; f++)
            {
                REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].CreateFile(rfsid, "\\temp_" + f + ".dat");
            }

            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].getNumInodesInTree() == numFiles + 1);
            RedFS_Inode rootDirWip = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP;
            RedFS_Inode inoFile = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");
            
            Assert.IsTrue(inoFile.is_dirty == true);
            //Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").isDirty == true);

            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree(rfsid);

            Assert.IsTrue(inoFile.is_dirty == false);
            Assert.IsTrue(rootDirWip.is_dirty == false);

            REDFSCore rfcore_new = REDFS.redfsContainer.ifsd_mux.redfsCore;
            //rfcore_new.redfs_discard_wip(inoFile);
            //rfcore_new.redfs_discard_wip(rootDirWip);
        }

        [TestMethod]
        public void TestREDFSCore0()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            //Now lets create a few files in the root dir and then unmount and remount
            CreateFewFilesInRootDir(1000);

            RedFS_FSID rfsid_p = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            RedFS_Inode inoFile_p = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");
            RedFS_Inode rootDirWip_p = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP;
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree(rfsid_p);

            REDFSCore rfcore_new = REDFS.redfsContainer.ifsd_mux.redfsCore;
            rfcore_new.redfs_discard_wip(inoFile_p);
            rfcore_new.redfs_discard_wip(rootDirWip_p);

            //Lets unmount and remount
            REDFS.UnmountContainer();

            //We have to do sync tree before cleanup or unmount
            REDFS.MountContainer(true, containerName);
            RedFS_FSID rfsid_temp = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree(rfsid_temp);

            //Now lets read the ino file & the root dir inode
            RedFS_Inode inoFile_t = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");
            RedFS_Inode rootDirWip_t = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP;

            rfcore_new = REDFS.redfsContainer.ifsd_mux.redfsCore;
            
            byte[] buffer2t = new byte[rootDirWip_t.get_filesize()];
            rfcore_new.redfs_read(rootDirWip_t, 0, buffer2t, 0, buffer2t.Length);
            string jsonString_t = Encoding.UTF8.GetString(buffer2t);

            rfcore_new.redfs_discard_wip(inoFile_t);
            rfcore_new.redfs_discard_wip(rootDirWip_t);

            CleanupTestContainer(containerName);
        }
    }
}
