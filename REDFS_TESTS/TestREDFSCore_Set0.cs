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
    public class TestREDFSCore_Set0
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        private void InitNewTestContainer(out string containerName)
        {
            REDFS.isTestMode = true;
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "Core_Set1_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\Core_Set1_" + id1;

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

        [TestMethod]
        public void TestREDFSCore0()
        {
            string containerName;
            InitNewTestContainer(out containerName);

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

            //for eas of suse
            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[0];

            RedFS_Inode inoFile = REDFS.redfsContainer.ifsd_mux.FSIDList[0].get_inode_file_wip("tester");

            //Create a higher level inode and a wip internallly
            REDFSInode rootDir = new REDFSInode(true, null, "\\");
            rootDir.CreateWipForNewlyCreatedInode(0, 2, -1);
            rootDir.items.Add("Vikrama");
            rootDir.items.Add("Reddy");
            rootDir.isDirty = true;
            rootDir.SyncInternal(inoFile, rfcore, null);

            rfcore.sync(inoFile);
            rfcore.sync(rootDir.myWIP);
            rfcore.flush_cache(inoFile, true);
            rfcore.flush_cache(rootDir.myWIP, true);

            //Lets see whats on disk.
            PrintableWIP inoWip = REDFS.redfsContainer.ifsd_mux.redfsCore.redfs_list_tree(inoFile);

            RedFS_Inode myWIP_t = new RedFS_Inode(WIP_TYPE.DIRECTORY_FILE, 2, -1);

            rfcore.redfs_checkout_wip(inoFile, myWIP_t, 2);
            byte[] buffer = new byte[myWIP_t.get_filesize()];
            rfcore.redfs_read(myWIP_t, 0, buffer, 0, buffer.Length);

            string jsonString = Encoding.UTF8.GetString(buffer);

            Console.WriteLine(inoWip.ino);

            rfcore.flush_cache(inoFile, true);
            rfcore.flush_cache(myWIP_t, true);

            Assert.IsTrue(rootDir.myWIP.Equals(myWIP_t));
            Assert.AreEqual(rootDir.cache_string, jsonString);

            CleanupTestContainer(containerName);
        }

        [TestMethod]
        public void TestREDFSCore1()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            ChunkInfo ci = new ChunkInfo();
            ci.allowedSegmentTypes = SEGMENT_TYPES.Default;
            ci.size = 4; //4GB
            ci.freeSpace = ci.size * 1024;
            ci.path = REDFS.getAbsoluteContainerPath() + "\\cifile1.dat";
            ci.speedClass = "default";
            ci.chunkIsAccessible = true;

            REDFS.redfsContainer.AddNewChunkToContainer(false, ci);

            REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.CHUNK_CREATE, "Create Chunking", "Writing", ci.id + "," + ci.path + "," + ci.size + "," + ci.allowedSegmentTypes);

            //Wait this will take a few seconds
            REDFS.redfsContainer.WaitForChunkCreationToComplete();

            DBNSegmentSpan[] dssList = DBNSegmentSpan.AutoGenerateContigiousSpans(SPAN_TYPE.DEFAULT, 0, 4, new int[] { ci.id }, null);

            DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            for (int i = 0; i < dssList.Length; i++)
            {
                spanMap.InsertDBNSegmentSpan(dssList[i]);
            }
            REDFS.redfsContainer.ReloadAllFSIDs();

            Assert.AreEqual(2, REDFS.redfsContainer.redfsChunks.Count);

            //Clone the root volume.
            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(0, "cloneOfRoot", "Testing", "#000000");
            REDFS.redfsContainer.ReloadAllFSIDs();

            Assert.AreEqual(REDFS.redfsContainer.volumeManager.volumes.Count, 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[0] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[1] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 2);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1] != null);

            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].CreateFile("\\temp.dat");

            RedFS_Inode inoFile = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");
            PrintableWIP pwip1 = REDFS.redfsContainer.ifsd_mux.redfsCore.redfs_list_tree(inoFile);
            

            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();
            PrintableWIP pwip2 = REDFS.redfsContainer.ifsd_mux.redfsCore.redfs_list_tree(inoFile);

            CleanupTestContainer(containerName);
        }


        [TestMethod]
        public void TestREDFSCore2()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            ChunkInfo ci = new ChunkInfo();
            ci.allowedSegmentTypes = SEGMENT_TYPES.Default;
            ci.size = 4; //4GB
            ci.freeSpace = ci.size * 1024;
            ci.path = REDFS.getAbsoluteContainerPath() + "\\cifile1.dat";
            ci.speedClass = "default";
            ci.chunkIsAccessible = true;

            REDFS.redfsContainer.AddNewChunkToContainer(false, ci);

            REDFS.redfsContainer.containerOperations.CreateNewOperation(ALLOWED_OPERATIONS.CHUNK_CREATE, "Create Chunking", "Writing", ci.id + "," + ci.path + "," + ci.size + "," + ci.allowedSegmentTypes);

            //Wait this will take a few seconds
            REDFS.redfsContainer.WaitForChunkCreationToComplete();

            DBNSegmentSpan[] dssList = DBNSegmentSpan.AutoGenerateContigiousSpans(SPAN_TYPE.DEFAULT, 0, 4, new int[] { ci.id } , null);

            DBNSegmentSpanMap spanMap = REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.dbnSpanMap;
            for (int i=0;i<dssList.Length;i++)
            {
                spanMap.InsertDBNSegmentSpan(dssList[i]);
            }
            REDFS.redfsContainer.ReloadAllFSIDs();

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

            //Lets create 1000 files
            for (int f = 0; f < 1000; f++)
            {
                REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].CreateFile("\\temp_" + f + ".dat");
            }
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].getNumInodesInTree() == 1001);
            RedFS_Inode rootDirWip = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP;
            RedFS_Inode inoFile = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");

            Assert.IsTrue(inoFile.is_dirty == true);
            //Assert.IsTrue(rootDirWip.is_dirty == true);

            Thread.Sleep(5000);

            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();

            Assert.IsTrue(inoFile.is_dirty == false);
            Assert.IsTrue(rootDirWip.is_dirty == false);

            Assert.IsTrue(rootDirWip.get_filesize() > 9999);

            /*
            byte[] buffer = new byte[rootDirWip.get_filesize()];
            rfcore.redfs_read(rootDirWip, 0, buffer, 0, buffer.Length);    //first read

            //This json has 1000 files metadata, when we read back, we should be able to get this info back from redfs.
            string jsonString = Encoding.UTF8.GetString(buffer);

            //now check by creating a new wip and reading
            RedFS_Inode rootDirWip_again = new RedFS_Inode(WIP_TYPE.DIRECTORY_FILE, 2, -1);
            rfcore.redfs_checkout_wip(inoFile, rootDirWip_again, 2);

            Assert.IsTrue(rootDirWip_again.Equals(rootDirWip));           //first check

            //verify 
            byte[] buffer_again = new byte[rootDirWip_again.get_filesize()];
            rfcore.redfs_read(rootDirWip_again, 0, buffer_again, 0, buffer_again.Length); //second read

            Assert.IsTrue(rootDirWip_again.Equals(rootDirWip));         //second check

            string jsonString_again = Encoding.UTF8.GetString(buffer_again);
            Assert.AreEqual(jsonString, jsonString_again);
            */

            REDFS.UnmountContainer();

            //************ now verify on disk **********************


            REDFS.MountContainer(true, containerName);

            //Read the new object
            REDFSCore rfcore_new = REDFS.redfsContainer.ifsd_mux.redfsCore;

            RedFS_Inode inoFile_t = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");

            RedFS_FSID myNewFSID = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            //REDFS.redfsContainer.ReloadAllFSIDs();
            RedFS_Inode rootDirWip_t = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP;

            RedFS_Inode rootDirWip_x = new RedFS_Inode(WIP_TYPE.DIRECTORY_FILE, 2, -1);
            rfcore_new.redfs_checkout_wip(inoFile_t, rootDirWip_x, 2);

            Assert.IsFalse(rootDirWip_x.is_dirty);

            Assert.IsTrue(rootDirWip_t.Equals(rootDirWip_x));

            Assert.IsTrue(rootDirWip_t.get_filesize() == rootDirWip_x.get_filesize());
            Assert.IsTrue(rootDirWip_t.get_filesize() > 9999);

            byte[] buffer2t = new byte[rootDirWip_t.get_filesize()];
            rfcore_new.redfs_read(rootDirWip_t, 0, buffer2t, 0, buffer2t.Length);
            string jsonString_t = Encoding.UTF8.GetString(buffer2t);
            
            byte[] buffer2x = new byte[rootDirWip_x.get_filesize()];
            rfcore_new.redfs_read(rootDirWip_x, 0, buffer2x, 0, buffer2x.Length);
            string jsonString_x = Encoding.UTF8.GetString(buffer2x);

            Assert.IsFalse(rootDirWip_x.is_dirty);

            //Assert.AreEqual(jsonString_t, jsonString_again);
            Assert.AreEqual(jsonString_x, jsonString_t);

            //This should also  match and jsonString_t should have metadata of the 1000 files we creategct4fff
            //Assert.AreEqual(jsonString.Length, jsonString_t.Length);

            Assert.IsTrue(rootDirWip.Equals(rootDirWip_t));
            

            RedFS_FSID rfsid_temp = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();

            Assert.IsFalse(rootDirWip_x.is_dirty);

            rfcore_new.redfs_discard_wip(rootDirWip_x);
            rfcore_new.redfs_discard_wip(inoFile_t);

            CleanupTestContainer(containerName);
        }
    }
}
