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
     * Issue observed.
     * Create a volume from root volume and copy files to it.
     * Create a snapshot and you will find the same files in it (new volume) obviuosly. Delete a file from liveview
     * Clone the snapshot copy.
     * Both the clone and live view will have empty volumes. Looks like the entire tree is being overwritten.
     */
    [TestClass]
    public class Issue_SS_and_Clone_1
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
            co1.containerName = "Issue_SS_and_Clone_1_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\Issue_SS_and_Clone_1_" + id1;

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

            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;

            PrintableWIP pwip_ifile = rfcore.redfs_list_tree(rfsid.get_inode_file_wip("c"), Array.Empty<long>(), Array.Empty<int>());
            PrintableWIP pwip_imap = rfcore.redfs_list_tree(rfsid.get_inodemap_wip(), Array.Empty<long>(), Array.Empty<int>());
            PrintableWIP pwip_rootdir = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, Array.Empty<long>(), Array.Empty<int>());

            Assert.AreEqual(18, pwip_ifile.getTotalOndiskBlocks());
            Assert.AreEqual(8, pwip_imap.getTotalOndiskBlocks());
            Assert.AreEqual(1, pwip_rootdir.getTotalOndiskBlocks());

            Assert.AreEqual(pwip_imap.getTotalOndiskBlocks() + pwip_ifile.getTotalOndiskBlocks() + pwip_rootdir.getTotalOndiskBlocks(), 27);
        }

        private void CreateSnapshotOfVolume(int volid)
        {

            REDFS.redfsContainer.ifsd_mux.Sync();
            Assert.AreEqual(2, REDFS.redfsContainer.redfsChunks.Count);
            //Clone the root volume.
            REDFS.redfsContainer.volumeManager.VolumeSnapshot(volid, "Testing SS");
            REDFS.redfsContainer.ReloadAllFSIDs();

            Assert.AreEqual(REDFS.redfsContainer.volumeManager.volumes.Count, 3);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[0] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[1] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[2] != null);

            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 3);

            //Our new 'liveview' tree should be online and available.
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2] != null);

            //To recover all allocated buffers
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();

            RedFS_FSID rfsid_t = REDFS.redfsContainer.ifsd_mux.FSIDList[2];
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].SyncTree();


            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;

            //Now check the source, i.e volid pass in function parameter
            PrintableWIP pwip_ifile = rfcore.redfs_list_tree(rfsid.get_inode_file_wip("c"), Array.Empty<long>(), Array.Empty<int>());
            PrintableWIP pwip_imap = rfcore.redfs_list_tree(rfsid.get_inodemap_wip(), Array.Empty<long>(), Array.Empty<int>());
            PrintableWIP pwip_rootdir = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, Array.Empty<long>(), Array.Empty<int>());

            Assert.AreEqual(18 + 4, pwip_ifile.getTotalOndiskBlocks()); //4 blocks added for 100 files.
            Assert.AreEqual(8+1, pwip_imap.getTotalOndiskBlocks()); //1 block for the above files
            Assert.AreEqual(1+2, pwip_rootdir.getTotalOndiskBlocks()); //we have data of file names etc in 2 blocks

            Assert.AreEqual(27 + 7, pwip_imap.getTotalOndiskBlocks() + pwip_ifile.getTotalOndiskBlocks() + pwip_rootdir.getTotalOndiskBlocks());


            //Now check the destination, i.e the new volume created in this function
            PrintableWIP pwip_ifile_t = rfcore.redfs_list_tree(rfsid_t.get_inode_file_wip("c"), Array.Empty<long>(), Array.Empty<int>());
            PrintableWIP pwip_imap_t = rfcore.redfs_list_tree(rfsid_t.get_inodemap_wip(), Array.Empty<long>(), Array.Empty<int>());
            PrintableWIP pwip_rootdir_t = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].GetInode("\\").myWIP, Array.Empty<long>(), Array.Empty<int>());

            Assert.AreEqual(18 + 4, pwip_ifile_t.getTotalOndiskBlocks()); //since we are snapshotting from source volume id, this should be same as srouce volume
            Assert.AreEqual(8+ 1, pwip_imap_t.getTotalOndiskBlocks()); //since we are snapshotting from source volume id, this should be same as srouce volume
            Assert.AreEqual(1 + 2, pwip_rootdir_t.getTotalOndiskBlocks()); //since we are snapshotting from source volume id, this should be same as srouce volume

            Assert.AreEqual(27 + 7, pwip_imap_t.getTotalOndiskBlocks() + pwip_ifile_t.getTotalOndiskBlocks() + pwip_rootdir_t.getTotalOndiskBlocks());

            Assert.IsTrue(pwip_imap.IsSimilarTree(pwip_imap_t));
            Assert.IsTrue(pwip_ifile.IsSimilarTree(pwip_ifile_t));
            Assert.IsTrue(pwip_rootdir.IsSimilarTree(pwip_rootdir_t));
            REDFS.redfsContainer.ifsd_mux.Sync();
        }

        private void CreateFilesInVolume(int volid)
        {
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[volid];
            for (int i = 0; i < 100; i++)
            {
                REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[volid].CreateFile("\\temp_" + i + ".dat");
            }
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[volid].getNumInodesInTree() == 101);
        }

        public void CreateCloneOfVolume(int volid)
        {
            //Clone the root volume.
            REDFS.redfsContainer.ifsd_mux.Sync();
            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(volid, "Testing SS", "desc", "FFFFFF");
            REDFS.redfsContainer.ReloadAllFSIDs();

            Assert.AreEqual(REDFS.redfsContainer.volumeManager.volumes.Count, 4);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[0] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[1] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[2] != null);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.FSIDList[3] != null);
        }

        private void CompareFilesInTwoVolumes(int volid1, int volid2, int expected1, int expected2)
        {
            IList<DokanNet.FileInformation> list1 = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[volid1].FindFilesWithPattern("\\", "*");
            IList<DokanNet.FileInformation> list2 = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[volid2].FindFilesWithPattern("\\", "*");

            foreach(DokanNet.FileInformation listitem in  list1)
            {

            }

            foreach(DokanNet.FileInformation listitem in list2)
            {

            }

            Assert.AreEqual(list1.Count, expected1);
            Assert.AreEqual(list2.Count, expected2);
        }

        [TestMethod]
        public void IssueTest_1_1()
        {
            string containerName;

            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            //Lets create some files in the new volume.
            CreateFilesInVolume(1);

            //Lets create real snapshots instead of simulating 
            REDFS.isTestMode = false;
            CreateSnapshotOfVolume(1);

            //Now we have a basevolume+its snapshot. Now lets clone the snapshot.
            CreateCloneOfVolume(1);

            CompareFilesInTwoVolumes(1, 3, 100, 100);
            CompareFilesInTwoVolumes(1, 2, 100, 100);
            CompareFilesInTwoVolumes(2, 3, 100, 100);

            CleanupTestContainer(containerName);
        }

        [TestMethod]
        public void IssueTest_1_2()
        {
            string containerName;

            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            //Lets create some files in the new volume.
            CreateFilesInVolume(1);
            Assert.AreEqual(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].getNumInodesInTree(), 101);

            //Lets create real snapshots instead of simulating 
            REDFS.isTestMode = false;
            CreateSnapshotOfVolume(1);
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].LoadRootDirectoryIntoInodeHashmap();
            Assert.AreEqual(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].getNumInodesInTree(), 101);

            //Delete one file in the liveview, i.e from volid = 2
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].SyncTree();
            Assert.AreEqual(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].getNumInodesInTree(), 101);
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].DeleteFile("\\temp_1.dat");
            Assert.AreEqual(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].getNumInodesInTree(), 100);


            //Now we have a basevolume+its snapshot. Now lets clone the snapshot.
            CreateCloneOfVolume(1);

            CompareFilesInTwoVolumes(1, 3, 100, 100);
            CompareFilesInTwoVolumes(1, 2, 100, 99);
            CompareFilesInTwoVolumes(2, 3, 99, 100);

            CleanupTestContainer(containerName);
        }
    }
}
