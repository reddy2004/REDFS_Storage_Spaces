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
    public class IntroTestVolumeClone
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
            co1.containerName = "IntroTestVolClone_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\IntroTestVolClone_" + id1;

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

        [TestMethod]
        public void IntroTest_7()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 2);

            //Objects of interest
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFSTree rftree = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1];
            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;

            //Clone volume using volumeManager, Do not try to clone using dup_fsid as it wont keep track of the volumes
            //in volumeManager.
            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(1, "cloneOfFirstVol", "Testing", "#000000");
            REDFS.redfsContainer.ReloadAllFSIDs();

            RedFS_FSID rfsid_clone = REDFS.redfsContainer.ifsd_mux.FSIDList[2];
            Assert.AreEqual(rfsid_clone.get_fsid(), 2);
            
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 3);
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2] != null);

            RedFS_Inode myWIP1 = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");
            RedFS_Inode myWIP2 = REDFS.redfsContainer.ifsd_mux.FSIDList[2].get_inode_file_wip("tester");

            PrintableWIP pwip1 = rfcore.redfs_list_tree(myWIP1, Array.Empty<long>(), Array.Empty<int>());
            PrintableWIP pwip2 = rfcore.redfs_list_tree(myWIP2, Array.Empty<long>(), Array.Empty<int>());

            Assert.AreEqual(pwip1.wipIdx.Length, 16); //16 pointers of L2 blocks
            Assert.AreEqual(pwip1.L0_DBNS.Length, 16777216); 
            Assert.AreEqual(pwip1.L1_DBNS.Length, 16384);

            Assert.AreEqual(pwip2.wipIdx.Length, 16); //16 pointers of L2 blocks
            Assert.AreEqual(pwip2.L0_DBNS.Length, 16777216);
            Assert.AreEqual(pwip2.L1_DBNS.Length, 16384);

            /*
             * Now lets check refcounts of these blocks. We have a wip that has only L2 blocks allocated while
             * the L1s are just marked 0. This will work because reading block 0 will yeild dbns as 0 for L0 blocks
             * essentially meaning we have a 128GB file filled with zeros.
             */

            for (int i = 0; i < pwip1.wipIdx.Length; i++)
            {
                long dbn = pwip1.wipIdx[i];
                int refcnt = 0, childrefcnt = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn, ref refcnt, ref childrefcnt);

                Assert.AreEqual(2, refcnt);
                Assert.AreEqual(0, childrefcnt);
            }

            int nonZeroDbns = 0;
            for (int i = 0; i < pwip2.L1_DBNS.Length; i++)
            {
                long dbn = pwip2.L1_DBNS[i];

                //Ideally L1 dbns are 0 as this is a prealloced file
                if (dbn != 0)
                {
                    int refcnt = 0, childrefcnt = 0;
                    rfcore.redfsBlockAllocator.GetRefcounts(dbn, ref refcnt, ref childrefcnt);

                    Assert.AreEqual(2, refcnt);
                    Assert.AreEqual(0, childrefcnt);

                    nonZeroDbns++;
                }
            }

            //Take the ref since we have done a reload of ReloadAllFSIDs();
            rftree = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1];
            DebugSummaryOfFSID dsof = new DebugSummaryOfFSID();
            rftree.GetDebugSummaryOfFSID(dsof);

            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].FlushCacheL0s();
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();

            Assert.AreEqual(nonZeroDbns, 1); //for root directory which was written out.
            CleanupTestContainer(containerName);
        }

        private void CreateCloneOfNewVolumeAndVerify()
        {
            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;

            int[] refs;
            int[] childrefs;

            //Clone volume using volumeManager, Do not try to clone using dup_fsid as it wont keep track of the volumes
            //in volumeManager.
            PrintableWIP rootDirSrc = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });
            PrintableWIP inoFileSrc = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("test"), new long[] { 0 }, new int[] { 0 });
            rfcore.redfs_get_refcounts(rootDirSrc.wipIdx, out refs, out childrefs);

            //helps to drain out the queue in wrloader
            rfcore.redfs_get_refcounts(new long[] { 1048, 1049, 1050, 1052 }, out refs, out childrefs);

            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(1, "cloneOfFirstVol", "Testing", "#000000");
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 3);

            rfcore.redfs_get_refcounts(new long[] { 1048, 1049, 1050, 1052 }, out refs, out childrefs);

            PrintableWIP rootDirSrcAfter = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });

            //Check the ino wip for fsid 2 and 1
            PrintableWIP inoFileSrcAfter = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("test"), new long[] { 0 }, new int[] { 0 });
            PrintableWIP inoFileDest = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.FSIDList[2].get_inode_file_wip("test"), new long[] { 0 }, new int[] { 0 });
            PrintableWIP rootDirDest = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });

            Assert.IsTrue(rootDirSrc.IsSimilarTree(rootDirSrcAfter));
            Assert.IsTrue(rootDirSrc.IsSimilarTree(rootDirDest));


            Assert.IsTrue(inoFileSrc.IsSimilarTree(inoFileSrcAfter));
            Assert.IsTrue(inoFileSrc.IsSimilarTree(inoFileDest));



            rfcore.redfs_get_refcounts(inoFileSrc.L0_DBNS, out refs, out childrefs);
            rfcore.redfs_get_refcounts(inoFileSrc.L1_DBNS, out refs, out childrefs);

            rfcore.redfs_get_refcounts(inoFileDest.L0_DBNS, out refs, out childrefs);
            rfcore.redfs_get_refcounts(inoFileDest.L1_DBNS, out refs, out childrefs);

            rfcore.redfs_get_refcounts(rootDirSrc.wipIdx, out refs, out childrefs);

            REDFS.redfsContainer.ReloadAllFSIDs();

        }

        /*
         * Same as the introtest_7 above, but this time we will do clones of clones and write out some empty files
         * and validate that they are distinct in each fsid and also inode and data refcounts reflect accordingly.
         */
        [TestMethod]
        public void IntroTest_8()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            int refcnt2a = 0, childrefcnt2a = 0;
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 2);

            CreateCloneOfNewVolumeAndVerify();

            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;

            //Get root dirs of both fsid 1 and 2
            PrintableWIP pwip2b = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });
            PrintableWIP pwip2bx = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });

            RedFS_FSID rfsid_clone = REDFS.redfsContainer.ifsd_mux.FSIDList[2];
            Assert.AreEqual(rfsid_clone.get_fsid(), 2);

            //Check the ino wip for fsid 2 and 1
            PrintableWIP pwipino1 = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("test"), new long[] { 0 }, new int[] { 0 }); 
            PrintableWIP pwipino2 =  rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.FSIDList[2].get_inode_file_wip("test"), new long[] { 0 }, new int[] { 0 });

            Assert.IsTrue(pwipino1.IsSimilarTree(pwipino2));


            //So far so good.

            //Now create a few files and write out some data. We will create OPS.FS_SPAN_OUT*OPS.NUM_WIPS_IN_BLOCK + 1 inodes in the cloned volume
            //so that the inodefile has two blocks worth of data and hence two L1 dbns will be cowed.
            for (int fileid = 0; fileid < (1 + OPS.FS_SPAN_OUT * OPS.NUM_WIPS_IN_BLOCK); fileid++) {
                
                byte[] buffer = new byte[OPS.FSID_BLOCK_SIZE];
                int bytesWritten = 0;
                Random r = new Random();
                r.NextBytes(buffer);
                REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].CreateFile("\\tempfile.dat." + fileid);

                REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].WriteFile("\\tempfile.dat." + fileid, buffer, out bytesWritten, 0);

            }

            PrintableWIP pwip2c1 = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });

            Thread.Sleep(1000);
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();


            PrintableWIP pwip2c2 = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });

            rfcore.redfsBlockAllocator.GetRefcounts(1024, ref refcnt2a, ref childrefcnt2a);
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].SyncTree();
            rfcore.redfsBlockAllocator.GetRefcounts(1024, ref refcnt2a, ref childrefcnt2a);

            PrintableWIP pwip2c = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });

            RedFS_Inode myWIP1 = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");
            RedFS_Inode myWIP2 = REDFS.redfsContainer.ifsd_mux.FSIDList[2].get_inode_file_wip("tester");

            Assert.AreEqual(myWIP2.get_filesize(), myWIP1.get_filesize());

            PrintableWIP pwip1 = rfcore.redfs_list_tree(myWIP1, Array.Empty<long>(), Array.Empty<int>());
            PrintableWIP pwip2 = rfcore.redfs_list_tree(myWIP2, Array.Empty<long>(), Array.Empty<int>());

            Assert.AreEqual(pwip1.wipIdx.Length, 16); //16 pointers of L2 blocks
            Assert.AreEqual(pwip1.L0_DBNS.Length, 16777216);
            Assert.AreEqual(pwip1.L1_DBNS.Length, 16384);

            Assert.AreEqual(pwip2.wipIdx.Length, 16); //16 pointers of L2 blocks
            Assert.AreEqual(pwip2.L0_DBNS.Length, 16777216);
            Assert.AreEqual(pwip2.L1_DBNS.Length, 16384);

            /*
             * Now lets check refcounts of these blocks. We have a wip that has only L2 blocks allocated while
             * the L1s are just marked 0. This will work because reading block 0 will yeild dbns as 0 for L0 blocks
             * essentially meaning we have a 128GB file filled with zeros.
             */

            for (int i = 0; i < pwip1.wipIdx.Length; i++)
            {
                long dbn1 = pwip1.wipIdx[i];
                int refcnt1 = 0, childrefcnt1 = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn1, ref refcnt1, ref childrefcnt1);

                long dbn2 = pwip2.wipIdx[i];
                int refcnt2 = 0, childrefcnt2 = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn2, ref refcnt2, ref childrefcnt2);

                //Since the first L2 dbn stored in wip has been cow;d and rest have been touched!
                if (i == 0)
                {
                    Assert.AreEqual(1, refcnt2);
                    Assert.AreEqual(0, childrefcnt2);
                    Assert.AreEqual(1, refcnt1);
                    Assert.AreEqual(0, childrefcnt1);
                }
                else
                {
                    Assert.AreEqual(2, refcnt2);
                    Assert.AreEqual(0, childrefcnt2);
                    Assert.AreEqual(2, refcnt1);
                    Assert.AreEqual(0, childrefcnt1);
                }
            }

            int nonZeroDbns = 0;
            for (int i = 0; i < pwip2.L1_DBNS.Length; i++)
            {
                long dbn2 = pwip2.L1_DBNS[i];
                long dbn1 = pwip1.L1_DBNS[i];

                //Ideally L1 dbns are 0 as this is a prealloced file
                if (dbn2 != 0)
                {
                    int refcnt = 0, childrefcnt = 0;
                    rfcore.redfsBlockAllocator.GetRefcounts(dbn2, ref refcnt, ref childrefcnt);

                    Assert.AreEqual(1, refcnt);
                    Assert.AreEqual(0, childrefcnt);

                    nonZeroDbns++;
                }

                //Ideally L1 dbns are 0 as this is a prealloced file
                if (dbn1 != 0)
                {
                    int refcnt = 0, childrefcnt = 0;
                    rfcore.redfsBlockAllocator.GetRefcounts(dbn1, ref refcnt, ref childrefcnt);

                    Assert.AreEqual(1, refcnt);
                    Assert.AreEqual(0, childrefcnt);

                    nonZeroDbns++;
                }
            }


            PrintableWIP pwip2d = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });


            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].FlushCacheL0s();
            Assert.AreEqual(nonZeroDbns, 3); //for root directory which was written out.

            Thread.Sleep(50000);

            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(2, "cloneOfSecondVol", "Testing", "#000000");
            PrintableWIP pwip3a = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });

            PrintableWIP pwip2m2 = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });
            PrintableWIP pwip2m3 = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[3].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });

            REDFS.redfsContainer.ReloadAllFSIDs();
            PrintableWIP pwip3b = rfcore.redfs_list_tree(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP, new long[] { 0 }, new int[] { 0 });

            RedFS_FSID rfsid_clone_again = REDFS.redfsContainer.ifsd_mux.FSIDList[3];
            
            DebugSummaryOfFSID dsof3 = new DebugSummaryOfFSID();
            DebugSummaryOfFSID dsof2 = new DebugSummaryOfFSID();
            //REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[3].GetDebugSummaryOfFSID(dsof3);
            //REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[2].GetDebugSummaryOfFSID(dsof2);

            Assert.AreEqual(dsof3.numDirectories, dsof2.numDirectories);
            Assert.AreEqual(dsof3.numFiles, dsof2.numFiles);
            Assert.AreEqual(dsof3.numL0s, dsof2.numL0s);
            Assert.AreEqual(dsof3.numL1s, dsof2.numL1s);
            Assert.AreEqual(dsof3.numL2s, dsof2.numL2s);
            Assert.AreEqual(dsof3.totalLogicalData, dsof2.totalLogicalData);
            

            CleanupTestContainer(containerName);
        }

        /*
         * Create vol->writedata ->do backed clone->delete data from snapshot->clone orig volume ===> crash
         * Create a volume vol1 with id 1 and write data
         * Do backed clone. The clone gets id = 2 and snapshot id = 3
         * Delete some data in snapshot i.e id volid=3
         * try cloning vol1
         */ 
        [TestMethod]
        public void IntroTest_9()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            REDFS.isTestMode = false;

            for (int fileid = 0; fileid <= 10; fileid++)
            {

                byte[] buffer = new byte[OPS.FSID_BLOCK_SIZE];
                int bytesWritten = 0;
                Random r = new Random();
                r.NextBytes(buffer);
                REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].CreateFile("\\tempfile.dat." + fileid);
                REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].WriteFile("\\tempfile.dat." + fileid, buffer, out bytesWritten, 0);
            }

            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 2);
            //Now lets create a backed clone of volid 1
            REDFS.redfsContainer.volumeManager.CloneVolume(1, "backedSecondVol");

            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.numValidFsids == 4);
            //Delete few files
            for (int fileid = 1; fileid < 5; fileid++)
            {
                REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[3].DeleteFile("\\tempfile.dat." + fileid);
            }

            //clone the volume with id 1
            REDFS.redfsContainer.volumeManager.CloneVolumeRaw(1, "cloneagain", "Testing", "#0000FF");
            REDFS.redfsContainer.ReloadAllFSIDs();

            CleanupTestContainer(containerName);
        }
    }
}
