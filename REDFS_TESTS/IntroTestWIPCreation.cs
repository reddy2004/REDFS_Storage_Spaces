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
            REDFS.isTestMode = true;

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
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].CreateFile("\\temp.dat");
            Assert.IsTrue(REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].getNumInodesInTree() == 2);

            //RedFS_Inode rootDirWip = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].GetInode("\\").myWIP;
            //RedFS_Inode inoFile = REDFS.redfsContainer.ifsd_mux.FSIDList[1].get_inode_file_wip("tester");

            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();

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
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].SyncTree();
            /*
             * We dont care that data is written out correctly or flushed. A background thread flushes all dirty buffers
             * to disk. The wrapper ensures that we read what we just wrote. it does  not matter to the caller if the data is
             * in memory or disk.
             */
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].ReadFile("\\temp.dat", buffer_out, out bytesRead, 0);

            Assert.AreEqual(bytesRead, 99999);

            for (int i = 0; i < 99999; i++)
            {
                Assert.AreEqual(buffer_in[i], buffer_out[i]);
            }
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();
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
         * 
         * In the below test, we create an inode and verify read/write and we have the correct block allocation. then we clone this wip and
         * verify that its exactly the same as the source wip.
         */
        [TestMethod]
        public void IntroTest_2()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];

            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;
            int newInodeNum = rfcore.NEXT_INODE_NUMBER(rfsid);

            RedFS_Inode myWIP = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, newInodeNum, 0);

            int dataSize = 80000;

            byte[] buffer_in = new byte[dataSize];
            byte[] buffer_out = new byte[dataSize];

            Random r = new Random();
            r.NextBytes(buffer_in);
            r.NextBytes(buffer_out); //just scramble it, it should be overritten with buffer_in's data when we read back.

            /*
             * Use the wip to write out some data to disk. You could write out any arbitrary data encoded to byte array. 
             * For ex. Im using a simple json to store the directory info and writing out the json->byte array for the dir wip in the project
             */
            rfcore.redfs_write(myWIP, 0, buffer_in, 0, dataSize, WRITE_TYPE.OVERWRITE_IN_PLACE);

            /*
             * Read directly using the REDFSCore.
             */
            rfcore.redfs_read(myWIP, 0, buffer_out, 0, dataSize);

            for (int i = 0; i < dataSize; i++)
            {
                Assert.AreEqual(buffer_in[i], buffer_out[i]);
            }


            /*
             * Since we are working with wips directly, we have to free up the allocated memory.
             */
            rfcore.sync(myWIP);

            PrintableWIP pwip = rfcore.redfs_list_tree(myWIP, Array.Empty<long>(), Array.Empty<int>());

            Assert.AreEqual(pwip.wipIdx.Length, 10); //10 blocks
            Assert.AreEqual(pwip.L0_DBNS, null); //coz all dbns are stored in wip itself
            Assert.AreEqual(pwip.L1_DBNS, null);

            /*
             * Now lets check refcounts of these blocks
             */

            for (int i = 0; i < pwip.wipIdx.Length; i++)
            {
                long dbn = pwip.wipIdx[i];
                int refcnt = 0, childrefcnt = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn, ref refcnt, ref childrefcnt);

                Assert.AreEqual(1, refcnt);
                Assert.AreEqual(0, childrefcnt);
            }

            Console.WriteLine("comparision completed!");
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

            /*
             * Now lets get the same wip we wrote to recently and clone it. Notice that this is not a part of
             * the inode file as we have no reference to inode file (inonum = 0). inode file is also a regular
             * file as far as RedfsCore is concerned and inode file itself is stored in the fsid.
             */
            int cloneInodeNum = rfcore.NEXT_INODE_NUMBER(rfsid);

            RedFS_Inode myWIP_t = myWIP; //since we have not stored it anywhere

            RedFS_Inode myWIP_c = rfcore.redfs_clone_wip(myWIP_t);

            r.NextBytes(buffer_out);

            rfcore.redfs_read(myWIP_c, 0, buffer_out, 0, buffer_out.Length);

            for (int i = 0; i < dataSize; i++)
            {
                Assert.AreEqual(buffer_in[i], buffer_out[i]);
            }

            rfcore.sync(myWIP_c);

            PrintableWIP pwip_c = rfcore.redfs_list_tree(myWIP_c, Array.Empty<long>(), Array.Empty<int>());

            Assert.AreEqual(pwip_c.wipIdx.Length, 10); //10 blocks
            Assert.AreEqual(pwip_c.L0_DBNS, null); //coz all dbns are stored in wip itself
            Assert.AreEqual(pwip_c.L1_DBNS, null);

            /*
             * Now lets check refcounts of these blocks in cloned file
             */

            for (int i = 0; i < pwip_c.wipIdx.Length; i++)
            {
                long dbn = pwip.wipIdx[i];
                long dbn_c = pwip_c.wipIdx[i];

                int refcnt = 0, childrefcnt = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn_c, ref refcnt, ref childrefcnt);

                Assert.AreEqual(2, refcnt);   //should be 2
                Assert.AreEqual(0, childrefcnt);

                Assert.AreEqual(dbn, dbn_c); //should be same as its a clone
            }

            rfcore.redfs_discard_wip(myWIP_t);
            rfcore.redfs_discard_wip(myWIP_c);

            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();
            CleanupTestContainer(containerName);
        }

        /*
         * In this test, we test clone of L0 file, i.e L0s are present in the wip itself
         */ 
        [TestMethod]
        public void IntroTest_3()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];

            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;

            int dataSize = 1024 * 1024; //9MB XXX todo this fails after 64 blocks. i.e > 512K

            byte[] buffer_in = new byte[dataSize];
            byte[] buffer_out = new byte[dataSize];
            byte[] buffer_raw_read = new byte[dataSize];

            Random r = new Random();
            r.NextBytes(buffer_in);
            r.NextBytes(buffer_out); //just scramble it, it should be overritten with buffer_in's data when we read back.

            int newInodeNum = rfcore.NEXT_INODE_NUMBER(rfsid);

            RedFS_Inode myWIP = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, newInodeNum, 0);
            /*
             * Use the wip to write out some data to disk. You could write out any arbitrary data encoded to byte array. 
             */
            rfcore.redfs_write(myWIP, 0, buffer_in, 0, dataSize, WRITE_TYPE.OVERWRITE_IN_PLACE);

            rfcore.sync(myWIP);
            rfcore.flush_cache(myWIP, false);

            RedFS_Inode myWIP_clone = rfcore.redfs_clone_wip(myWIP);

            PrintableWIP pwip_0 = rfcore.redfs_list_tree(myWIP_clone, Array.Empty<long>(), Array.Empty<int>());

            int refcntL1 = 0, childrefcntL1 = 0;
            rfcore.redfsBlockAllocator.GetRefcounts(pwip_0.wipIdx[0], ref refcntL1, ref childrefcntL1);
            Assert.AreEqual(2, refcntL1);
            Assert.AreEqual(0, childrefcntL1); //we have already touched L1, so child counts L0's will be ref=2 as well as the L1 with ref=2

            for (int i = 0; i < pwip_0.L0_DBNS.Length; i++)
            {
                long dbn = pwip_0.L0_DBNS[i];
                int refcnt = 0, childrefcnt = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn, ref refcnt, ref childrefcnt);
                Assert.AreEqual(2, refcnt);
                Assert.AreEqual(0, childrefcnt); //Should be 0 for L0
            }

            rfcore.redfs_write(myWIP_clone, 0, buffer_in, 0, OPS.FS_BLOCK_SIZE, WRITE_TYPE.OVERWRITE_IN_PLACE);
            rfcore.sync(myWIP_clone);
            rfcore.flush_cache(myWIP_clone, false);

           

            PrintableWIP pwip_1 = rfcore.redfs_list_tree(myWIP_clone, Array.Empty<long>(), Array.Empty<int>());

            int refcntL1a = 0, childrefcntL1a = 0;
            rfcore.redfsBlockAllocator.GetRefcounts(pwip_1.wipIdx[0], ref refcntL1a, ref childrefcntL1a);
            Assert.AreEqual(1, refcntL1a); //Notice L1s ref is now 1 since it was cow'd when we wrote fbn 0
            Assert.AreEqual(0, childrefcntL1a);

            for (int i = 0; i < pwip_1.L0_DBNS.Length; i++)
            {
                long dbn = pwip_1.L0_DBNS[i];
                int refcnt = 0, childrefcnt = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn, ref refcnt, ref childrefcnt);

                if (i == 0)
                {
                    Assert.AreEqual(1, refcnt);
                }
                else
                {
                    Assert.AreEqual(2, refcnt);
                }
                Assert.AreEqual(0, childrefcnt); //Should be 0 for L0
            }

            rfcore.redfs_discard_wip(myWIP);
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();
            CleanupTestContainer(containerName);
        }


        /*
        * We proceed to a slightly more complex test.
        * Create a wip which has L1s, i.e greater than 8MB file
        * Do multiple clones of the file, verify the blocks and refcounts
        * Overwrite one file and verify COW is working as expected.
        */

            [TestMethod]
        public void IntroTest_4()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];

            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;

            int dataSize = 1024 * 1024 * 9; //9MB

            byte[] buffer_in = new byte[dataSize];
            byte[] buffer_out = new byte[dataSize];
            byte[] buffer_raw_read = new byte[dataSize];

            Random r = new Random();
            r.NextBytes(buffer_in);
            r.NextBytes(buffer_out); //just scramble it, it should be overritten with buffer_in's data when we read back.

            int numClones = 10;

            int newInodeNum = rfcore.NEXT_INODE_NUMBER(rfsid);

            RedFS_Inode myWIP = new RedFS_Inode(WIP_TYPE.REGULAR_FILE, newInodeNum, 0);


            /*
             * Use the wip to write out some data to disk. You could write out any arbitrary data encoded to byte array. 
             */
            rfcore.redfs_write(myWIP, 0, buffer_in, 0, dataSize, WRITE_TYPE.OVERWRITE_IN_PLACE);

            rfcore.sync(myWIP);
            rfcore.flush_cache(myWIP, false);

            /*
             * Wip being cloned must nnot be dirty and should have no incore buffers
            */
            RedFS_Inode[] clones = new RedFS_Inode[numClones];

            for (int cloneid = 0; cloneid < numClones; cloneid++)
            {
                clones[cloneid] = rfcore.redfs_clone_wip(myWIP);
                rfcore.flush_cache(myWIP, false);
            }

            PrintableWIP pwip_0 = rfcore.redfs_list_tree(clones[0], Array.Empty<long>(), Array.Empty<int>()); //One of the clones
            PrintableWIP pwip_1 = rfcore.redfs_list_tree(clones[1], Array.Empty<long>(), Array.Empty<int>());

            Assert.AreEqual(pwip_0.wipIdx.Length, 2); //2 L1 blocks
            Assert.AreEqual(pwip_0.L0_DBNS.Length, 1152);
            Assert.AreEqual(pwip_0.L1_DBNS, null);//coz all L1 dbns are stored in wip itself

            Assert.AreEqual(pwip_1.wipIdx.Length, 2); //2 L1 blocks
            Assert.AreEqual(pwip_1.L0_DBNS.Length, 1152);
            Assert.AreEqual(pwip_1.L1_DBNS, null);//coz all L1 dbns are stored in wip itself

            for (int l0dbn = 0; l0dbn < 1152; l0dbn++)
            {
                Assert.AreEqual(pwip_0.L0_DBNS[l0dbn], pwip_1.L0_DBNS[l0dbn]);

                //Also do raw read to verify that blocks are being read correctly.
                rfcore.redfs_do_raw_read_block(pwip_0.L0_DBNS[l0dbn], buffer_raw_read, l0dbn * OPS.FS_BLOCK_SIZE);
            }

            /*
             * verify we can read data too
             */
            r.NextBytes(buffer_out); //scrable

            //Just make two reads,, just for fun! with two different clones
            rfcore.redfs_read(clones[2], 0, buffer_out, 0, buffer_out.Length / 2);
            rfcore.redfs_read(clones[3], buffer_out.Length / 2, buffer_out, buffer_out.Length / 2, buffer_out.Length / 2);

            int mismatchCount = 0;
            int firstMismatch = 0;
            for (int i = 0; i < dataSize; i++)
            {
                bool hasMismatch = (buffer_in[i] != buffer_out[i]);
                if (hasMismatch)
                {
                    mismatchCount++;
                    if (firstMismatch == 0)
                    {
                        firstMismatch = i;
                    }
                }
                Assert.AreEqual(buffer_in[i], buffer_out[i]);
            }
            Console.WriteLine("has mismatch, count = " + mismatchCount);
            /*
             * Now lets check refcounts of these blocks
             */

            for (int i = 0; i < pwip_1.wipIdx.Length; i++)
            {
                long dbn = pwip_1.wipIdx[i];
                int refcnt = 0, childrefcnt = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn, ref refcnt, ref childrefcnt);

                Assert.AreEqual(11, refcnt);
                Assert.AreEqual(0, childrefcnt);
            }

            /*
             * Write some junk into clones[9] and verify its gone in correctly.
             * also check ref counts of other clones
             */
            r.NextBytes(buffer_out); //scrable
            rfcore.redfs_write(clones[9], 0, buffer_out, 0, dataSize, WRITE_TYPE.OVERWRITE_IN_PLACE);

            rfcore.redfs_read(clones[9], 0, buffer_raw_read, 0, buffer_raw_read.Length);

            for (int i = 0; i < dataSize; i++)
            {
                Assert.AreEqual(buffer_out[i], buffer_raw_read[i]);
            }

            PrintableWIP pwip_2 = rfcore.redfs_list_tree(clones[9], Array.Empty<long>(), Array.Empty<int>());

            for (int i = 0; i < pwip_2.wipIdx.Length; i++)
            {
                long dbn = pwip_2.wipIdx[i];

                long dbn_prev = pwip_1.wipIdx[i]; //from the previous clones

                int refcnt = 0, childrefcnt = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn, ref refcnt, ref childrefcnt);

                Assert.AreEqual((i==0) ? 1 : 11, refcnt);
                Assert.AreEqual(0, childrefcnt);

                rfcore.redfsBlockAllocator.GetRefcounts(dbn_prev, ref refcnt, ref childrefcnt);

                Assert.AreEqual((i == 0)? 10 : 11, refcnt);
                Assert.AreEqual(0, childrefcnt);
            }

            //Now since L1's are touched, the refcounts of the child would have been modified as well
            for (int i = 0; i < pwip_2.L0_DBNS.Length; i++)
            {
                long dbn = pwip_2.L0_DBNS[i];
                long dbn_prev = pwip_1.L0_DBNS[i];

                int refcnt = 0, childrefcnt = 0;
                rfcore.redfsBlockAllocator.GetRefcounts(dbn, ref refcnt, ref childrefcnt);

                Assert.AreEqual(1, refcnt);
                Assert.AreEqual(0, childrefcnt);

                rfcore.redfsBlockAllocator.GetRefcounts(dbn_prev, ref refcnt, ref childrefcnt);
                Assert.AreEqual(10, refcnt);
                Assert.AreEqual(0, childrefcnt);
            }
                

            rfcore.redfs_discard_wip(myWIP);

            for (int cloneid = 0; cloneid < numClones; cloneid++)
            {
                rfcore.sync(clones[cloneid]);
                rfcore.flush_cache(clones[cloneid], false);
                rfcore.redfs_discard_wip(clones[cloneid]);
            }
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();
            CleanupTestContainer(containerName);
        }
    }
}
