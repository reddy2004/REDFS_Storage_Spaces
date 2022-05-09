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
    public class IntroTestREDFSTree
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
            co1.containerName = "IntroTestTreeCreation_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\IntroTestTreeCreation_" + id1;

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
         * In the following tests, we test the REDFSTree class. Now, REDFSTree is a typical dirTree type filesystem
         * that is most commonly used on computers. The user manages the files in a set of directectories that look
         * like a tree (computer science tree). Each node of the tree, be it dir or file is an inode that is managed
         * by REDFSCore. To access the tree itself, we need to know the 'root' dir of the tree and an inode file to
         * manage the set of dir/files located inside the 'root'. So we actually store the inodefile in the FSID 
         * (file system id) block and designate the inode number 2 to be the root dir. All user created files and
         * directories will start from inode 64
         */ 
        [TestMethod]
        public void IntroTest_5()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            /*
             * Lets get a reference to our objects of interest. Previously we have already seen how to create wips
             * and store them. A wip can be used to write out any arbitrary byte array at any offset just like a typical file
             * To track these files, we need an inodeFile and a pointer to keep track of the inode file.
             * 
             * InodeFile is wip with no inodenumber . All user created files/dir (a.ka. inodes) will
             * start from ino=64. So to access a filesystem, we access the inode file, the inode file is a flat file that consistss
             * of the root metadata of all the inodes in that filesystem. At offset 2 of the inode file, we have to root dir, and ino=64 onwards
             * is user created files. When creating new volume from ZeroVolume, the system automatically creates a FS in the new FSID,
             * and creates and inode file (header stored in fsid block) and a root dir inode at offset=2 in the inode file itself.
             */ 
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFSTree rftree = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1];
            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;

            rftree.CreateDirectory("\\dir1");
            rftree.CreateDirectory("\\dir2");

            for (int i=0;i<100;i++)
            {
                string newdir = "d" + i + "";
                rftree.CreateDirectory("\\dir1\\" + newdir);
                rftree.CreateDirectory("\\dir2\\" + newdir);
            }

            Assert.IsTrue(rftree.getNumInodesInTree() == (1 + 2 +200));
            Thread.Sleep(50000);

            //Now all of them are cleared out of memory, so the global hashmap inodes[] will have only
            //1 entry left corresponding to the rootDir. rootDirs of all fsids are alwasy in memory.
            rftree.SyncTree();
            Assert.AreEqual(rftree.getNumInodesInTree(), 3);
            Assert.IsTrue(rftree.GetInode("\\").isInodeSkeleton == true);

            byte[] buffer = new byte[OPS.FS_BLOCK_SIZE];
            int bytesWritten = 0;
            rftree.CreateFile("\\dir1\\d2\\tempfile.dat");

            Random r = new Random();
            r.NextBytes(buffer);

            rftree.WriteFile("\\dir1\\d2\\tempfile.dat", buffer, out bytesWritten, 128);
            Assert.AreEqual(rftree.getNumInodesInTree(), 104);

            Thread.Sleep(50000);
            rftree.SyncTree();
            

            //rfcore.redfs_discard_wip(dir1WIP);
            //rfcore.redfs_discard_wip(dir2WIP);

            Assert.AreEqual(bytesWritten, OPS.FS_BLOCK_SIZE);
            Assert.AreEqual(rftree.getNumInodesInTree(), 5);

            /* 
             * Notice that due to the Thread.sleep() for 50 seconds, all the inodes inmemory are written back to disk.
             * Lets try to read the file we just created. When we read file, only the dirs in its path and children are loaded
             * into memory. In our case tempfile is already in memory and directories dont get dirty due to a write.
             */
            byte[] buffer2 = new byte[OPS.FS_BLOCK_SIZE];
            int bytesRead = 0;
            rftree.ReadFile("\\dir1\\d2\\tempfile.dat", buffer2, out bytesRead, 128);

            //Compare we read what we just wrote.
            for (int i=0;i<buffer.Length;i++)
            {
                Assert.AreEqual(buffer[i], buffer2[i]);
            }
            Assert.AreEqual(rftree.getNumInodesInTree(), 5);
            REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1].FlushCacheL0s();
            CleanupTestContainer(containerName);
        }

        /*
         * In the previous test, we saw how to create directories and files and write and read from a file.
         * In this test, we will do other file system operations such as rename, move, delete of both files
         * and directories.
         */
        [TestMethod]
        public void IntroTest_6()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            CreateTestContainer(containerName);

            CreateCloneOfZeroVolume();

            /*
             * Lets get a reference to our objects of interest. Previously we have already seen how to create wips
             * and store them. A wip can be used to write out any arbitrary byte array at any offset just like a typical file
             * To track these files, we need an inodeFile and a pointer to keep track of the inode file.
             * 
             * InodeFile is wip with no inodenumber . All user created files/dir (a.ka. inodes) will
             * start from ino=64. So to access a filesystem, we access the inode file, the inode file is a flat file that consistss
             * of the root metadata of all the inodes in that filesystem. At offset 2 of the inode file, we have to root dir, and ino=64 onwards
             * is user created files. When creating new volume from ZeroVolume, the system automatically creates a FS in the new FSID,
             * and creates and inode file (header stored in fsid block) and a root dir inode at offset=2 in the inode file itself.
             */
            RedFS_FSID rfsid = REDFS.redfsContainer.ifsd_mux.FSIDList[1];
            REDFSTree rftree = REDFS.redfsContainer.ifsd_mux.RedfsVolumeTrees[1];
            REDFSCore rfcore = REDFS.redfsContainer.ifsd_mux.redfsCore;

            rftree.CreateDirectory("\\dir1");
            rftree.CreateDirectory("\\dir2");
            rftree.CreateDirectory("\\dir2\\dir2A");
            rftree.CreateDirectory("\\dir2\\dir2A\\dir2Ai");

            rftree.SyncTree();
            Assert.AreEqual(rftree.getNumInodesInTree(), 5);

            rftree.MoveInode(rfsid, "\\dir2\\dir2A\\dir2Ai", "\\dir1\\dir2Aj", false, true);

            CleanupTestContainer(containerName);
        }
    }
}
