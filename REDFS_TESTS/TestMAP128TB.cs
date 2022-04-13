using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestMAP128TB
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        [TestMethod]
        public void TestCreateMapFile()
        {
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "AllocMap_1_Test" + id1;
            co1.containerPath = docFolder + "\\REDFS\\AllocMap_1_Test" + id1;

            REDFS.AddNewContainer(co1);

            REDFS.MountContainer(true, co1.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);

            Map128TB allocMap = new Map128TB("allocMap");
            allocMap.init();

            if (allocMap.try_alloc_bit(1025))
            {
                Assert.IsTrue(allocMap.USED_BLK_COUNT == 1);
                allocMap.free_bit(1025);
                Assert.IsTrue(allocMap.USED_BLK_COUNT == 0);
            }


            allocMap.shut_down();

            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(co1.containerName);
        }


        [TestMethod]
        public void TestUpdateAndVerifyMapFileWithMultipleAllocs()
        {
            ContainerObject co1 = new ContainerObject();
            Random rand = new Random();
            int id1 = rand.Next();
            co1.containerName = "AllocMap_2_Test" + id1;
            co1.containerPath = docFolder + "\\REDFS\\AllocMap_2_Test" + id1;

            REDFS.AddNewContainer(co1);

            REDFS.MountContainer(true, co1.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);

            Map128TB allocMap = new Map128TB("allocMap");
            allocMap.init();

            int[] mydbns = new int[1024];
            for (int i=0;i<mydbns.Length;i++)
            {
                mydbns[i] = 1024 + rand.Next(OPS.NUM_DBNS_IN_1GB * 1024);
                if (!allocMap.try_alloc_bit(mydbns[i]))
                {
                    Assert.Fail("Coundnt allocate bit for dbn @idx=" + i + " dbn:" + mydbns[i]);
                }
            }
            
            for (int i = 0; i < mydbns.Length; i++)
            {
                if (allocMap.is_block_free(mydbns[i]))
                {
                    Assert.Fail("Block is free while it should have been set dbn @idx=" + i + " dbn:" + mydbns[i]);
                }
            }

            Assert.IsTrue(allocMap.USED_BLK_COUNT == 1024);

            for (int i = 0; i < mydbns.Length; i++)
            {
                allocMap.free_bit(mydbns[i]);
            }

            Assert.IsTrue(allocMap.USED_BLK_COUNT == 0);
            
            allocMap.shut_down();
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(co1.containerName);
        }

        [TestMethod]
        public void TestMap32TBWithDeepCopyAndRead()
        {
            Assert.AreEqual(OPS.NUM_DBNS_IN_1GB, 131072);

            //First set bits then copy the file to other location, read and verify contents are same.
            int percentBitsToSet = 20;
            ContainerObject co1 = new ContainerObject();
            Random rand = new Random();
            int id1 = rand.Next();
            co1.containerName = "AllocMap_2_Test" + id1;
            co1.containerPath = docFolder + "\\REDFS\\AllocMap_2_Test" + id1;

            REDFS.AddNewContainer(co1);

            REDFS.MountContainer(true, co1.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);

            Map128TB allocMap = new Map128TB("allocMap");
            allocMap.init();

            int counter0 = 0;
            for (int dbn = 1024; dbn < OPS.NUM_DBNS_IN_1GB * 1024; dbn++)
            {
                if (rand.Next(100) < percentBitsToSet) {
                    counter0 += allocMap.try_alloc_bit(dbn) ? 1 : 0;
                }
            }
            Console.WriteLine("Total blocks allocate = " + allocMap.USED_BLK_COUNT);
            
            //verify
            int counter = 0;
            
            for (int dbn = 0; dbn < OPS.NUM_DBNS_IN_1GB * 1024; dbn++)
            {
                if (!allocMap.is_block_free(dbn))
                {
                    counter++;
                }
            }
            
            allocMap.shut_down();

            File.Copy(co1.containerPath + "\\allocMap", co1.containerPath + "\\allocMapDuplicate");
            File.Copy(co1.containerPath + "\\allocMap.x", co1.containerPath + "\\allocMapDuplicate.x");

            Map128TB allocOrig = new Map128TB("allocMap");
            Map128TB allocDupl = new Map128TB("allocMapDuplicate");
            allocOrig.init();
            allocDupl.init();

            Assert.IsTrue(allocOrig.Equals(allocDupl));
            Assert.AreEqual(allocOrig.USED_BLK_COUNT, allocDupl.USED_BLK_COUNT);

            allocOrig.shut_down();
            allocDupl.shut_down();
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(co1.containerName);

        }
    }
}
