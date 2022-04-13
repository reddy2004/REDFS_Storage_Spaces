using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestContainerWrapper
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        [TestMethod]
        public void TestMultipleContainerCase()
        {
            int currentKnownContainers = REDFS.availableContainers.Count;

            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "TCW_1_Test_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\TCW_1_Test_" + id1;

            ContainerObject co2 = new ContainerObject();
            int id2 = (new Random()).Next();
            co2.containerName = "TCW_2_Test_" + id2;
            co2.containerPath = docFolder + "\\REDFS\\TCW_2_Test_" + id2;

            ContainerObject co3 = new ContainerObject();
            int id3 = (new Random()).Next();
            co3.containerName = "TCW_3_Test_" + id3;
            co3.containerPath = docFolder + "\\REDFS\\TCW_3_Test_" + id3;


            REDFS.AddNewContainer(co1);
            REDFS.AddNewContainer(co2);
            REDFS.AddNewContainer(co3);

            Assert.IsTrue(REDFS.availableContainers.Count == (3 + currentKnownContainers));

            REDFS.MountContainer(true, co2.containerName);
            Assert.IsNotNull(REDFS.redfsContainer);

            Assert.AreEqual(co2.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co2.containerPath, REDFS.redfsContainer.containerFolderPath);

            REDFS.UnmountContainer();
            Assert.IsNull(REDFS.redfsContainer);
            
            //Try mount and unmount again.

            REDFS.MountContainer(true, co1.containerName);

            Assert.AreEqual(co1.containerName, REDFS.redfsContainer.containerName);
            Assert.AreEqual(co1.containerPath, REDFS.redfsContainer.containerFolderPath);

            REDFS.redfsContainer.CreateRootVolumeForNewContainer();

            Assert.AreEqual(1, REDFS.redfsContainer.volumeManager.volumes.Count);
            REDFS.UnmountContainer();
            REDFS.CleanupTestContainer(co1.containerName);
            REDFS.CleanupTestContainer(co2.containerName);
            REDFS.CleanupTestContainer(co3.containerName);
        }
    }
}
