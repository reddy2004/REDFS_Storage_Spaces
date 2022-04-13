using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestVolumeManager
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        private void InitNewTestContainer(out string containerName)
        {
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
        public void TestConstructor()
        {
            
            File.Delete(docFolder + "\\REDFS\\Test\\volumes.json");

            VolumeManager vm = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm.CreateRootVolumeForNewContainer();

            Assert.AreEqual(vm.volumes.Count, 1);
        }

        [TestMethod]
        public void TestExistingVolumesLoad()
        {
            File.Delete(docFolder + "\\REDFS\\Test\\volumes.json");

            VolumeManager vm = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm.CreateRootVolumeForNewContainer();

            Assert.AreEqual(vm.volumes.Count, 1);

            VolumeManager vm2 = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm2.LoadVolumeListFromDisk();
            Assert.AreEqual(vm2.volumes.Count, 1);
        }

        [TestMethod]
        public void TestCloneOfRootVolume()
        {
            File.Delete(docFolder + "\\REDFS\\Test\\volumes.json");

            VolumeManager vm = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm.CreateRootVolumeForNewContainer();

            Assert.AreEqual(vm.volumes.Count, 1);

            VolumeManager vm2 = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm2.LoadVolumeListFromDisk();
            Assert.AreEqual(vm2.volumes.Count, 1);

            vm2.CreateZeroVolume("zero1");
            Assert.AreEqual(vm2.volumes.Count, 2);
        }

        [TestMethod]
        public void TestCloneOfRegularVolume()
        {
            File.Delete(docFolder + "\\REDFS\\Test\\volumes.json");

            VolumeManager vm = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm.CreateRootVolumeForNewContainer();

            Assert.AreEqual(vm.volumes.Count, 1);

            VolumeManager vm2 = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm2.LoadVolumeListFromDisk();
            Assert.AreEqual(vm2.volumes.Count, 1);

            vm2.CloneVolumeRaw(0, "cloneOfRoot", "Testing", "#000000");
            Assert.AreEqual(vm2.volumes.Count, 2);
        }

        [TestMethod]
        public void TestBackedCloneOfRegularVolume()
        {
            File.Delete(docFolder + "\\REDFS\\Test\\volumes.json");

            VolumeManager vm = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm.CreateRootVolumeForNewContainer();

            Assert.AreEqual(vm.volumes.Count, 1);

            VolumeManager vm2 = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm2.LoadVolumeListFromDisk();
            Assert.AreEqual(vm2.volumes.Count, 1);

            vm2.CloneVolume(0, "cloneOfRoot");
            Assert.AreEqual(vm2.volumes.Count, 3);
        }

        [TestMethod]
        public void TestSnapshotOfRegularVolume()
        {
            File.Delete(docFolder + "\\REDFS\\Test\\volumes.json");

            VolumeManager vm = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm.CreateRootVolumeForNewContainer();

            Assert.AreEqual(vm.volumes.Count, 1);

            VolumeManager vm2 = new VolumeManager(docFolder + "\\REDFS\\Test");
            vm2.LoadVolumeListFromDisk();
            Assert.AreEqual(vm2.volumes.Count, 1);

            vm2.CreateZeroVolume("zero1");
            Assert.AreEqual(2, vm2.volumes.Count);

            vm2.VolumeSnapshot(1, "snapOfZero1");
            Assert.AreEqual(3, vm2.volumes.Count);
        }
    }
}
