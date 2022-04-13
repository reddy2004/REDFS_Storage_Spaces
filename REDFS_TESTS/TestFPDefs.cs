using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestFPDefs
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
        [TestMethod]
        public void VerifySetAndGetMethods()
        {
            Random r = new Random();
            fingerprintBACKUP fbkp = new fingerprintBACKUP();
            fbkp.inode = 1;
            fbkp.fbn = 2;
            r.NextBytes(fbkp.fp);

            byte[] buffer = new byte[((Item)fbkp).get_size()];
            ((Item)fbkp).get_bytes(buffer, 0);

            fingerprintBACKUP fbkp_t = new fingerprintBACKUP();
            ((Item)fbkp_t).parse_bytes(buffer, 0);

            Assert.AreEqual(OPS.HashToString(fbkp.fp), OPS.HashToString(fbkp_t.fp));
            Assert.AreEqual(fbkp.inode, fbkp_t.inode);
            Assert.AreEqual(fbkp.fbn, fbkp_t.fbn);
            Assert.IsTrue(fbkp.Equals(fbkp_t));

            fingerprintCLOG fbclog = new fingerprintCLOG(DEDUP_SORT_ORDER.DBN_BASED);
            fbclog.fsid = 1;
            fbclog.inode = 2;
            fbclog.fbn = 3;
            fbclog.dbn = 4;
            fbclog.cnt = 5;
            r.NextBytes(fbclog.fp);
            byte[] buffer2 = new byte[((Item)fbclog).get_size()];
            ((Item)fbclog).get_bytes(buffer2, 0);

            fingerprintCLOG fbclog_t = new fingerprintCLOG(DEDUP_SORT_ORDER.DBN_BASED);
            ((Item)fbclog_t).parse_bytes(buffer2, 0);
            Assert.IsTrue(fbclog.Equals(fbclog_t));



            fingerprintDMSG fbdmsg = new fingerprintDMSG(DEDUP_SORT_ORDER.DBN_BASED);
            fbdmsg.fsid = 1;
            fbdmsg.inode = 2;
            fbdmsg.fbn = 3;
            fbdmsg.sourcedbn = 4;
            fbdmsg.destinationdbn = 5;
            r.NextBytes(fbdmsg.fp);
            byte[] buffer3 = new byte[((Item)fbdmsg).get_size()];
            ((Item)fbdmsg).get_bytes(buffer3, 0);

            fingerprintDMSG fbdmsg_t = new fingerprintDMSG(DEDUP_SORT_ORDER.DBN_BASED);
            ((Item)fbdmsg_t).parse_bytes(buffer3, 0);
            Assert.IsTrue(fbdmsg.Equals(fbdmsg_t));


            fingerprintFPDB fbfpdb = new fingerprintFPDB(DEDUP_SORT_ORDER.DBN_BASED);
            fbfpdb.dbn = 4;
            r.NextBytes(fbfpdb.fp);
            byte[] buffer4 = new byte[((Item)fbfpdb).get_size()];
            ((Item)fbfpdb).get_bytes(buffer4, 0);

            fingerprintFPDB fbfpdb_t = new fingerprintFPDB(DEDUP_SORT_ORDER.DBN_BASED);
            ((Item)fbfpdb_t).parse_bytes(buffer4, 0);
            Assert.IsTrue(fbfpdb.Equals(fbfpdb_t));
        }

        private void InitNewTestContainer(out string containerName)
        {
            ContainerObject co1 = new ContainerObject();
            int id1 = (new Random()).Next();
            co1.containerName = "FPTest_Clog_" + id1;
            co1.containerPath = docFolder + "\\REDFS\\FPTest_Clog_" + id1;

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
         * Randomly generate entries and sort them with sort API and compare the
         * sorted ones manually using the IComparator.
         * We should use containers here.
         */ 
        [TestMethod]
        public void VerifyCompareAndSortForfingerprintCLOG()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            //Do test here
            Directory.CreateDirectory(REDFS.getAbsoluteContainerPath() + "//temp//");
            FileStream fdest = new FileStream(REDFS.getAbsoluteContainerPath() + "//temp//clog_file", FileMode.Create);

            int numClogEntries = 8192;
            fingerprintCLOG fbclog = new fingerprintCLOG(DEDUP_SORT_ORDER.DBN_BASED);
            byte[] buffer = new byte[((Item)fbclog).get_size()];
            Random r = new Random();

            while (numClogEntries-- > 0)
            {
                fbclog.dbn = r.Next();
                r.NextBytes(fbclog.fp);
                ((Item)fbclog).get_bytes(buffer, 0);
                fdest.Write(buffer, 0, buffer.Length);
            }

            Assert.AreEqual(8192 * ((Item)fbclog).get_size(), fdest.Length);
            fdest.Flush();
            fdest.Close();
            
            SortAPI sapi = new SortAPI(REDFS.getAbsoluteContainerPath() + "//temp//clog_file", 
                REDFS.getAbsoluteContainerPath() + "//temp//clog_file_sorted", new fingerprintCLOG(DEDUP_SORT_ORDER.DBN_BASED));

            sapi.do_chunk_sort();
            sapi.do_merge_work();
            sapi.close_streams();

            Assert.IsTrue(SortAPI.VerifyFileIsSorted(REDFS.getAbsoluteContainerPath() + "//temp//clog_file_sorted", new fingerprintCLOG(DEDUP_SORT_ORDER.DBN_BASED)));

            //end of test.
            CleanupTestContainer(containerName);
        }

        [TestMethod]
        public void VerifyCompareAndSortForfingerprintDMSG()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            //Do test here
            Directory.CreateDirectory(REDFS.getAbsoluteContainerPath() + "//temp//");
            FileStream fdest = new FileStream(REDFS.getAbsoluteContainerPath() + "//temp//dmsg_file", FileMode.Create);

            int numClogEntries = 8192;
            fingerprintDMSG fbdmsg = new fingerprintDMSG(DEDUP_SORT_ORDER.FINGERPRINT_BASED); //Lets do fp based this time
            byte[] buffer = new byte[((Item)fbdmsg).get_size()];
            Random r = new Random();

            while (numClogEntries-- > 0)
            {
                fbdmsg.sourcedbn = r.Next();
                r.NextBytes(fbdmsg.fp);
                ((Item)fbdmsg).get_bytes(buffer, 0);
                fdest.Write(buffer, 0, buffer.Length);
            }

            Assert.AreEqual(8192 * ((Item)fbdmsg).get_size(), fdest.Length);
            fdest.Flush();
            fdest.Close();

            SortAPI sapi = new SortAPI(REDFS.getAbsoluteContainerPath() + "//temp//dmsg_file",
                REDFS.getAbsoluteContainerPath() + "//temp//dmsg_file_sorted", new fingerprintDMSG(DEDUP_SORT_ORDER.FINGERPRINT_BASED));

            sapi.do_chunk_sort();
            sapi.do_merge_work();
            sapi.close_streams();

            Assert.IsTrue(SortAPI.VerifyFileIsSorted(REDFS.getAbsoluteContainerPath() + "//temp//dmsg_file_sorted", new fingerprintDMSG(DEDUP_SORT_ORDER.FINGERPRINT_BASED)));

            //end of test.
            CleanupTestContainer(containerName);
        }

        [TestMethod]
        public void VerifyCompareAndSortForfingerprintFPDB()
        {
            string containerName;
            InitNewTestContainer(out containerName);

            //Do test here
            Directory.CreateDirectory(REDFS.getAbsoluteContainerPath() + "//temp//");
            FileStream fdest = new FileStream(REDFS.getAbsoluteContainerPath() + "//temp//fpdb_file", FileMode.Create);

            int numClogEntries = 8192;
            fingerprintFPDB fbfpdb = new fingerprintFPDB(DEDUP_SORT_ORDER.DBN_BASED);
            byte[] buffer = new byte[((Item)fbfpdb).get_size()];
            Random r = new Random();

            while (numClogEntries-- > 0)
            {
                fbfpdb.dbn = r.Next();
                r.NextBytes(fbfpdb.fp);
                ((Item)fbfpdb).get_bytes(buffer, 0);
                fdest.Write(buffer, 0, buffer.Length);
            }

            Assert.AreEqual(8192 * ((Item)fbfpdb).get_size(), fdest.Length);
            fdest.Flush();
            fdest.Close();

            SortAPI sapi = new SortAPI(REDFS.getAbsoluteContainerPath() + "//temp//fpdb_file",
                REDFS.getAbsoluteContainerPath() + "//temp//fpdb_file_sorted", new fingerprintDMSG(DEDUP_SORT_ORDER.DBN_BASED));

            sapi.do_chunk_sort();
            sapi.do_merge_work();
            sapi.close_streams();

            Assert.IsTrue(SortAPI.VerifyFileIsSorted(REDFS.getAbsoluteContainerPath() + "//temp//fpdb_file_sorted", new fingerprintDMSG(DEDUP_SORT_ORDER.DBN_BASED)));

            //end of test.
            CleanupTestContainer(containerName);
        }
    }
}
