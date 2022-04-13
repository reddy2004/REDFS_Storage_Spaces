using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestRedBuffers
    {
        string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

        [TestMethod]
        public void TestComparator()
        {
            Assert.IsTrue(OPS.FS_BLOCK_SIZE == 8192);
            Assert.IsTrue(OPS.FS_PTR_SIZE == 8);
            Random rand = new Random();

            Red_Buffer bufL0 = new RedBufL0(0);
            Red_Buffer bufL1 = new RedBufL0(1);
            Red_Buffer bufL2 = new RedBufL0(2);

            rand.NextBytes(bufL0.buf_to_data());
            rand.NextBytes(bufL1.buf_to_data());
            rand.NextBytes(bufL2.buf_to_data());

            Red_Buffer bufL0_t = REDFS_BUFFER_ENCAPSULATED.DeepCopy(bufL0);
            Red_Buffer bufL1_t = REDFS_BUFFER_ENCAPSULATED.DeepCopy(bufL1);
            Red_Buffer bufL2_t = REDFS_BUFFER_ENCAPSULATED.DeepCopy(bufL2);

            Assert.IsTrue(bufL0.Equals(bufL0_t));
            Assert.IsTrue(bufL0.Equals(bufL0_t));
            Assert.IsTrue(bufL0.Equals(bufL0_t));

            Assert.IsFalse(bufL0.Equals(bufL1_t));
            Assert.IsFalse(bufL0.Equals(bufL2_t));
            Assert.IsFalse(bufL1.Equals(bufL2_t));
        }

        /*
         * Given a FBN, we create 1, 2 or 3 Red_buffers, populate them as if they are already valid and return them
         * This 'sub-tree' from wip-> ... -> LO can be tested independently without creating an actual tree or init'ing
         * a new container
         */ 
        private Red_Buffer[] CreateArtificialTreeFromL0FBN(long filesize, int fbn)
        {
            int levels = OPS.FSIZETOILEVEL(filesize);
            Random r = new Random();

            if (levels == 0)
            {
                RedBufL0 rb0 = new RedBufL0(fbn);
                rb0.m_dbn = r.Next(0, Int32.MaxValue);

                Red_Buffer[] returnValue = new Red_Buffer[1];
                returnValue[0] = rb0;
                return returnValue;
            }
            else if (levels == 1)
            {
                RedBufL0 rb0 = new RedBufL0(fbn);
                rb0.m_dbn = r.Next(0, Int32.MaxValue);

                RedBufL1 rb1 = new RedBufL1(OPS.SomeFBNToStartFBN(1, fbn));
                rb1.m_dbn = r.Next(0, Int32.MaxValue);

                int L1idx = rb0.myidx_in_myparent();
                rb1.set_child_dbn(L1idx, rb0.m_dbn);

                Red_Buffer[] returnValue = new Red_Buffer[2];
                returnValue[0] = rb0;
                returnValue[1] = rb1;
                return returnValue;
            }
            else if (levels == 2)
            {
                RedBufL0 rb0 = new RedBufL0(fbn);
                rb0.m_dbn = r.Next(0, Int32.MaxValue);

                RedBufL1 rb1 = new RedBufL1(OPS.SomeFBNToStartFBN(1, fbn));
                rb1.m_dbn = r.Next(0, Int32.MaxValue);

                int L1idx = rb0.myidx_in_myparent();
                rb1.set_child_dbn(L1idx, rb0.m_dbn);

                RedBufL2 rb2 = new RedBufL2(OPS.SomeFBNToStartFBN(2, fbn));
                int L2idx = rb1.myidx_in_myparent();
                rb2.set_child_dbn(L2idx, rb1.m_dbn);

                Red_Buffer[] returnValue = new Red_Buffer[3];
                returnValue[0] = rb0;
                returnValue[1] = rb1;
                returnValue[2] = rb2;
                return returnValue;
            }
            else
            {
                throw new SystemException("File fbn is too large and is not supported");
            }
        }

        [TestMethod]
        public void TestWIPTreeL1()
        {
            Random rand = new Random();

            long testFileSize = 76543210;
            Red_Buffer[]  rbs = CreateArtificialTreeFromL0FBN(testFileSize, 1000);
            Assert.AreEqual(OPS.FSIZETOILEVEL(testFileSize), 1);

            Assert.AreEqual(rbs[0].get_start_fbn(), 1000);
            Assert.AreEqual(rbs[1].get_start_fbn(), OPS.OffsetToStartFBN(1, 1000));
        }

        [TestMethod]
        public void TestWIPTreeL2()
        {
            Random rand = new Random();

            long testFileSize = 876543210;
            int fbn = 12345;

            Red_Buffer[] rbs = CreateArtificialTreeFromL0FBN(testFileSize, fbn);
            Assert.AreEqual(OPS.FSIZETOILEVEL(testFileSize), 2);

            Assert.AreEqual(rbs[0].get_start_fbn(), fbn);
            Assert.AreEqual(rbs[1].get_start_fbn(), OPS.SomeFBNToStartFBN(1, fbn));
            Assert.AreEqual(rbs[2].get_start_fbn(), OPS.SomeFBNToStartFBN(2, fbn));

            //verify with manually computed values
            Assert.AreEqual(rbs[1].get_start_fbn(), 1024 * (fbn /1024));
            Assert.AreEqual(rbs[2].get_start_fbn(), 1024 * 1024 * (fbn / (1024 * 1024)));
        }
    }
}
