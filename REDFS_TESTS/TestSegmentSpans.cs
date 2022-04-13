using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestSegmentSpans
    {
        [TestMethod]
        public void TestSimpleSpanAtZero()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[1];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 0);
                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 0, dataDefault, null);

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(true, dss.isSegmentValid);

                Assert.AreEqual(0, dss.GetDBNSpaceSegmentOffset());

                long sdbn, edbn;
                int blocks;

                dss.GetStartAndEndDBNForSpan(out sdbn, out edbn, out blocks);
                Assert.AreEqual(sdbn, OPS.NUM_DBNS_IN_1GB * 0);
                Assert.AreEqual(edbn, OPS.NUM_DBNS_IN_1GB * 1);
                Assert.AreEqual(blocks, OPS.NUM_DBNS_IN_1GB);
            }
        }

        [TestMethod]
        public void TestSimpleSpanAtOffset()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[1];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.Default, 2, 100); /* The 100th GB of file */
                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, OPS.NUM_DBNS_IN_1GB * 9, dataDefault, null); /* Simple span of 1GB, starting at 9th 1GB of DBN space */

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(true, dss.isSegmentValid);

                Assert.AreEqual(9, dss.GetDBNSpaceSegmentOffset());

                long sdbn, edbn;
                int blocks;

                dss.GetStartAndEndDBNForSpan(out sdbn, out edbn, out blocks);
                Assert.AreEqual(sdbn, OPS.NUM_DBNS_IN_1GB * 9);
                Assert.AreEqual(edbn, OPS.NUM_DBNS_IN_1GB * 10);
                Assert.AreEqual(blocks, OPS.NUM_DBNS_IN_1GB);
            }
        }

        [TestMethod]
        public void TestSimpleSpanAtOffsetWithIncorrectConstructor()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[1];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.Default, 2, 100); /* The 100th GB of file */
                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, 262144 * 9, dataDefault, null); /* Simple span of 1GB, starting at 9th 1GB of DBN space */

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(false, dss.isSegmentValid);
            }
        }

        [TestMethod]
        public void TestMirrorSpanAtOffset()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[2];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, 2, 100); /* The 100th GB of file */
                dataDefault[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, 3, 200); /* The 200th GB of file */

                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, OPS.NUM_DBNS_IN_1GB * 6, dataDefault, null); /* Simple span of 1GB, starting at 6th 1GB of DBN space */

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(true, dss.isSegmentValid);

                Assert.AreEqual(6, dss.GetDBNSpaceSegmentOffset());
                Assert.AreEqual(1, dss.GetNumUserDataSegments());
                Assert.AreEqual(0, dss.GetNumParitySegments());

                long sdbn, edbn;
                int blocks;

                dss.GetStartAndEndDBNForSpan(out sdbn, out edbn, out blocks);
                Assert.AreEqual(sdbn, OPS.NUM_DBNS_IN_1GB * 6);
                Assert.AreEqual(edbn, OPS.NUM_DBNS_IN_1GB * 7);
                Assert.AreEqual(blocks, OPS.NUM_DBNS_IN_1GB);
            }
        }

        [TestMethod]
        public void TestMirrorSpanAtOffsetWithIncorrectConstructorArgs()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataMirror = new RAWSegment[2];
                dataMirror[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, 2, 100); /* The 100th GB of file */
                dataMirror[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, 3, 200); /* The 200th GB of file */

                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, 262144 * 6, dataMirror, dataMirror); /* Simple span of 1GB, starting at 6th 1GB of DBN space */

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(false, dss.isSegmentValid);
            }
        }

        [TestMethod]
        public void TestRAID5SpanAtOffset()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[4];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 100); /* The 100th GB of file 2*/
                dataDefault[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 3, 200); /* The 200th GB of file 3*/
                dataDefault[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 4, 200); /* The 200th GB of file 4*/
                dataDefault[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 10, 200); /* The 200th GB of file 10 */

                RAWSegment[] parity = new RAWSegment[1];
                parity[0] = new RAWSegment(SEGMENT_TYPES.RAID5Parity, 11, 22); /* The 22th GB of file 11 */

                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.RAID5, OPS.NUM_DBNS_IN_1GB * 6, dataDefault, parity); /* Simple span of 1GB, starting at 6th 1GB of DBN space */

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(true, dss.isSegmentValid);

                Assert.AreEqual(6, dss.GetDBNSpaceSegmentOffset());
                Assert.AreEqual(4, dss.GetNumUserDataSegments());
                Assert.AreEqual(1, dss.GetNumParitySegments());

                long sdbn, edbn;
                int blocks;

                dss.GetStartAndEndDBNForSpan(out sdbn, out edbn, out blocks);
                Assert.AreEqual(sdbn, OPS.NUM_DBNS_IN_1GB * 6);
                Assert.AreEqual(edbn, OPS.NUM_DBNS_IN_1GB * 10);
                Assert.AreEqual(blocks, 4 * OPS.NUM_DBNS_IN_1GB);


                int chunk_id;
                long actual_offset_to_read;
                dss.FindReadInformationForDBN(OPS.NUM_DBNS_IN_1GB * 6, out chunk_id, out actual_offset_to_read, false);
                Assert.AreEqual(chunk_id, 2);
                Assert.AreEqual((long)100 * OPS.NUM_DBNS_IN_1GB * OPS.FS_BLOCK_SIZE, actual_offset_to_read); /* This segment at 100GB offset in chunk */

                dss.FindReadInformationForDBN(OPS.NUM_DBNS_IN_1GB * 6+1, out chunk_id, out actual_offset_to_read, false);
                Assert.AreEqual(chunk_id, 3);
                Assert.AreEqual((long)200 * OPS.NUM_DBNS_IN_1GB * OPS.FS_BLOCK_SIZE, actual_offset_to_read); /* This segment at 200GB offset in chunk */

                dss.FindReadInformationForDBN(OPS.NUM_DBNS_IN_1GB * 6 + 6, out chunk_id, out actual_offset_to_read, false);
                Assert.AreEqual(chunk_id, 4);
                Assert.AreEqual((long)200 * OPS.NUM_DBNS_IN_1GB * OPS.FS_BLOCK_SIZE + OPS.FS_BLOCK_SIZE, actual_offset_to_read); /* This segment at 200GB offset in chunk */
            }
        }

        [TestMethod]
        public void TestRAID5SpanAtOffsetWithIncorrectArgs1()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[4];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 100); /* The 100th GB of file 2*/
                dataDefault[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 3, 200); /* The 200th GB of file 3*/
                dataDefault[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 200); /* The 200th GB of file 4*/
                dataDefault[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 10, 200); /* The 200th GB of file 10 */

                RAWSegment[] parity = new RAWSegment[1];
                parity[0] = new RAWSegment(SEGMENT_TYPES.RAID5Parity, 11, 22); /* The 22th GB of file 11 */

                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 262144 * 6, dataDefault, parity); /* Simple span of 1GB, starting at 6th 1GB of DBN space */

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(false, dss.isSegmentValid);
                Assert.AreEqual("Incorrect Span type or incorrect segments", dss.invalidReason);
            }
        }


        [TestMethod]
        public void TestRAID5SpanAtOffsetWithIncorrectArgs2()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[4];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 100); /* The 100th GB of file 2*/
                dataDefault[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 3, 200); /* The 200th GB of file 3*/
                dataDefault[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 200); /* The 200th GB of file 4*/
                dataDefault[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 10, 200); /* The 200th GB of file 10 */

                RAWSegment[] parity = new RAWSegment[1];
                parity[0] = new RAWSegment(SEGMENT_TYPES.RAID5Parity, 11, 22); /* The 22th GB of file 11 */

                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.RAID5, 262144 * 6, dataDefault, parity); /* Simple span of 1GB, starting at 6th 1GB of DBN space */

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(false, dss.isSegmentValid);
                Assert.AreEqual("Overlapping ChunkIds", dss.invalidReason);
            }
        }

        [TestMethod]
        public void TestSpanMapBufferConversionsMirror()
        {
            RAWSegment[] dataMirror = new RAWSegment[2];
            dataMirror[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, 2, 100); /* The 100th GB of file */
            dataMirror[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, 3, 200); /* The 200th GB of file */

            DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, 262144 * 6, dataMirror, null); /* Simple span of 1GB, starting at 6th 1GB of DBN space */

            byte[] buffer = new byte[8];
            dataMirror[0].GetRAWBytes(buffer, 0);
            dataMirror[1].GetRAWBytes(buffer, 4);

            RAWSegment[] dataMirror2 = new RAWSegment[2];
            dataMirror2[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, buffer, 0);
            dataMirror2[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, buffer, 4);

            DBNSegmentSpan dss2 = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, 262144 * 6, dataMirror2, null);

            Assert.IsTrue(dss1.Equals(dss2));

            byte[] buffer2 = new byte[32];
            dss2.GetRAWBytes(buffer2);

            DBNSegmentSpan dss3 = new DBNSegmentSpan(dss2.start_dbn, buffer2);

            Assert.IsTrue(dss1.Equals(dss3));
        }

        [TestMethod]
        public void TestSpanMapBufferConversionsRAID5()
        {
            RAWSegment[] segData = new RAWSegment[4];
            segData[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 100); /* The 100th GB of file */
            segData[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 3, 200); /* The 200th GB of file */
            segData[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 4, 200); /* The 200th GB of file */
            segData[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 5, 200); /* The 200th GB of file */

            RAWSegment[] segParity = new RAWSegment[1];
            segParity[0] = new RAWSegment(SEGMENT_TYPES.RAID5Parity, 6, 10); /* The 10th GB of file */

            DBNSegmentSpan dss1 = new DBNSegmentSpan(SPAN_TYPE.RAID5, 262144 * 6, segData, segParity); /* Simple span of 1GB, starting at 6th 1GB of DBN space */
            byte[] buffer2 = new byte[32];
            dss1.GetRAWBytes(buffer2);

            DBNSegmentSpan dss2 = new DBNSegmentSpan(dss1.start_dbn, buffer2);

            Assert.IsTrue(dss1.Equals(dss2));
        }


    }
}
