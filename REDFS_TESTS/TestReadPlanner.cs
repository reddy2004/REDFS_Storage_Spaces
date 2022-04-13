using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestReadPlanner
    {

        [TestMethod]
        public void TestBasicReadPlanOnDefaultSpan()
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

            DBNSegmentSpanMap spanMap = new DBNSegmentSpanMap(4);
            spanMap.InsertDBNSegmentSpan(dss);
            long[] dbns = { 0, 1, 2 };
            List<ReadPlanElement> rpe = spanMap.PrepareReadPlan(dbns);

            Console.WriteLine(rpe[0].chunkId + "," + rpe[0].readOffset);
            Console.WriteLine(rpe[1].chunkId + "," + rpe[1].readOffset);
            Console.WriteLine(rpe[2].chunkId + "," + rpe[2].readOffset);

            Assert.IsTrue(rpe[0].chunkId == 1 && rpe[1].chunkId == 1 && rpe[2].chunkId == 1);
            Assert.AreEqual(rpe.Count, dbns.Length);
            Assert.AreEqual(rpe[0].readOffset, 0);
            Assert.AreEqual(rpe[1].readOffset, OPS.FS_BLOCK_SIZE);
            Assert.AreEqual(rpe[2].readOffset, 2 * OPS.FS_BLOCK_SIZE);

        }

        [TestMethod]
        public void TestBasicReadPlanOnDefaultSpanAtOffset()
        {
            RAWSegment[] dataDefault = new RAWSegment[1];
            dataDefault[0] = new RAWSegment(SEGMENT_TYPES.Default, 2, 100); /* Segment at chunk2 at 100GB offset */
            DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, OPS.NUM_DBNS_IN_1GB, dataDefault, null); /* Corresponds to start of 2nd GB in DBN space */

            /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
            Assert.AreEqual(true, dss.isSegmentValid);
            Assert.AreEqual(1, dss.GetDBNSpaceSegmentOffset());

            long sdbn, edbn;
            int blocks;

            dss.GetStartAndEndDBNForSpan(out sdbn, out edbn, out blocks);
            Assert.AreEqual(sdbn, OPS.NUM_DBNS_IN_1GB * 1); /* Starts at 2nd GB */
            Assert.AreEqual(edbn, OPS.NUM_DBNS_IN_1GB * 2); /* Ends at 3rd GB since its a simple span of size 1G */
            Assert.AreEqual(blocks, OPS.NUM_DBNS_IN_1GB); /* Simple span */

            DBNSegmentSpanMap spanMap = new DBNSegmentSpanMap(4); /* Lets keep a 4GB DBN space layout */

            spanMap.InsertDBNSegmentSpan(dss);
            long[] dbns = { OPS.NUM_DBNS_IN_1GB + 4, OPS.NUM_DBNS_IN_1GB + 10, OPS.NUM_DBNS_IN_1GB + 21 }; /* Read the 4th, 10th and 21st block in the segment */
            List<ReadPlanElement> rpe = spanMap.PrepareReadPlan(dbns);

            Assert.IsTrue(rpe[0].chunkId == 2 && rpe[1].chunkId == 2 && rpe[2].chunkId == 2);
            Assert.AreEqual(rpe.Count, dbns.Length);

            Assert.AreEqual(rpe[0].readOffset, (long)100 * (1024 * 1024 * 1024) + 4 * OPS.FS_BLOCK_SIZE); /*for dbn[0], Chunk2 offset is at 100GB + 4 blocks */
            Assert.AreEqual(rpe[1].readOffset, (long)100 * (1024 * 1024 * 1024) + 10 * OPS.FS_BLOCK_SIZE); /*for dbn[1], Chunk2 offset is at 100GB + 10 blocks */
            Assert.AreEqual(rpe[2].readOffset, (long)100 * (1024 * 1024 * 1024) + 21 * OPS.FS_BLOCK_SIZE); /*for dbn[2], Chunk2 offset is at 100GB + 21 blocks */

        }

        [TestMethod]
        public void TestBasicReadPlanOnMirrorSpanAtOffset()
        {
            RAWSegment[] dataDefault = new RAWSegment[2];
            dataDefault[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, 2, 100); /* Segment at chunk 2 at 100GB offset */
            dataDefault[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, 12, 100); /* Segment at chunk 12 at 100GB offset */
            DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, 131072, dataDefault, null); /* Corresponds to start of 2nd GB in DBN space */

            /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
            Console.WriteLine(dss.invalidReason);
            Assert.AreEqual(true, dss.isSegmentValid);
            Assert.AreEqual(1, dss.GetDBNSpaceSegmentOffset());
            

            long sdbn, edbn;
            int blocks;

            dss.GetStartAndEndDBNForSpan(out sdbn, out edbn, out blocks);
            Assert.AreEqual(sdbn, OPS.NUM_DBNS_IN_1GB * 1); /* Starts at 2nd GB */
            Assert.AreEqual(edbn, OPS.NUM_DBNS_IN_1GB * 2); /* Ends at 3rd GB since its a simple span of size 1G */
            Assert.AreEqual(blocks, OPS.NUM_DBNS_IN_1GB); /* Simple mirror span of 1GB dbn space */

            DBNSegmentSpanMap spanMap = new DBNSegmentSpanMap(4); /* Lets keep a 4GB DBN space layout */

            spanMap.InsertDBNSegmentSpan(dss);
            long[] dbns = { OPS.NUM_DBNS_IN_1GB + 4, OPS.NUM_DBNS_IN_1GB + 10, OPS.NUM_DBNS_IN_1GB + 21 }; /* Read the 4th, 10th and 21st block in the segment */
            List<ReadPlanElement> rpe = spanMap.PrepareReadPlan(dbns);

            Assert.IsTrue(rpe[0].chunkId == 2 && rpe[1].chunkId == 2 && rpe[2].chunkId == 2); /* We could return 12 or 2 actually : todo */
            Assert.AreEqual(rpe.Count, dbns.Length);

            Assert.AreEqual(rpe[0].readOffset, (long)100 * (1024 * 1024 * 1024) + 4 * OPS.FS_BLOCK_SIZE); /*for dbn[0], Chunk 2 offset is at 100GB + 4 blocks */
            Assert.AreEqual(rpe[1].readOffset, (long)100 * (1024 * 1024 * 1024) + 10 * OPS.FS_BLOCK_SIZE); /*for dbn[1], Chunk 2 offset is at 100GB + 10 blocks */
            Assert.AreEqual(rpe[2].readOffset, (long)100 * (1024 * 1024 * 1024) + 21 * OPS.FS_BLOCK_SIZE); /*for dbn[2], Chunk 2 offset is at 100GB + 21 blocks */
        }

        public void TestBasicReadPlanOnRaidSpanAtOffset()
        {
            RAWSegment[] dataSegment = new RAWSegment[2];
            dataSegment[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 100); /* Segment at chunk 2 at 100GB offset */
            dataSegment[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 12, 200); /* Segment at chunk 12 at 200GB offset */
            dataSegment[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 14, 300); /* Segment at chunk 14 at 300GB offset */
            dataSegment[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 16, 400); /* Segment at chunk 16 at 400GB offset */

            RAWSegment[] parity = new RAWSegment[1];
            parity[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 4, 100); /* Segment at chunk 4 at 100GB offset */

            DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.RAID5, 262144, dataSegment, parity); /* Corresponds to start of 2nd GB in DBN space */

            /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
            Console.WriteLine(dss.invalidReason);
            Assert.AreEqual(true, dss.isSegmentValid);
            Assert.AreEqual(1, dss.GetDBNSpaceSegmentOffset());

            long sdbn, edbn;
            int blocks;

            dss.GetStartAndEndDBNForSpan(out sdbn, out edbn, out blocks);
            Assert.AreEqual(sdbn, 262144 * 1); /* Starts at 2nd GB */
            Assert.AreEqual(edbn, 262144 * 2); /* Ends at 3rd GB since its a simple span of size 1G */
            Assert.AreEqual(blocks, 262144); /* Simple mirror span of 1GB dbn space */

            DBNSegmentSpanMap spanMap = new DBNSegmentSpanMap(8); /* Lets keep a 8GB DBN space layout */

            spanMap.InsertDBNSegmentSpan(dss);
            long[] dbns = { 262144 + 4, 262144 + 5, 262144 + 6, 262144 + 7 }; /* Read the 4th, 5th, 6th and 7th in the segment */
            List<ReadPlanElement> rpe = spanMap.PrepareReadPlan(dbns);

            Assert.IsTrue(rpe[0].chunkId == 2 && rpe[1].chunkId == 12 && rpe[2].chunkId == 14 && rpe[2].chunkId == 16); /* Loops as a round for all chunkids - 2,12,14,16 */
            Assert.AreEqual(rpe.Count, dbns.Length);

            Assert.AreEqual(rpe[0].readOffset, (long)100 * (1024 * 1024 * 1024) + 4 * 4096); /*for dbn[0], Chunk 2 offset is at 100GB + 4 blocks */
            Assert.AreEqual(rpe[1].readOffset, (long)200 * (1024 * 1024 * 1024) + 4 * 4096); /*for dbn[1], Chunk 2 offset is at 200GB + 4 blocks */
            Assert.AreEqual(rpe[2].readOffset, (long)300 * (1024 * 1024 * 1024) + 4 * 4096); /*for dbn[2], Chunk 2 offset is at 300GB + 4 blocks */
            Assert.AreEqual(rpe[2].readOffset, (long)400 * (1024 * 1024 * 1024) + 4 * 4096); /*for dbn[3], Chunk 2 offset is at 400GB + 4 blocks */

            //Lets try the next 4 consequtive dbns
            long[] dbns2 = { 262144 + 8, 262144 + 9, 262144 + 10, 262144 + 11 };

            rpe = spanMap.PrepareReadPlan(dbns2);

            Assert.IsTrue(rpe[0].chunkId == 2 && rpe[1].chunkId == 12 && rpe[2].chunkId == 14 && rpe[2].chunkId == 16); /* Loops as a round for all chunkids - 2,12,14,16 */
            Assert.AreEqual(rpe.Count, dbns.Length);

            Assert.AreEqual(rpe[0].readOffset, (long)100 * (1024 * 1024 * 1024) + 4 * 4096); /*for dbn[0], Chunk 2 offset is at 100GB + 8 blocks */
            Assert.AreEqual(rpe[1].readOffset, (long)200 * (1024 * 1024 * 1024) + 4 * 4096); /*for dbn[1], Chunk 2 offset is at 200GB + 8 blocks */
            Assert.AreEqual(rpe[2].readOffset, (long)300 * (1024 * 1024 * 1024) + 4 * 4096); /*for dbn[2], Chunk 2 offset is at 300GB + 8 blocks */
            Assert.AreEqual(rpe[2].readOffset, (long)400 * (1024 * 1024 * 1024) + 4 * 4096); /*for dbn[3], Chunk 2 offset is at 400GB + 8 blocks */
        }
    }
}
