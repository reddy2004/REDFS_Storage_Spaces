using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using REDFS_ClusterMode;
using Newtonsoft.Json;

namespace REDFS_TESTS
{
    [TestClass]
    public class TestSegmentSpanMap
    {
        [TestMethod]
        public void TestSimpleSpanInMap()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[1];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.Default, 1, 0);
                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.DEFAULT, 0, dataDefault, null);

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(true, dss.isSegmentValid);

                DBNSegmentSpanMap spanMap = new DBNSegmentSpanMap(8); /* 8 GB of data */
                spanMap.InsertDBNSegmentSpan(dss);

                Assert.AreEqual(spanMap.GetDBNSegmentSpan(dss.start_dbn).type, SPAN_TYPE.DEFAULT);
            }
        }

        [TestMethod]
        public void TestMirroredSpanInMap()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[2];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.Mirrored, 1, 0);
                dataDefault[1] = new RAWSegment(SEGMENT_TYPES.Mirrored, 2, 0);

                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.MIRRORED, 0, dataDefault, null);

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(true, dss.isSegmentValid);

                DBNSegmentSpanMap spanMap = new DBNSegmentSpanMap(8); /* 8 GB of data */
                spanMap.InsertDBNSegmentSpan(dss);

                Assert.AreEqual(spanMap.GetDBNSegmentSpan(dss.start_dbn).type, SPAN_TYPE.MIRRORED);
            }
        }

        [TestMethod]
        public void TestRAID5SpanInMap()
        {
            using (var sw = new StringWriter())
            {
                RAWSegment[] dataDefault = new RAWSegment[4];
                dataDefault[0] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 1, 0);
                dataDefault[1] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 2, 0);
                dataDefault[2] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 3, 0);
                dataDefault[3] = new RAWSegment(SEGMENT_TYPES.RAID5Data, 4, 0);

                RAWSegment[] parity = new RAWSegment[1];
                parity[0] = new RAWSegment(SEGMENT_TYPES.RAID5Parity, 5, 0);

                DBNSegmentSpan dss = new DBNSegmentSpan(SPAN_TYPE.RAID5, 0, dataDefault, parity);

                /* Verify that the segment span's start corresponds to the segment offset as seen in DBN Space */
                Assert.AreEqual(true, dss.isSegmentValid);

                DBNSegmentSpanMap spanMap = new DBNSegmentSpanMap(8); /* 8 GB of data */
                spanMap.InsertDBNSegmentSpan(dss);

                Assert.AreEqual(spanMap.GetDBNSegmentSpan(dss.start_dbn).type, SPAN_TYPE.RAID5);
                Assert.AreEqual(spanMap.GetDBNSegmentSpan(dss.start_dbn + 131072).type, SPAN_TYPE.RAID5);
                Assert.AreEqual(spanMap.GetDBNSegmentSpan(dss.start_dbn + 131072 * 2).type, SPAN_TYPE.RAID5);
                Assert.AreEqual(spanMap.GetDBNSegmentSpan(dss.start_dbn + 131072 * 3).type, SPAN_TYPE.RAID5);
            }
        }
    }
}
