using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace REDFS_ClusterMode
{
    public enum METRIC_NAME
    {
        FIRST,
        READ_FROM_FILE, //in use
        WRITE_TO_FILE, //in use
        READ_KILOBYTES, //in use
        WRITE_KILOBYTES, //in use
        DOKAN_CALLS,
        LOGICAL_DATA,
        PHYSICAL_DATA,
        DBN_ALLOC_MS_1,
        DBN_ALLOC_MS_2,
        LOADBUF_LATENCY_MS,
        FASTWRITE_LATENCY_MS,
        BLOCKS_ALLOCATED,
        BLOCKS_FREED,
        USED_BLOCK_COUNT,
        BLOCK_DRAIN,
        LAST
    }

    class MetricEntry
    {
        public long start_millis;
        public long end_millies;
        public long amount = 0;
    }

    class MetricSlice
    {
        int enumCount = METRIC_NAME.LAST - METRIC_NAME.FIRST;
        long[] CurrentMetricCounters;
        long[] CurrentMetricSamples;
        public int[] AverageValues;
        public long[] CumulativeValues;

        public MetricSlice(MetricSlice prevSlice)
        {
            CurrentMetricCounters = new long[enumCount];
            CurrentMetricSamples = new long[enumCount];
            AverageValues = new int[enumCount];
            CumulativeValues = new long[enumCount];


            for (var i = 0; i < enumCount; i++)
            {
                CurrentMetricCounters[i] = 0;
                CurrentMetricSamples[i] = 0;
            }

            if (prevSlice != null)
            {
                for (var i = 0; i < enumCount; i++)
                {
                    if ((i == (int)METRIC_NAME.BLOCKS_ALLOCATED) || (i == (int)METRIC_NAME.BLOCKS_FREED) ||
                            (i == (int)METRIC_NAME.READ_KILOBYTES) || (i == (int)METRIC_NAME.WRITE_KILOBYTES) ||
                            i == (int)(METRIC_NAME.USED_BLOCK_COUNT) || i == (int)(METRIC_NAME.BLOCK_DRAIN))
                    {
                        CumulativeValues[i] = prevSlice.CumulativeValues[i];
                    }
                }
            }
        }

        public void InsertEntry(METRIC_NAME type, long ticks)
        {
            CurrentMetricCounters[((int)type)] += ticks;
            CurrentMetricSamples[((int)type)]++;
            //Console.WriteLine(CurrentMetricCounters[((int)type)] + " : " + CurrentMetricSamples[((int)type)]);
        }

        public void CompleteSlice()
        {
            //compute the averages and keep it ready.
            for (var i = 0; i < enumCount; i++)
            {
                if ((i == (int)METRIC_NAME.READ_KILOBYTES) || (i == (int)METRIC_NAME.WRITE_KILOBYTES))
                {
                    AverageValues[i] = (int)CurrentMetricCounters[i] / (1024);
                    CurrentMetricCounters[i] += (int)CurrentMetricCounters[i] / (1024);
                }
                else if (i == (int)METRIC_NAME.DOKAN_CALLS || i == (int)(METRIC_NAME.BLOCKS_ALLOCATED) || i == (int)(METRIC_NAME.BLOCKS_FREED))
                {
                    AverageValues[i] = (int)CurrentMetricCounters[i];
                    CumulativeValues[i] += (int)CurrentMetricCounters[i];
                }
                else if (i == (int)(METRIC_NAME.USED_BLOCK_COUNT) || i == (int)(METRIC_NAME.BLOCK_DRAIN))
                {
                    AverageValues[i] = (CurrentMetricSamples[i] > 0) ? (int)(CurrentMetricCounters[i] / CurrentMetricSamples[i]) : 0;
                }
                else if ((i == (int)METRIC_NAME.LOGICAL_DATA) || (i == (int)METRIC_NAME.PHYSICAL_DATA))
                {
                    AverageValues[i] = (CurrentMetricSamples[i] > 0) ? (int)((CurrentMetricCounters[i] / CurrentMetricSamples[i]) / (1024 * 1024)) : 0;
                }
                else if ((i == (int)METRIC_NAME.DBN_ALLOC_MS_1) || (i == (int)METRIC_NAME.DBN_ALLOC_MS_2) || 
                        (i == (int)METRIC_NAME.LOADBUF_LATENCY_MS) || (i == (int)METRIC_NAME.FASTWRITE_LATENCY_MS))
                {
                    AverageValues[i] = (CurrentMetricSamples[i] > 0) ? (int)((CurrentMetricCounters[i] / CurrentMetricSamples[i])) : 0;
                }
                else
                {
                    AverageValues[i] = (CurrentMetricSamples[i] > 0) ? (int)((CurrentMetricCounters[i] / CurrentMetricSamples[i]) / 1000) : 0;
                }
            }
        }

        public int GetEntry(METRIC_NAME type)
        {
            return (CurrentMetricSamples[((int)type)] > 0) ? (int)((CurrentMetricCounters[((int)type)] / CurrentMetricSamples[((int)type)]) / 1000) : 0;
        }

        public string GetDetails(METRIC_NAME type)
        {
            return "NET " + CurrentMetricCounters[((int)type)] + " ticks from " + CurrentMetricSamples[((int)type)] + " samples, equals " + GetEntry(type) + " avg milliseconds";
        }
    }

    public class Metrics
    {
        MetricSlice currentSlice;
        int enumCount = METRIC_NAME.LAST - METRIC_NAME.FIRST;
        Boolean stopThread = false;

        List<MetricSlice> timeGraph = new List<MetricSlice>(120); //120 seconds 

        IDictionary ongoing = new Dictionary<string, MetricEntry>();

        public Metrics()
        {
            currentSlice = new MetricSlice(null);
        }

        public void init()
        {
            Thread tc = new Thread(new ThreadStart(tServiceThread));
            tc.Start();
        }

        public void stop()
        {
            stopThread = true;
        }

        public void tServiceThread()
        {
            Console.WriteLine("Thread starting");
            while (!stopThread)
            {
                Thread.Sleep(5000);
                StartNewMetricSlice();
                if (REDFS.redfsContainer != null)
                {
                    REDFSCoreSideMetrics.m.InsertMetric(METRIC_NAME.USED_BLOCK_COUNT, REDFS.redfsContainer.ifsd_mux.redfsCore.redfsBlockAllocator.allocBitMap32TBFile.USED_BLK_COUNT);
                    REDFSCoreSideMetrics.m.InsertMetric(METRIC_NAME.BLOCK_DRAIN, 0);
                }
            }
            Console.WriteLine("Thread finished!");
        }

        public void StartNewMetricSlice()
        {
            int slices = timeGraph.Count;
            if (slices == 120)
            {
                timeGraph.RemoveAt(0);   
            }
            currentSlice.CompleteSlice();
            timeGraph.Add(currentSlice);

            currentSlice = new MetricSlice(currentSlice);
        }

        private string UniqueId(METRIC_NAME type, int ukey)
        {
            return "K-" + ((int)type) + "-" + ukey.ToString();
        }

        public void InsertMetric(METRIC_NAME type, long amount)
        {
            currentSlice.InsertEntry(type, amount);
        }

        public void StartMetric(METRIC_NAME type, int ukey)
        {
            string id = UniqueId(type, ukey);
            if (ongoing.Contains(id))
            {
                //INCORRECT_START
                ongoing.Remove(id); //or else it will fail again.
            }
            else
            {
                MetricEntry me = new MetricEntry();
                me.start_millis = DateTime.UtcNow.Ticks;
                ongoing.Add(id, me);
            }
        }

        public void StopMetric(METRIC_NAME type, int ukey)
        {
            string id = UniqueId(type, ukey);
            //Console.WriteLine(id);

            if (ongoing.Contains(id))
            {
                MetricEntry me = (MetricEntry)ongoing[id];
                me.end_millies = DateTime.UtcNow.Ticks;
                long total_milis = me.end_millies - me.start_millis;
                ongoing.Remove(id);
                currentSlice.InsertEntry(type, total_milis);
            }
            else
            {
                //INCORRECT_END
            }
        }

        public int GetEntry(METRIC_NAME type)
        {
            return currentSlice.GetEntry(type);
        }

        public string GetDetails(METRIC_NAME type)
        {
            return currentSlice.GetDetails(type);
        }

        /*Array of arrays
         * [] << Each array has 120 seconds worth of data for a particular metric, with avg value at each second.
         * []
         * []
         * ... << number of arrays is the number of known metrics
         */ 
        public string GetJSONDump()
        {
            List<List<int>> allData = new List<List<int>>();

            for (int i=0;i< enumCount;i++)
            {
                List<int> forCurrentMetric = new List<int>();

                foreach (MetricSlice m in timeGraph)
                {
                    if ((i == (int)METRIC_NAME.BLOCKS_ALLOCATED) || (i == (int)METRIC_NAME.BLOCKS_FREED))
                    {
                        forCurrentMetric.Add((int)m.CumulativeValues[i]);
                    }
                    else
                    {
                        forCurrentMetric.Add(m.AverageValues[i]);
                    }
                    
                }
                allData.Add(forCurrentMetric);
            }

            return JsonConvert.SerializeObject(allData, Formatting.None);
        }
    }
    
    /*
     * This static class has a static member of Metrics class. This class will accumulate data for 120 slices of (x,y) graphs points. i.e 120 points on x axis
     * Each of the 120 points are computed at 5 second intervals.
     * Each metric, such as READ_BYTES, is averaged over the 5 second interval and the summary of that 5 second interval becomes a slice in the latest of the 120 points.
     */
    public static class DokanSideMetrics
    {
        public static Metrics m = new Metrics();

        public static void init()
        {
            m.init();
        }

        public static string GetJSONDump()
        {
            return m.GetJSONDump();
        }
    }

    public static class REDFSCoreSideMetrics
    {
        public static Metrics m = new Metrics();

        public static void init()
        {
            m.init();
        }

        public static string GetJSONDump()
        {
            return m.GetJSONDump();
        }
    }


    static class Logger
    {
        public static string logFilePath = @"C:\Users\vikra\Documents\REDFS\Logs\";
        public static Boolean isInited = false;
        public static StreamWriter sw;

        public static void init()
        {
            logFilePath += DateTime.Now.ToString("yyyyMMddHHmmssffff") + ".log";

            sw = new StreamWriter(logFilePath);

            isInited = true;
        }

        public static void LOG(string from, string message)
        {
            if (!isInited) init();
            sw.WriteLine("[" + from + "] " +  message);
            sw.Flush();
        }
    }
}
