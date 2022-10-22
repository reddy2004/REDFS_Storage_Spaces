using System;
using DokanNet;
using DokanNet.Logging;
using Newtonsoft.Json;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using System.Threading;

namespace REDFS_ClusterMode
{
    public class Program
    {
        public static void StartHTTPServerThread()
        {
            Thread tc = new Thread(new ThreadStart(RunHTTPServer));
            tc.Start();
        }

        public static void RunHTTPServer()
        {
            // Create a Http server and start listening for incoming connections
            System.Net.HttpListener listener = new HttpListener();
            listener.IgnoreWriteExceptions = true;
            listener.Prefixes.Add("http://localhost:8000/");
            listener.Start();
            Console.WriteLine("Listening for connections on {0}", "http://localhost:8000/");

            // Handle requests
            HttpServer hs = new HttpServer(listener);
            Task listenTask = hs.HandleIncomingConnections();

            try
            {
                listenTask.GetAwaiter().GetResult();
            } 
            catch (Exception e)
            {
                Console.WriteLine("Issue with null value. to be fixed");
            }
            // Close the listener
            listener.Close();
        }

        public static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            try
            {
                StartHTTPServerThread();
            }
            catch (DokanException ex)
            {
                Console.WriteLine(@"Error: " + ex.Message);
            }
        }
    }
}
