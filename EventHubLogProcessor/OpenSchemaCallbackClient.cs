using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json.Linq;
using Serilog;

namespace EventHubLogProcessor
{
    public class OpenSchemaCallback
    {
        protected static readonly Lazy<HttpClient> _httpclient = new Lazy<HttpClient>(() =>
        {
            HttpClient httpclient = new HttpClient();
            httpclient.DefaultRequestHeaders.Clear();
            return httpclient;
        });

        protected static HttpClient httpClient
        {
            get { return _httpclient.Value; }
        }

        public static async Task PostCallback(
            CloudBlockBlob blob,
            Uri endpoint,
            string schemaName,
            string iKey)
        {
            try
            {
                var sasPolicy = new SharedAccessBlobPolicy();
                sasPolicy.SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24);
                sasPolicy.Permissions = SharedAccessBlobPermissions.Read;
                var sasToken = blob.GetSharedAccessSignature(sasPolicy);
                var sasUri = blob.Uri + sasToken;

                var payload = new JObject(
                    new JProperty("data",
                        new JObject(
                            new JProperty("baseType", "OpenSchemaData"),
                            new JProperty("baseData", new JObject(
                                new JProperty("ver", "2"),
                                new JProperty("blobSasUri", sasUri),
                                new JProperty("sourceName", schemaName),
                                new JProperty("sourceVersion", "1.0")
                                )
                            )
                        )
                    ),
                    new JProperty("ver", "1"),
                    new JProperty("name", "Microsoft.ApplicationInsights.OpenSchema"),
                    new JProperty("time", DateTime.UtcNow),
                    new JProperty("iKey", iKey)
                );

				var sw = Stopwatch.StartNew();
                HttpResponseMessage rsp = await httpClient
                    .PostAsync(endpoint, new StringContent(payload.ToString()))
                    .ConfigureAwait(false);

                // log to serilog pipeline for diagnostoc purposes
                // TODO: come up with long term solution for logging info about logging pipeline
                Log.Verbose("OpenSchemaCallback publish result {StatusCode} took {ElapsedTimeInMs}", rsp.StatusCode, sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                // TODO - handle exceptions and errors
               Log.Error(ex, "OpenSchemaCallback failed to publish callback with error {ExceptionMessage}", ex.Message);
            }
        }
    }
}
