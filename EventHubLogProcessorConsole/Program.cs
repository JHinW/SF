using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventHubLogProcessor;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Serilog;
using Serilog.Sinks.Elasticsearch;

namespace EventHubLogProcessorConsole
{
	class Program
	{
		static void Main(string[] args)
		{
			var eventHubPath = ConfigurationManager.AppSettings["EventHubPath"];
			var eventHubConnectionString = ConfigurationManager.AppSettings["EventHubConnectionString"];
			var eventProcessorStorageConnectionString = ConfigurationManager.AppSettings["EventProcessorStorageConnectionString"];
			var eventProcessorLeaseContainerName = ConfigurationManager.AppSettings["EventProcessorLeaseContainerName"];
			var consumerGroupName = ConfigurationManager.AppSettings["EventProcessorConsumerGroupName"];
			var environment = ConfigurationManager.AppSettings["Environment"];
			// kusto specfic

			var kustoConsumerGroupName = ConfigurationManager.AppSettings["KustoConsumerGroupName"];
			var kustoTelemetryServiceEndpoint = ConfigurationManager.AppSettings["KustoTelemetryServiceEndpoint"];
			var kustoInstrumentationKey = ConfigurationManager.AppSettings["KustoInstrumentationKey"];
			var kustoStorageAccountKeys = ConfigurationManager.AppSettings["KustoStorageAccountKeys"];
			var kustoBaseContainerName = ConfigurationManager.AppSettings["KustoBaseContainerName"];
			var kustoBlobMemoryBufferSizeInBytes = long.Parse(ConfigurationManager.AppSettings["KustoBlobMemoryBufferSizeInBytes"]);

			Dictionary<string, Guid> schemaInfo = new Dictionary<string, Guid>();
			schemaInfo.Add("Log", Guid.Parse(ConfigurationManager.AppSettings["KustoLogSchemaId"]));
			schemaInfo.Add("Interactions", Guid.Parse(ConfigurationManager.AppSettings["KustoInteractionsSchemaId"]));

			var host = new EventProcessorHost(
				hostName: Guid.NewGuid().ToString(),
				eventHubPath: eventHubPath,
				consumerGroupName: consumerGroupName,
				eventHubConnectionString: eventHubConnectionString,
				storageConnectionString: eventProcessorStorageConnectionString,
				leaseContainerName: eventProcessorLeaseContainerName);

			var kustoHost = new EventProcessorHost(
				hostName: Guid.NewGuid().ToString(),
				eventHubPath: eventHubPath,
				consumerGroupName: kustoConsumerGroupName,
				eventHubConnectionString: eventHubConnectionString,
				storageConnectionString: eventProcessorStorageConnectionString,
				leaseContainerName: eventProcessorLeaseContainerName);

			var options = new EventProcessorOptions()
			{
				InitialOffsetProvider = partitionId => new DateTime(2016, 12, 7, 11, 0, 0, DateTimeKind.Local),
				InvokeProcessorAfterReceiveTimeout = false,
				PrefetchCount = 5000,
				MaxBatchSize = 500,
				ReceiveTimeOut = TimeSpan.FromSeconds(60),
			};

			var kustoOptions = new EventProcessorOptions()
			{
				InitialOffsetProvider = partitionId => new DateTime(2016, 12, 7, 11, 0, 0, DateTimeKind.Local),
				InvokeProcessorAfterReceiveTimeout = false,
				PrefetchCount = 50000,
				MaxBatchSize = 10000,
				ReceiveTimeOut = TimeSpan.FromSeconds(120),
			};

			Log.Logger = new LoggerConfiguration()
				.WriteTo.Console()
				.Enrich.WithProperty("Environment", environment)
				.Enrich.WithProperty("MachineName", Environment.MachineName)
				.CreateLogger();
			var listener = new SerilogTraceListener.SerilogTraceListener(Log.Logger);
			Trace.Listeners.Add(listener);

			var esUri = new Uri(ConfigurationManager.AppSettings["ElasticSearchUri"]);
			var esUserName = ConfigurationManager.AppSettings["ElasticSearchUserName"];
			var esPassword = ConfigurationManager.AppSettings["ElasticSearchPassword"];

			var factory = new EsSinkEventProcessorFactory(esUri, esUserName, esPassword, sendProcessingStats: false);
			var kustoFactory = new KustoSinkEventProcessorFactory(kustoStorageAccountKeys,
				kustoTelemetryServiceEndpoint, kustoInstrumentationKey, false, schemaInfo, kustoBaseContainerName, kustoBlobMemoryBufferSizeInBytes);
			Log.Information("EventHubLogProcessorWorker has been started");

			host.RegisterEventProcessorFactoryAsync(factory, options).Wait();
			kustoHost.RegisterEventProcessorFactoryAsync(kustoFactory, kustoOptions).Wait();

			Console.ReadLine();
			host.UnregisterEventProcessorAsync().Wait();
			kustoHost.UnregisterEventProcessorAsync().Wait();
		}
	}
}
