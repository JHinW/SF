using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using E2ECommon.Logging;
using EventHubLogProcessor;
using Serilog;

namespace EventHubLogProcessor.ProcessorSvc
{
	/// <summary>
	/// The FabricRuntime creates an instance of this class for each service type instance. 
	/// </summary>
	internal sealed class ProcessorSvc : StatelessService
	{
		private Dictionary<string, object> AdditionalLogProperties => new Dictionary<string, object>
		{
			{ "InstanceId", this.Context?.InstanceId },
			{ "PartitionId", this.Context?.PartitionId },
			{ "NodeId", this.Context?.NodeContext?.NodeId },
			{ "NodeName", this.Context?.NodeContext?.NodeName },
		};

		public ProcessorSvc(StatelessServiceContext context)
				: base(context)
		{ }

		/// <summary>
		/// Optional override to create listeners (like tcp, http) for this service instance.
		/// </summary>
		/// <returns>The collection of listeners.</returns>
		protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
		{
			return new ServiceInstanceListener[]
			{
				new ServiceInstanceListener(serviceContext => new OwinCommunicationListener(null, Startup.ConfigureApp, serviceContext))
			};
		}

		protected override async Task RunAsync(CancellationToken cancellationToken)
		{
			try
			{
				Log.Information("ProcessorSvc.RunAsync started. InstanceId={InstanceId}, PartitionId={PartitionId}, NodeId={NodeId}, NodeName={NodeName}", Context?.InstanceId, Context?.PartitionId, Context?.NodeContext?.NodeId, Context?.NodeContext?.NodeName);

				var settings = new AppConfigSettingsProvider();

				// Load input settings.
				var hostName = Guid.NewGuid().ToString();
				var eventHubPath = settings.GetSettingValue("InputEventHubName");
				var eventHubConnectionString = settings.GetSettingValue("InputEventHubConnectionString");
				var eventProcessorStorageConnectionString = settings.GetSettingValue("EventProcessorStorageConnectionString");
				var eventProcessorLeaseContainerName = settings.GetSettingValue("EventProcessorStorageLeaseContainer");
				var consumerGroupName = settings.GetSettingValue("EventProcessorConsumerGroupName");
				var awsConsumerGroupName = settings.GetSettingValue("AwsEventProcessorConsumerGroupName");

				// kusto specfic

				var kustoConsumerGroupName = settings.GetSettingValue("KustoConsumerGroupName");
				var kustoTelemetryServiceEndpoint = settings.GetSettingValue("KustoTelemetryServiceEndpoint");
				var kustoInstrumentationKey = settings.GetSettingValue("KustoInstrumentationKey");
				var kustoStorageAccountKeys = settings.GetSettingValue("KustoStorageAccountKeys");
				var kustoBaseContainerName = settings.GetSettingValue("KustoBaseContainerName");
				var kustoBlobMemoryBufferSizeInBytes = long.Parse(settings.GetSettingValue("KustoBlobMemoryBufferSizeInBytes"));
				var kustoCompressBlobs = bool.Parse(settings.GetSettingValue("KustoCompressBlobs"));

				Dictionary<string, Guid> schemaInfo = new Dictionary<string, Guid>();
				//schemaInfo.Add("Log", Guid.Parse(settings.GetSettingValue("KustoLogSchemaId")));
				//schemaInfo.Add("Interactions", Guid.Parse(settings.GetSettingValue("KustoInteractionsSchemaId")));

				var host = new EventProcessorHost(
					hostName: hostName,
					eventHubPath: eventHubPath,
					consumerGroupName: consumerGroupName,
					eventHubConnectionString: eventHubConnectionString,
					storageConnectionString: eventProcessorStorageConnectionString,
					leaseContainerName: eventProcessorLeaseContainerName);

				var awsHost = new EventProcessorHost(
					hostName: hostName,
					eventHubPath: eventHubPath,
					consumerGroupName: awsConsumerGroupName,
					eventHubConnectionString: eventHubConnectionString,
					storageConnectionString: eventProcessorStorageConnectionString,
					leaseContainerName: eventProcessorLeaseContainerName);

				var kustoHost = new EventProcessorHost(
					hostName: hostName,
					eventHubPath: eventHubPath,
					consumerGroupName: kustoConsumerGroupName,
					eventHubConnectionString: eventHubConnectionString,
					storageConnectionString: eventProcessorStorageConnectionString,
					leaseContainerName: eventProcessorLeaseContainerName);

				var options = new EventProcessorOptions()
				{
					InitialOffsetProvider = partitionId => DateTime.UtcNow,
					InvokeProcessorAfterReceiveTimeout = false,
					PrefetchCount = 5000,
					MaxBatchSize = 500,
					ReceiveTimeOut = TimeSpan.FromSeconds(60),
				};

				var kustoOptions = new EventProcessorOptions()
				{
					// TODO: reset back to utc now
					// for now set time to 3 hours ago to see how long it takes to catch up

					InitialOffsetProvider = partitionId => DateTime.UtcNow.AddHours(-3),
					InvokeProcessorAfterReceiveTimeout = false,
					PrefetchCount = 50000,
					MaxBatchSize = 10000,
					ReceiveTimeOut = TimeSpan.FromSeconds(120),
				};

				// Log exceptions received.
				options.ExceptionReceived += (sender, e) => e.Exception.LogException("ProcessorSvc.ExceptionReceived", AdditionalLogProperties);
				kustoOptions.ExceptionReceived += (sender, e) => e.Exception.LogException("ProcessorSvc.ExceptionReceived", AdditionalLogProperties);

				// Load output settings.
				var esUri = new Uri(settings.GetSettingValue("OutputElasticSearchUri"));
				var esUserName = settings.GetSettingValue("OutputElasticSearchUsername");
				var esPassword = settings.GetSettingValue("OutputElasticSearchPassword");

                /*
				var awsEsUri = new Uri(settings.GetSettingValue("AWSElasticSearchUri"));
				var awsEsUserName = settings.GetSettingValue("AWSElasticSearchUsername");
				var awsEsPassword = settings.GetSettingValue("AWSElasticSearchPassword");
                */

				var factory = new EsSinkEventProcessorFactory(esUri, esUserName, esPassword);
				/*var awsEsSink = new EsSinkEventProcessorFactory(awsEsUri, awsEsUserName, awsEsPassword);
				var kustoFactory = new KustoSinkEventProcessorFactory(kustoStorageAccountKeys,
				kustoTelemetryServiceEndpoint, kustoInstrumentationKey, true, schemaInfo, kustoBaseContainerName, kustoBlobMemoryBufferSizeInBytes, kustoCompressBlobs);
                */

				// Register the event processor.
				await host.RegisterEventProcessorFactoryAsync(factory, options);
				//await awsHost.RegisterEventProcessorFactoryAsync(awsEsSink, options);
				//await kustoHost.RegisterEventProcessorFactoryAsync(kustoFactory, kustoOptions);

				cancellationToken.Register(() =>
				{
					Log.Information("ProcessorSvc.RunAsync stopped. InstanceId={InstanceId}, PartitionId={PartitionId}, NodeId={NodeId}, NodeName={NodeName}", Context?.InstanceId, Context?.PartitionId, Context?.NodeContext?.NodeId, Context?.NodeContext?.NodeName);
					host.UnregisterEventProcessorAsync().GetAwaiter().GetResult();
					//awsHost.UnregisterEventProcessorAsync().GetAwaiter().GetResult();
					//kustoHost.UnregisterEventProcessorAsync().GetAwaiter().GetResult();
				});
			}
			catch (Exception e)
			{
				e.LogException("ProcessorSvc.RunAsync", AdditionalLogProperties);
				throw;
			}
		}
	}
}
