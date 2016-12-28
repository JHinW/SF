using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Nest;
using Microsoft.WindowsAzure.Storage.Blob;

namespace EventHubLogProcessor
{
	public class KustoSinkEventProcessorFactory : IEventProcessorFactory
	{
		private readonly string _telemetryServiceEndpoint;
		private readonly string _instrumentationKey;
		private readonly string _baseContainerName;
		private readonly List<CloudBlobClient> _blobClients;
		private readonly bool _sendProcessingStats;
		private readonly Dictionary<string, Guid> _schemaInformation;
		private readonly long _bufferSizeInBytes;
		private readonly bool _compressBlobs;

		public KustoSinkEventProcessorFactory(string storageAccountConfigValue, string telemetryServiceEndpoint, string instrumentationKey, bool sendProcessingStats, Dictionary<string, Guid> schemaInformation, string baseContainerName = "kusto-logs", long kustoBlobMemoryBufferSizeInBytes = 20971520, bool compressBlobs = false)
		{
			if (string.IsNullOrEmpty(storageAccountConfigValue))
			{
				throw new ArgumentNullException(nameof(storageAccountConfigValue));
			}
			if (string.IsNullOrEmpty(telemetryServiceEndpoint))
			{
				throw new ArgumentNullException(nameof(telemetryServiceEndpoint));
			}
			_telemetryServiceEndpoint = telemetryServiceEndpoint;
			_instrumentationKey = instrumentationKey;
			_baseContainerName = baseContainerName;
			_sendProcessingStats = sendProcessingStats;
			_schemaInformation = schemaInformation;
			_bufferSizeInBytes = kustoBlobMemoryBufferSizeInBytes;
			_compressBlobs = compressBlobs;

			//TODO: check args for sensible values
			List<CloudStorageAccount> storageAccounts = storageAccountConfigValue
				.Split(",".ToCharArray(), StringSplitOptions.RemoveEmptyEntries)
				.Select(CloudStorageAccount.Parse).ToList();

			_blobClients = storageAccounts.Select(s => s.CreateCloudBlobClient()).ToList();
		}

		public IEventProcessor CreateEventProcessor(PartitionContext context)
		{
			return new KustoSinkEventProcessor(
				_blobClients,
				new Uri(_telemetryServiceEndpoint),
				_instrumentationKey,
				_baseContainerName,
				_schemaInformation,
				_sendProcessingStats,
				_bufferSizeInBytes,
				_compressBlobs);
		}
	}
}
