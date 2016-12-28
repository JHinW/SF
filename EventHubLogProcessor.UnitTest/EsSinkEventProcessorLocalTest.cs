using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Microsoft.ServiceBus.Messaging;
using Moq;
using Nest;
using Xunit;

namespace EventHubLogProcessor.UnitTest
{
	/// <summary>
	/// Unit test class for verifying the EsSinkEventProcessor against a local Elasticsearch deployment.
	/// Make this class 'public' to run the xUnit unit test methods.
	/// </summary>
	class EsSinkEventProcessorLocalTest
	{
		private const int MaxFailedDocumentRetries = 10;
		private const int MaxAbandonedDocumentRetries = 10;

		private readonly IElasticClient _client;
		private readonly Mock<ICheckpointManager> _checkpointManager;
		private readonly PartitionContext _partition;
		private readonly EsSinkEventProcessor _processor;

		public EsSinkEventProcessorLocalTest()
		{
			_client = new ElasticClient(new Uri("http://localhost:9200"));
			_checkpointManager = new Mock<ICheckpointManager>(MockBehavior.Loose);
			_partition = MockPartitionContext(_checkpointManager.Object);
			_processor = new EsSinkEventProcessor(
				_client,
				initialBackoffDelayInMilliseconds: 0,
				maxBackoffDelayInMilliseconds: 0,
				maxFailedDocumentRetries: MaxFailedDocumentRetries,
				maxAbandonedDocumentRetries: MaxAbandonedDocumentRetries);
		}

		[Fact]
		public async Task ProcessEvents_ZeroDocuments_NoExceptions()
		{
			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, new EventData[] { });
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);
		}

		[Fact]
		public async Task ProcessEvents_OneDocument_Succeeds()
		{
			var events = new[]
			{
				MockEventData.CreateSerilogEventData($"{{ \"message\": \"ProcessEvents_OneDocument_Succeeds\", \"@timestamp\":\"{DateTime.UtcNow:o}\", \"data\": 1 }}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(0, _processor.LastBatchFailedDocuments);
			Assert.Equal(0, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_MultipleDocumentTypes_Succeeds()
		{
			var events = new[]
			{
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 1}"),
				MockEventData.CreateRoboCustosEventData(@"{ ""message"": ""log message"", ""data"": 2}"),
				MockEventData.CreateExternalTelemetryEventData(@"{ ""message"": ""log message"", ""data"": 3}"),
				MockEventData.CreateAzureResourcesEventData(@"{ ""message"": ""log message"", ""data"": 4}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(0, _processor.LastBatchFailedDocuments);
			Assert.Equal(0, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_OneEmptyDocument_AbandonsDocument()
		{
			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, new[] { MockEventData.CreateSerilogEventData("") });
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(1, _processor.LastBatchFailedDocuments);
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_OneEmptyDocument_TwoValidDocuments_AbandonsEmptyDocument()
		{
			var events = new[]
			{
				MockEventData.CreateSerilogEventData($"{{ \"message\": \"ProcessEvents_OneEmptyDocument_TwoValidDocuments_AbandonsEmptyDocument\", \"@timestamp\":\"{DateTime.UtcNow:o}\", \"data\": 1 }}"),
				MockEventData.CreateSerilogEventData($""),
				MockEventData.CreateSerilogEventData($"{{ \"message\": \"ProcessEvents_OneEmptyDocument_TwoValidDocuments_AbandonsEmptyDocument\", \"@timestamp\":\"{DateTime.UtcNow:o}\", \"data\": 2 }}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(1, _processor.LastBatchFailedDocuments);
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_OneInvalidJsonDocument_TwoValidDocuments_AbandonsInvalidJsonDocument()
		{
			var events = new[]
			{
				MockEventData.CreateSerilogEventData($"{{ \"message\": \"ProcessEvents_OneInvalidJsonDocument_TwoValidDocuments_AbandonsInvalidJsonDocument\", \"@timestamp\":\"{DateTime.UtcNow:o}\", \"data\": 1 }}"),
				MockEventData.CreateSerilogEventData(@"invalid json"),
				MockEventData.CreateSerilogEventData($"{{ \"message\": \"ProcessEvents_OneInvalidJsonDocument_TwoValidDocuments_AbandonsInvalidJsonDocument\", \"@timestamp\":\"{DateTime.UtcNow:o}\", \"data\": 2 }}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(1, _processor.LastBatchFailedDocuments);
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_OneUnknownDocumentType_AbandonsUnknownDocumentType()
		{
			var events = new[]
			{
				MockEventData.CreateInvalidEventData(@"{ ""message"": ""invalid type"" }"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(0, _processor.LastBatchFailedDocuments); // The invalid document type should not be retried.
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments); // The invalid document type should be abandoned immediately.
		}

		[Fact]
		public async Task ProcessEvents_OneUnknownDocumentType_TwoValidDocuments_AbandonsUnknownDocumentType()
		{
			var events = new[]
			{
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 1}"),
				MockEventData.CreateInvalidEventData(@"{ ""message"": ""invalid type"" }"),
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 2}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(0, _processor.LastBatchFailedDocuments); // The invalid document type should not be retried.
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments); // The invalid document type should be abandoned immediately.
		}

		[Fact]
		public async Task ProcessEvents_MultipleInvalidDocuments_AbandonsInvalidDocuments()
		{
			var events = new[]
			{
				MockEventData.CreateSerilogEventData(@"invalid json"),
				MockEventData.CreateSerilogEventData(@""),
				MockEventData.CreateSerilogEventData(@"invalid json"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(3, _processor.LastBatchFailedDocuments);
			Assert.Equal(3, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_InvalidDocument_AndUnknownDocumentType_AbandonsInvalidAndUnknownDocuments()
		{
			var events = new[]
			{
				MockEventData.CreateInvalidEventData(@"{ ""message"": ""invalid type"" }"),
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 1}"),
				MockEventData.CreateSerilogEventData(@"invalid json"),
				MockEventData.CreateRoboCustosEventData(@"{ ""message"": ""log message"", ""data"": 2}"),
				MockEventData.CreateSerilogEventData(@""),
				MockEventData.CreateExternalTelemetryEventData(@"{ ""message"": ""log message"", ""data"": 3}"),
				MockEventData.CreateAzureResourcesEventData(@"{ ""message"": ""log message"", ""data"": 4}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(2, _processor.LastBatchFailedDocuments);
			Assert.Equal(3, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_DocumentWithNewlines_AbandonsInvalidDocument()
		{
			var events = new[]
			{
				MockEventData.CreateSerilogEventData($"{{ \"message\": \"ProcessEvents_DocumentWithNewlines_AbandonsInvalidDocument\", \"@timestamp\":\"{DateTime.UtcNow:o}\", \"data\": 1 }}"),
				MockEventData.CreateSerilogEventData(@"
				{
					""message"": ""documents formatted with newlines are invalid for Elasticsearch bulk upload."",
					""data"": 2
				}"),
				MockEventData.CreateSerilogEventData($"{{ \"message\": \"ProcessEvents_DocumentWithNewlines_AbandonsInvalidDocument\", \"@timestamp\":\"{DateTime.UtcNow:o}\", \"data\": 3 }}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			Assert.Equal(0, _processor.LastBatchFailedDocuments);
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments);
		}

		private static PartitionContext MockPartitionContext(ICheckpointManager checkpointManager)
		{
			var ctor = typeof(PartitionContext).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { typeof(ICheckpointManager) }, null);
			var partition = (PartitionContext)ctor?.Invoke(new object[] { checkpointManager });
			Assert.NotNull(partition);

			partition.Lease = new Lease()
			{
				Epoch = 1,
				Offset = "10",
				Owner = "EsSinkEventProcessorTest",
				PartitionId = "0",
				SequenceNumber = 17,
				Token = Guid.NewGuid().ToString(),
			};

			return partition;
		}
	}
}
