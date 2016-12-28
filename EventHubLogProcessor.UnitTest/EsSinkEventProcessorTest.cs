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
	public class EsSinkEventProcessorTest
	{
		private const int MaxFailedDocumentRetries = 10;
		private const int MaxAbandonedDocumentRetries = 10;

		private readonly MockElasticClient _client;
		private readonly Mock<ICheckpointManager> _checkpointManager;
		private readonly PartitionContext _partition;
		private readonly EsSinkEventProcessor _processor;

		public EsSinkEventProcessorTest()
		{
			_client = new MockElasticClient(MockBehavior.Strict);
			_checkpointManager = new Mock<ICheckpointManager>(MockBehavior.Loose);
			_partition = MockPartitionContext(_checkpointManager.Object);
			_processor = new EsSinkEventProcessor(
				_client.Object,
				initialBackoffDelayInMilliseconds: 0,
				maxBackoffDelayInMilliseconds: 0,
				maxFailedDocumentRetries: MaxFailedDocumentRetries,
				maxAbandonedDocumentRetries: MaxAbandonedDocumentRetries);
		}

		[Fact]
		public async Task CloseAsync_Shutdown_CallsCheckpoint()
		{
			await _processor.OpenAsync(_partition);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			_checkpointManager.Verify(m => m.CheckpointAsync(It.IsAny<Lease>(), It.IsAny<string>(), It.IsAny<long>()), Times.Once);
		}

		[Fact]
		public async Task CloseAsync_LeaseLost_NoCheckpoint()
		{
			await _processor.OpenAsync(_partition);
			await _processor.CloseAsync(_partition, CloseReason.LeaseLost);

			_checkpointManager.Verify(m => m.CheckpointAsync(It.IsAny<Lease>(), It.IsAny<string>(), It.IsAny<long>()), Times.Never);
		}

		[Fact]
		public async Task ProcessEvents_ZeroDocuments_NoElasticClientCalls()
		{
			// Processing zero items - processor should not call any APIs on IElasticClient (test will fail because the mock is Strict with no methods setup).
			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, new EventData[] { });
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);
		}

		[Fact]
		public async Task ProcessEvents_OneDocument_Succeeds_OneElasticClientCall()
		{
			_client.OnBulkAsync().Returns(HttpStatusCode.OK);

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, new [] { MockEventData.CreateSerilogEventData("{}")});
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			_client.VerifyBulkAsyncCalled(Times.Once());
			Assert.Equal(0, _processor.LastBatchFailedDocuments);
			Assert.Equal(0, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_OneDocument_WithFailures_RetriesElasticClientCallUntilSuccess()
		{
			// Fail the BulkAsync call 100 times with BadGateway, then succeed with OK.
			const int failureCount = 100;
			int callCount = 0;
			_client.OnBulkAsync().Returns(() => ElasticsearchResponseExtensions.GetBulkResponse(++callCount <= failureCount ? HttpStatusCode.BadGateway : HttpStatusCode.OK));

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, new[] { MockEventData.CreateSerilogEventData("{}") });
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			_client.VerifyBulkAsyncCalled(Times.Exactly(failureCount + 1));
			Assert.Equal(0, _processor.LastBatchFailedDocuments);
			Assert.Equal(0, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_MultipleDocumentTypes_Succeeds()
		{
			_client.SimulateElasticSearch();

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

			// We should try calling BulkAsync once for the initial request.
			_client.VerifyBulkAsyncCalled(Times.Exactly(1));
			Assert.Equal(4 + 2, _client.GetTotalItems()); // 4 for serilog/robocustos/externaltelemetry/azure-resources indexes, 2 for ingestionstats
			Assert.Equal(1, _client.GetTotalItemsInIndex("logstash"));
			Assert.Equal(1, _client.GetTotalItemsInIndex("robointeractions"));
			Assert.Equal(1, _client.GetTotalItemsInIndex("externaltelemetry"));
			Assert.Equal(1, _client.GetTotalItemsInIndex("azure-resources"));
			Assert.Equal(2, _client.GetTotalItemsInIndex("ingestionstats"));
			Assert.Equal(0, _processor.LastBatchFailedDocuments);
			Assert.Equal(0, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_OneEmptyDocument_AbandonsDocument()
		{
			_client.SimulateElasticSearch();

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, new[] { MockEventData.CreateSerilogEventData("") });
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			// We should try calling BulkAsync once for the initial request, MaxFailedDocumentRetries times to retry the failed document, then once to abandon the document.
			_client.VerifyBulkAsyncCalled(Times.Exactly(1 + MaxFailedDocumentRetries + 1));
			Assert.Equal(1, _processor.LastBatchFailedDocuments);
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_OneEmptyDocument_TwoValidDocuments_AbandonsEmptyDocument()
		{
			_client.SimulateElasticSearch();

			var events = new[]
			{
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 1}"),
				MockEventData.CreateSerilogEventData(@""),
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 2}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			// We should try calling BulkAsync once for the initial request, MaxFailedDocumentRetries times to retry the failed document, then once to abandon the document.
			_client.VerifyBulkAsyncCalled(Times.Exactly(1 + MaxFailedDocumentRetries + 1));
			Assert.Equal(1, _processor.LastBatchFailedDocuments);
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_OneInvalidJsonDocument_TwoValidDocuments_AbandonsInvalidJsonDocument()
		{
			_client.SimulateElasticSearch();

			var events = new[]
			{
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 1}"),
				MockEventData.CreateSerilogEventData(@"invalid json"),
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 2}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			// We should try calling BulkAsync once for the initial request, MaxFailedDocumentRetries times to retry the failed documents, then once to abandon the document.
			_client.VerifyBulkAsyncCalled(Times.Exactly(1 + MaxFailedDocumentRetries + 1));
			Assert.Equal(1, _processor.LastBatchFailedDocuments);
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_OneUnknownDocumentType_AbandonsUnknownDocumentType()
		{
			_client.SimulateElasticSearch();

			var events = new[]
			{
				MockEventData.CreateInvalidEventData(@"{ ""message"": ""invalid type"" }"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			// We should try calling BulkAsync once to abandon the invalid document.
			_client.VerifyBulkAsyncCalled(Times.Exactly(1));
			Assert.Equal(0, _processor.LastBatchFailedDocuments); // The invalid document type should not be retried.
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments); // The invalid document type should be abandoned immediately.
		}

		[Fact]
		public async Task ProcessEvents_OneUnknownDocumentType_TwoValidDocuments_AbandonsUnknownDocumentType()
		{
			_client.SimulateElasticSearch();

			var events = new[]
			{
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 1}"),
				MockEventData.CreateInvalidEventData(@"{ ""message"": ""invalid type"" }"),
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 2}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			// We should try calling BulkAsync once for the initial request, then once to abandon the invalid document.
			_client.VerifyBulkAsyncCalled(Times.Exactly(1 + 1));
			Assert.Equal(0, _processor.LastBatchFailedDocuments); // The invalid document type should not be retried.
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments); // The invalid document type should be abandoned immediately.
		}

		[Fact]
		public async Task ProcessEvents_MultipleInvalidDocuments_AbandonsInvalidDocuments()
		{
			_client.SimulateElasticSearch();

			var events = new[]
			{
				MockEventData.CreateSerilogEventData(@"invalid json"),
				MockEventData.CreateSerilogEventData(@""),
				MockEventData.CreateSerilogEventData(@"invalid json"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			// We should try calling BulkAsync once for the initial request, MaxFailedDocumentRetries times to retry the failed document, then once to abandon the document.
			_client.VerifyBulkAsyncCalled(Times.Exactly(1 + MaxFailedDocumentRetries + 1));
			Assert.Equal(3, _processor.LastBatchFailedDocuments);
			Assert.Equal(3, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_InvalidDocument_AndUnknownDocumentType_AbandonsInvalidAndUnknownDocuments()
		{
			_client.SimulateElasticSearch();

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

			// We should try calling BulkAsync once for the initial request, MaxFailedDocumentRetries times to retry the failed documents,
			// once to abandon the invalid documents, and once to abandon the unknown document type.
			_client.VerifyBulkAsyncCalled(Times.Exactly(1 + MaxFailedDocumentRetries + 1 + 1));
			Assert.Equal(4 + 2 + 3, _client.GetTotalItems()); // 4 successful, 2 ingestionstats, 3 abandoned
			Assert.Equal(1, _client.GetTotalItemsInIndex("logstash"));
			Assert.Equal(1, _client.GetTotalItemsInIndex("robointeractions"));
			Assert.Equal(1, _client.GetTotalItemsInIndex("externaltelemetry"));
			Assert.Equal(1, _client.GetTotalItemsInIndex("azure-resources"));
			Assert.Equal(2, _client.GetTotalItemsInIndex("ingestionstats"));
			Assert.Equal(3, _client.GetTotalItemsInIndex("abandoneddocs"));
			Assert.Equal(2, _processor.LastBatchFailedDocuments);
			Assert.Equal(3, _processor.LastBatchAbandonedDocuments);
		}

		[Fact]
		public async Task ProcessEvents_DocumentWithNewlines_AbandonsInvalidDocument()
		{
			_client.SimulateElasticSearch();

			var events = new[]
			{
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 1}"),
				MockEventData.CreateSerilogEventData(@"
				{
					""message"": ""documents formatted with newlines are invalid for Elasticsearch bulk upload."",
					""data"": 2
				}"),
				MockEventData.CreateSerilogEventData(@"{ ""message"": ""log message"", ""data"": 3}"),
			};

			await _processor.OpenAsync(_partition);
			await _processor.ProcessEventsAsync(_partition, events);
			await _processor.CloseAsync(_partition, CloseReason.Shutdown);

			// We should try calling BulkAsync once for the initial request, then once to abandon the invalid document.
			_client.VerifyBulkAsyncCalled(Times.Exactly(1 + 1));
			Assert.Equal(0, _processor.LastBatchFailedDocuments);
			Assert.Equal(1, _processor.LastBatchAbandonedDocuments);
		}

		private static PartitionContext MockPartitionContext(ICheckpointManager checkpointManager)
		{
			var ctor = typeof(PartitionContext).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null, new [] { typeof(ICheckpointManager) }, null);
			var partition = (PartitionContext) ctor?.Invoke(new object[] { checkpointManager });
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
