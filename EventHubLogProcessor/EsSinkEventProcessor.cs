using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Microsoft.ServiceBus.Messaging;
using Nest;
using Newtonsoft.Json;
using Serilog;

namespace EventHubLogProcessor
{
	public sealed class EsSinkEventProcessor : IEventProcessor
	{
		private static readonly TimeSpan CheckpointFrequency = TimeSpan.FromMinutes(1);
		private readonly int MaxFailedDocumentRetries = 10;
		private readonly int MaxAbandonedDocumentRetries = 10;
		private readonly int InitialBackoffDelayInMilliseconds = 100;
		private readonly int MaxBackoffDelayInMilliseconds = 5000;

		private readonly IElasticClient _client;
		private readonly bool _sendProcessingStats = true;
		private DateTime _lastCheckpointTime = DateTime.UtcNow;
		private TimeSpan _lastBatchElapsedTime = TimeSpan.Zero;
		private int _lastBatchFailedDocuments = 0;
		private int _lastBatchAbandonedDocuments = 0;

		public EsSinkEventProcessor(IElasticClient client, bool sendProcessingStats = true, int? initialBackoffDelayInMilliseconds = null, int? maxBackoffDelayInMilliseconds = null, int? maxFailedDocumentRetries = null, int? maxAbandonedDocumentRetries = null)
		{
			_client = client;
			_sendProcessingStats = sendProcessingStats;

			if (initialBackoffDelayInMilliseconds.HasValue)
				InitialBackoffDelayInMilliseconds = initialBackoffDelayInMilliseconds.Value;
			if (maxBackoffDelayInMilliseconds.HasValue)
				MaxBackoffDelayInMilliseconds = maxBackoffDelayInMilliseconds.Value;
			if (maxFailedDocumentRetries.HasValue)
				MaxFailedDocumentRetries = maxFailedDocumentRetries.Value;
			if (maxAbandonedDocumentRetries.HasValue)
				MaxAbandonedDocumentRetries = maxAbandonedDocumentRetries.Value;
		}

		public DateTime LastCheckpointTime => _lastCheckpointTime;
		public TimeSpan LastBatchElapsedTime => _lastBatchElapsedTime;
		public int LastBatchFailedDocuments => _lastBatchFailedDocuments;
		public int LastBatchAbandonedDocuments => _lastBatchAbandonedDocuments;

		public Task OpenAsync(PartitionContext context)
		{
			Log.Information("Processor opened for partition {PartitionId}", context.Lease.PartitionId);
			return Task.FromResult(0);
		}

		public async Task CloseAsync(PartitionContext context, CloseReason reason)
		{
			Log.Information("Processor closed for partition {PartitionId} for reason {Reason}", context.Lease.PartitionId, reason);

			// On clean shutdown, checkpoint our latest state.
			if (reason == CloseReason.Shutdown)
			{
				await context.CheckpointAsync();
			}
		}

		public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
		{
			try
			{
				// Get the bulk message to send.
				Dictionary<string, BulkItem> items;
				List<BulkItem> invalidItems;
				var body = GetBulkBody(context, messages, out items, out invalidItems);
				if (body != null)
				{
					// Send the bulk message to ElasticSearch.
					var response = await SendBatchAsync(body);

					// Analyze the response.
					await AnalyzeResponse(response, items);
				}

				// Immediately abandon any invalid items.
				if (invalidItems?.Count > 0)
				{
					await SendInvalidDocumentInfo(invalidItems);
				}

				// Checkpoint now that we've processed the messages.
				if (DateTime.UtcNow > _lastCheckpointTime.Add(CheckpointFrequency))
				{
					await context.CheckpointAsync();
					_lastCheckpointTime = DateTime.UtcNow;
				}
			}
			catch (Exception e)
			{
				if (IsTaskCanceled(e))
				{
					Log.Information("Task canceled while processing {Count} messages in partition {PartitionId}", messages.Count(), context.Lease.PartitionId);
				}
				else
				{
					Log.Error(e, "Error processing {PartitionId}: {Message}", context.Lease.PartitionId, e.Message);
				}
				throw;
			}
		}

		private static bool IsTaskCanceled(Exception e)
		{
			if (e == null)
			{
				return false;
			}
			if (e is TaskCanceledException)
			{
				return true;
			}
			var asAgg = e as AggregateException;
			if (asAgg != null)
			{
				return IsTaskCanceled(asAgg.InnerException);
			}
			return false;
		}

		/// <summary>
		/// Builds the ElasticSearch bulk index request from the given EventHub messages, include ingestionstats data.
		/// Outputs a dictionary (<paramref name="items"/>) mapping from document id to the pre-processed BulkItem.
		/// Outputs a list (<paramref name="invalidItems"/>) of invalid EventHub messages to be immediately abandoned.
		/// </summary>
		private string GetBulkBody(PartitionContext context, IEnumerable<EventData> messages, out Dictionary<string, BulkItem> items, out List<BulkItem> invalidItems)
		{
			var now = DateTime.UtcNow;
			var builder = new StringBuilder();
			items = new Dictionary<string, BulkItem>();
			invalidItems = new List<BulkItem>();
			var messageCount = 0;

			// Find the last, oldest item by creation time, and oldest item by enqueue time.
			BulkItem lastItem = null, oldestItemByTimestamp = null, oldestItemByEnqueueTime = null;
			foreach (var message in messages)
			{
				var bulkItem = BulkItem.FromEventData(message);

				// If the item appears to be invalid, mark it to be abandoned immediately.
				if (!bulkItem.IsValid)
				{
					invalidItems.Add(bulkItem);
					continue;
				}

				// Include this item in the bulk index request.
				builder.AppendLine(bulkItem.ToBulkString());
				items[bulkItem.DocumentId] = bulkItem;

				// Remember the last/oldest items.
				if (oldestItemByTimestamp == null || bulkItem.Timestamp < oldestItemByTimestamp.Timestamp)
					oldestItemByTimestamp = bulkItem;
				if (oldestItemByEnqueueTime == null || bulkItem.EnqueueTime < oldestItemByEnqueueTime.EnqueueTime)
					oldestItemByEnqueueTime = bulkItem;
				lastItem = bulkItem;
				messageCount++;
			}

			// Verify we received at least one item.
			if (lastItem == null)
				return null;

			if (_sendProcessingStats)
			{
				// Compute the various lag metrics.
				var lag = GetTimeSpanOrZero(now - lastItem.EnqueueTime);
				var maxLag = GetTimeSpanOrZero(now - oldestItemByEnqueueTime.EnqueueTime);
				var lagFromMessageCreation = GetTimeSpanOrZero(now - lastItem.Timestamp);
				var maxLagFromMessageCreation = GetTimeSpanOrZero(now - oldestItemByTimestamp.Timestamp);

				// Get the batch statistics.
				var batchStats = GetBatchStats(context, now, messageCount, lastItem, oldestItemByTimestamp, oldestItemByEnqueueTime, lag, maxLag, lagFromMessageCreation, maxLagFromMessageCreation);
				builder.AppendLine(batchStats.ToBulkString());
				items[batchStats.DocumentId] = batchStats;

				// Get the partition statistics.
				var partitionStats = GetPerPartitionBatchStats(context, now, lastItem, oldestItemByTimestamp, oldestItemByEnqueueTime, lag, maxLag, lagFromMessageCreation, maxLagFromMessageCreation);
				builder.AppendLine(partitionStats.ToBulkString());
				items[partitionStats.DocumentId] = partitionStats;
			}

			return builder.ToString();
		}

		/// <summary>
		/// Send a bulk index request to ElasticSearch with infinite retries, timing how long it takes to complete.
		/// The batch processing time will be included in ingestionstats for the next batch request.
		/// </summary>
		private async Task<ElasticsearchResponse<BulkResponse>> SendBatchAsync(string body)
		{
			var timer = Stopwatch.StartNew();
			var response = await BulkSendWithRetries("EsSinkEventProcessor.SendBatchAsync", body);
			_lastBatchElapsedTime = timer.Elapsed;
			return response;
		}

		/// <summary>
		/// Analyze the response from the ElasticSearch bulk index request.  Any failed documents will be retried a fixed number
		/// of times.  If those documents continue to fail, they will be abandoned (sent to the abandoneddocs index in ElasticSearch).
		/// </summary>
		private async Task AnalyzeResponse(ElasticsearchResponse<BulkResponse> response, Dictionary<string, BulkItem> items)
		{
			_lastBatchFailedDocuments = 0;
			_lastBatchAbandonedDocuments = 0;

			if (response.Success && response.Body.Errors)
			{
				_lastBatchFailedDocuments = response.Body.ItemsWithErrors.Count();

				// Try re-sending the failed documents individually a fixed number of times.
				var retryResponse = await RetryFailedDocuments(response.Body.ItemsWithErrors, items);

				// Send the failed documents to the ElasticSearch abandoneddocs index.
				if (!retryResponse.Success || retryResponse.Body.Errors)
				{
					var abandonedInfo = GetBulkAbandonedDocumentInfo(retryResponse.Body.ItemsWithErrors, items);
					await SendAbandonedDocumentInfo(abandonedInfo);
				}
			}
		}

		/// <summary>
		/// Retry sending the failed documents a fixed number of times before abandoning them.  The abandoned documents are returned.
		/// </summary>
		/// <param name="failedDocuments">Failed documents to retry sending to ElasticSearch.</param>
		/// <param name="items">Processed documents indexed by document id.</param>
		/// <returns>The documents to abandon.</returns>
		private Task<ElasticsearchResponse<BulkResponse>> RetryFailedDocuments(IEnumerable<BulkResponseItemBase> failedDocuments, Dictionary<string, BulkItem> items)
		{
			var body = GetBulkFailedDocumentInfo(failedDocuments, items);
			return BulkSendWithRetries("EsSinkEventProcessor.RetryFailedDocuments", body, maxRetries: MaxFailedDocumentRetries);
		}

		/// <summary>
		/// Send the abandoned documents as a bulk index request to ElasticSearch with a fixed number of retries.
		/// If the documents fail to be sent to ElasticSearch, an error message is logged to Serilog.
		/// These logs will typically be sent directly to ElasticSearch and Azure Table storage.
		/// </summary>
		private async Task SendAbandonedDocumentInfo(string abandonedInfo)
		{
			try
			{
				// Send the abandoned documents and the associated errors to Elasticsearch.
				// If they failed due to malformed content, then retries will always fail.
				// If they failed due to ElasticSearch issue (node down, queue full, etc.), then retries could succeed.
				var response = await BulkSendWithRetries("EsSinkEventProcessor.SendAbandonedDocumentInfo", abandonedInfo, maxRetries: MaxAbandonedDocumentRetries);

				if (!response.Success || !response.Body.IsValid || response.Body.Errors)
				{
					Log.Error("Failed to log abandoned doc information");
				}
			}
			catch (Exception e)
			{
				Log.Error(e, "Failed to log abandoned doc information");
			}
		}

		/// <summary>
		/// Send the invalid documents directly to the abandoneddocs index in ElasticSearch with a fixed number of retries.
		/// </summary>
		private Task SendInvalidDocumentInfo(IEnumerable<BulkItem> invalidDocuments)
		{
			var abandonedInfo = GetBulkInvalidDocumentInfo(invalidDocuments);
			return SendAbandonedDocumentInfo(abandonedInfo);
		}

		/// <summary>
		/// Get the bulk index request body for the failed documents to retry sending to ElasticSearch.
		/// </summary>
		private string GetBulkFailedDocumentInfo(IEnumerable<BulkResponseItemBase> failedDocuments, Dictionary<string, BulkItem> items)
		{
			var now = DateTime.UtcNow;
			var builder = new StringBuilder();

			foreach (var failedDocument in failedDocuments)
			{
				var item = items[failedDocument.Id];
				builder.AppendLine(item.ToBulkString());
			}

			return builder.ToString();
		}

		/// <summary>
		/// Get the bulk index request body for the abandoned documents to try sending to ElasticSearch.
		/// </summary>
		private string GetBulkAbandonedDocumentInfo(IEnumerable<BulkResponseItemBase> abandonedDocuments, Dictionary<string, BulkItem> items)
		{
			var now = DateTime.UtcNow;
			var builder = new StringBuilder();

			foreach (var abandonedDocument in abandonedDocuments)
			{
				var abandonedDocInfo = GetAbandonDocumentInfo(now, items[abandonedDocument.Id], abandonedDocument.Error?.ToString());
				builder.AppendLine(abandonedDocInfo.ToBulkString());
				_lastBatchAbandonedDocuments++;
			}

			return builder.ToString();
		}

		/// <summary>
		/// Get the bulk index request body for the invalid documents to try sending to ElasticSearch.
		/// </summary>
		private string GetBulkInvalidDocumentInfo(IEnumerable<BulkItem> invalidDocuments)
		{
			var now = DateTime.UtcNow;
			var builder = new StringBuilder();

			foreach (var invalidDocument in invalidDocuments)
			{
				var abandonedDocInfo = GetAbandonDocumentInfo(now, invalidDocument, invalidDocument.InvalidReason);
				builder.AppendLine(abandonedDocInfo.ToBulkString());
				_lastBatchAbandonedDocuments++;
			}

			return builder.ToString();
		}

		/// <summary>
		/// Send a bulk index request to ElasticSearch with automatic retries, optionally with a maximum number of retries.
		/// If <paramref name="maxRetries"/> is not specified (or set to <see cref="int.MaxValue"/>), this will retry indefinitely.
		/// </summary>
		private Task<ElasticsearchResponse<BulkResponse>> BulkSendWithRetries(string messagePrefix, string body, int maxRetries = int.MaxValue)
		{
			return SendWithRetries(
				messagePrefix,
				() => _client.LowLevel.BulkAsync<BulkResponse>(body, p => p.Consistency(Consistency.One).Refresh(false)),
				r => (maxRetries == int.MaxValue) ? r.Success : r.Success && r.Body.IsValid,
				maxRetries);
		}

		/// <summary>
		/// Send an ElasticSearch request with automatic retries, optionally with a maximum number of retries.
		/// If <paramref name="maxRetries"/> is not specified (or set to <see cref="int.MaxValue"/>), this will retry indefinitely.
		/// </summary>
		private async Task<ElasticsearchResponse<T>> SendWithRetries<T>(string messagePrefix, Func<Task<ElasticsearchResponse<T>>> action, Func<ElasticsearchResponse<T>, bool> responsePredicate, int maxRetries = int.MaxValue) where T : ResponseBase
		{
			var backoffDelayInMilliseconds = InitialBackoffDelayInMilliseconds;
			int retryCount = 0;

			while (true)
			{
				try
				{
					// Invoke the ElasticSearch send action.
					var response = await action.Invoke();

					// The response predicate determines what a successful ElasticSearch response looks like.
					if (responsePredicate.Invoke(response))
					{
						return response;
					}

					// After retrying for the maximum number of times, simply return the last ElasticSearch response.
					if (++retryCount >= maxRetries)
					{
						return response;
					}
				}
				catch (Exception e)
				{
					// Log any errors but continue retrying.
					Log.Error(e, messagePrefix);
				}

				// Trace every 10th retry.
				if (retryCount%10 == 0)
				{
					Log.Warning("{MessagePrefix}: failed to send.  Retrying ({RetryCount}) after {RetryDelayInMilliseconds} ms.", messagePrefix, retryCount, backoffDelayInMilliseconds);
					backoffDelayInMilliseconds = Math.Min(backoffDelayInMilliseconds * 2, MaxBackoffDelayInMilliseconds);
				}

				// Exponential back-off before retrying.
				await Task.Delay(backoffDelayInMilliseconds);
			}
		}

		private BulkItem GetBatchStats(
			PartitionContext context,
			DateTime now,
			int messageCount,
			BulkItem lastItem,
			BulkItem oldestItemByTimestamp,
			BulkItem oldestItemByEnqueueTime,
			TimeSpan lag,
			TimeSpan maxLag,
			TimeSpan lagFromMessageCreation,
			TimeSpan maxLagFromMessageCreation)
		{
			var messageBody =
				"{" +
					$"\"lastMessageTimestampInBatch\":\"{lastItem.Timestamp:o}\"," +
					$"\"lastMessageEnqueueTimeInBatch\":\"{lastItem.EnqueueTime:o}\"," +
					$"\"oldestMessageTimestampInBatch\":\"{oldestItemByTimestamp.Timestamp:o}\"," +
					$"\"oldestMessageEnqueueTimeInBatch\":\"{oldestItemByEnqueueTime.EnqueueTime:o}\"," +
					$"\"idOfOldestMessageInBatch\":\"{oldestItemByTimestamp.DocumentId}\"," +
					$"\"idOfOldestEnqueuedMessageInBatch\":\"{oldestItemByEnqueueTime.DocumentId}\"," +
					$"\"lagInMilliseconds\":{lag.TotalMilliseconds}," +
					$"\"maxLagInMilliseconds\":{maxLag.TotalMilliseconds}," +
					$"\"lagInMinutes\":{lag.TotalMinutes}," +
					$"\"maxLagInMinutes\":{maxLag.TotalMinutes}," +
					$"\"lagFromMessageCreationTimeInMinutes\":{lagFromMessageCreation.TotalMinutes}," +
					$"\"maxLagFromMessageCreationTimeInMinutes\":{maxLagFromMessageCreation.TotalMinutes}," +
					$"\"timestamp\":\"{now:o}\"," +
					$"\"lastBatchElapsedTimeInMilliseconds\":{_lastBatchElapsedTime.TotalMilliseconds}," +
					$"\"taskId\":{context.Lease.PartitionId}," +
					$"\"batchSize\":{messageCount}," +
					$"\"lastBatchFailedDocuments\":{_lastBatchFailedDocuments}," +
					$"\"lastBatchAbandonedDocuments\":{_lastBatchAbandonedDocuments}" +
				"}";

			return new BulkItem(
				indexBaseName: "ingestionstats",
				timestamp: now,
				enqueueTime: now,
				documentType: "batchstats",
				documentId: Guid.NewGuid().ToString(),
				documentBody: messageBody);
		}

		private BulkItem GetPerPartitionBatchStats(
			PartitionContext context,
			DateTime now,
			BulkItem lastItem,
			BulkItem oldestItemByTimestamp,
			BulkItem oldestItemByEnqueueTime,
			TimeSpan lag,
			TimeSpan maxLag,
			TimeSpan lagFromMessageCreation,
			TimeSpan maxLagFromMessageCreation)
		{
			var messageBody =
				"{" +
					$"\"lastMessageTimestampInBatch\":\"{lastItem.Timestamp:o}\"," +
					$"\"lastMessageEnqueueTimeInBatch\":\"{lastItem.EnqueueTime:o}\"," +
					$"\"oldestMessageTimestampInBatch\":\"{oldestItemByTimestamp.Timestamp:o}\"," +
					$"\"oldestMessageEnqueueTimeInBatch\":\"{oldestItemByEnqueueTime.EnqueueTime:o}\"," +
					$"\"lagInMilliseconds\":{lag.TotalMilliseconds}," +
					$"\"maxLagInMilliseconds\":{maxLag.TotalMilliseconds}," +
					$"\"lagInMinutes\":{lag.TotalMinutes}," +
					$"\"maxLagInMinutes\":{maxLag.TotalMinutes}," +
					$"\"lagFromMessageCreationTimeInMinutes\":{lagFromMessageCreation.TotalMinutes}," +
					$"\"maxLagFromMessageCreationTimeInMinutes\":{maxLagFromMessageCreation.TotalMinutes}," +
					$"\"timestamp\":\"{now:o}\"," +
					$"\"partitionId\":{context.Lease.PartitionId}," +
					$"\"taskId\":{context.Lease.PartitionId}" +
				"}";

			return new BulkItem(
				indexBaseName: "ingestionstats",
				timestamp: now,
				enqueueTime: now,
				documentType: "perpartitionstats",
				documentId: Guid.NewGuid().ToString(),
				documentBody: messageBody);
		}

		private BulkItem GetAbandonDocumentInfo(
			DateTime now,
			BulkItem abandonedItem,
			string errorMessage)
		{
			var messageBody =
				"{" +
					$"\"docId\":\"{abandonedItem.DocumentId}\"," +
					$"\"docContent\":{JsonConvert.ToString(abandonedItem.DocumentBodyStart)}," +
					$"\"lastError\":{JsonConvert.ToString(errorMessage)}," +
					$"\"timestamp\":\"{now:o}\"" +
				"}";

			return new BulkItem(
				indexBaseName: "abandoneddocs",
				timestamp: now,
				enqueueTime: now,
				documentType: "abandoneddocinfo",
				documentId: Guid.NewGuid().ToString(),
				documentBody: messageBody);
		}

		private static TimeSpan GetTimeSpanOrZero(TimeSpan timeSpan)
		{
			return timeSpan < TimeSpan.Zero ? TimeSpan.Zero : timeSpan;
		}
	}
}
