using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using EventHubLogProcessor.InteractionJsonHelpers;
using Microsoft.AzureCAT.Extensions.Logging.AppInsights.Models;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;

namespace EventHubLogProcessor
{
	public sealed class KustoSinkEventProcessor : IEventProcessor
	{
		private KustoBatchStats _batchStats = new KustoBatchStats();
		private bool _sendProcessingStats;

		public bool CompressBlobs { get; set; }

		private static readonly TimeSpan CheckpointFrequency = TimeSpan.FromMinutes(3);

		private DateTime _lastCheckpointTime = DateTime.UtcNow;

		private Dictionary<string, BlobContainerSink> _blobSinksBySchema = new Dictionary<string, BlobContainerSink>();

		public KustoSinkEventProcessor(IEnumerable<CloudBlobClient> blobClients,
			Uri telemetryServiceEndpoint, string instrumentationKey, string baseContainerName, Dictionary<string, Guid> schemaInformation, 
			bool sendProcessingStats, long bufferSizeInBytes, bool compressBlobs,  int? initialBackoffDelayInMilliseconds = null,
			int? maxBackoffDelayInMilliseconds = null)
		{
			_sendProcessingStats = sendProcessingStats;
			var blobClientsList = new List<CloudBlobClient>(blobClients);

			foreach (var schema in schemaInformation.Keys)
			{
				_blobSinksBySchema.Add(schema,
					new BlobContainerSink(blobClientsList, schema, schemaInformation[schema], telemetryServiceEndpoint, instrumentationKey,
						baseContainerName, bufferSizeInBytes, compressBlobs, initialBackoffDelayInMilliseconds, maxBackoffDelayInMilliseconds));
			}
		}

		public DateTime LastCheckpointTime => _lastCheckpointTime;
		public TimeSpan LastBatchElapsedTime { get; } = TimeSpan.Zero;

		public int LastBatchFailedDocuments { get; } = 0;

		public int LastBatchAbandonedDocuments { get; } = 0;

		public Task OpenAsync(PartitionContext context)
		{
			Log.Information("KustoProcessor opened for partition {PartitionId}", context.Lease.PartitionId);
			return Task.FromResult(0);
		}

		public async Task CloseAsync(PartitionContext context, CloseReason reason)
		{
			Log.Information("KustoProcessor closed for partition {PartitionId} for reason {Reason}", context.Lease.PartitionId, reason);

			// On clean shutdown, flush buffers and checkpoint our latest state.
			if (reason == CloseReason.Shutdown)
			{
				FlushAllBuffers();
				await context.CheckpointAsync();
			}
		}


		/// <summary>
		/// Gets the batch stats log entry from the stats
		/// </summary>
		/// <returns></returns>
		private LogOpenSchema GetBatchStatsLogEntry(BlobContainerSink sink, int partitionId)
		{
			LogOpenSchema ret = new LogOpenSchema();
			ret.Timestamp = DateTime.UtcNow;
			ret.ApplicationName = "EventHubLogProcessor";
			ret.MachineRole = "EventHubLogProcessor";
			ret.Environment = "EventHubLogProcessor";
			ret.Level = "Information";
			ret.MessageId = Guid.NewGuid().ToString();
			ret.MachineName = Environment.MachineName;
			ret.Message = "KustoBatchStats see Blob for details";
			ret.MessageTemplate = ret.Message;
			// build the custom blob
			StringBuilder sb = new StringBuilder("{");
			sb.Append(_batchStats.ToJsonPoperties());
			sb.Append($"\"PartitionId\" : {partitionId},");
			sb.Append($"\"LastBlobSize\" : {sink.BlobSize},");
			sb.Append($"\"LastBlobRawSize\" : {sink.BlobRawSize},");
			sb.Append($"\"LastBlobWriteErrorCount\" : {sink.WriteErrorCount},");
			sb.Append($"\"LastBlobOldestDocTimestamp\" : \"{sink.OldestDoc}\",");
			var lag = DateTime.UtcNow - sink.OldestDoc;
			sb.Append($"\"LastBlobOldestDocLag\" : \"{lag}\",");
			sb.Append($"\"LastBlobOldestDocLagMinutes\" : \"{lag.TotalMinutes}\"");
			sb.Append("}");
			ret.Blob = sb.ToString();
			return ret;
		}

		private DeserializationInfo DeserializeMessage(EventData eventData)
		{
			var sw = Stopwatch.StartNew();
			var type = eventData.GetMessageType();
			Exception serializationException = null;
			BaseOpenSchema deserialized = null;
			try
			{
				switch (type)
				{
					case EventProcessorConstants.SerilogEventType:
						deserialized = GetKustoLog(eventData);
						break;
					case EventProcessorConstants.RoboCustosInteractionEventType:
						deserialized = GetKustoInteraction(eventData);
						break;
				}
			}
			catch (Exception e)
			{
				Log.Error(e, "Error deserializing event data");
				serializationException = e;
			}
			sw.Stop();
			_batchStats.ReportSerialization(sw.ElapsedMilliseconds, serializationException);
			return new DeserializationInfo(deserialized, type, sw.Elapsed, serializationException);
		}

		public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
		{
			int partitionId = Int32.Parse(context.Lease.PartitionId);
			int messageCount = 0;
			bool bufferFlushed = false;
			try
			{
				// these messages will be of different types based on the header information in the event data
				// we need to process them all as part of this request, each type will go to a different blob  as is required by kusto ingestion.

				// loop through all the events and process them depending on the type

				foreach (var evt in messages)
				{
					messageCount++;
					var message = DeserializeMessage(evt);

					if (message.DeserializedEvent != null)
					{
						switch (message.EventType)
						{
							case EventProcessorConstants.SerilogEventType:
								// now we have a kusto log message, we can send
								var logSink = _blobSinksBySchema["Log"];
								var flushed = await logSink.WriteToBlobBuffer(message.DeserializedEvent);
								bufferFlushed |= flushed;
								// for log schema we will add additional stats
								if (flushed && _sendProcessingStats)
								{
									await logSink.WriteToBlobBuffer(GetBatchStatsLogEntry(logSink, partitionId));
									logSink.ResetStats();
								}
								break;
							case EventProcessorConstants.RoboCustosInteractionEventType:
								bufferFlushed |= await _blobSinksBySchema["Interactions"].WriteToBlobBuffer(message.DeserializedEvent);
								break;
							//TODO: maybe deal with these although sending to kusto may not be the right thing here, ignore for now
							case EventProcessorConstants.ExternalTelemetryEventType:
							case EventProcessorConstants.AzureResourcesEventType:
							break;
						}
					}
				}

				if (bufferFlushed || DateTime.UtcNow > _lastCheckpointTime.Add(CheckpointFrequency))
				{
					// at least one buffer was flushed as part of this batch of messages
					// force flush again and checkpoint here to reduce possibility of duplicate messages being sent (kusto has no protection agsint dupes so we
					// want to avoid it as much as we can
					FlushAllBuffers();
					_batchStats.Clear();
					await context.CheckpointAsync();
					_lastCheckpointTime = DateTime.UtcNow;
				}
			}
			catch (Exception e)
			{
				if (IsTaskCanceled(e))
				{
					Log.Information("Task canceled while processing {Count} messages in partition {PartitionId}",
						messageCount, context.Lease.PartitionId);
				}
				else
				{
					Log.Error(e, "Error processing {PartitionId}: {Message}", context.Lease.PartitionId, e.Message);
				}
				throw;
			}
		}

		private LogOpenSchema GetKustoLog(EventData evt)
		{
			LogOpenSchema kustoLog = new LogOpenSchema();
			kustoLog.Timestamp = evt.GetTimestamp();
			kustoLog.MessageId = evt.GetMessageId();
			var doc = JObject.Parse(Encoding.UTF8.GetString(evt.GetBytes()));
			foreach (var prop in doc.Properties())
			{
				switch (prop.Name)
				{
					case "@timestamp":
						kustoLog.Timestamp = prop.Value.Value<DateTime>().ToUniversalTime();
						break;
					case "level":
						kustoLog.Level = prop.Value.Value<string>();
						break;
					case "message":
						kustoLog.Message = prop.Value.Value<string>();
						break;
					case "messageTemplate":
						kustoLog.MessageTemplate = prop.Value.Value<string>();
						break;
					case "fields":
						List<JProperty> blobProps = new List<JProperty>();
						// this is the nested property with custom properties based off string format parameters or enrichers.
						// extract all the info for well known fields and put the rest in the blob
						foreach (var customProp in prop.Value.Children<JProperty>())
						{
							switch (customProp.Name)
							{
								case "Environment":
									kustoLog.Environment = customProp.Value.Value<string>();
									break;
								case "MachineRole":
									kustoLog.MachineRole = customProp.Value.Value<string>();
									kustoLog.ApplicationName = customProp.Value.Value<string>();
									break;
								case "RoboCustosOperationId":
									kustoLog.CorrelationId = customProp.Value.Value<string>();
									break;
								case "MachineName":
									kustoLog.MachineName = customProp.Value.Value<string>();
									break;
								case "MessageId":
									kustoLog.MessageId = customProp.Value.Value<string>();
									break;
								default:
									blobProps.Add(customProp);
									break;
							}
						}
						kustoLog.Blob = JsonConvert.SerializeObject(new JObject(blobProps.ToArray()), Formatting.None);
						break;
				}
			}
			return kustoLog;
		}

		private static InteractionsOpenSchema GetKustoInteraction(EventData evt)
		{
			InteractionsOpenSchema kustoInteraction = new InteractionsOpenSchema();
			kustoInteraction.Timestamp = evt.GetTimestamp();
			kustoInteraction.MessageId = evt.GetMessageId();
			var rawString = Encoding.UTF8.GetString(evt.GetBytes());
			var interactionDoc = JObject.Parse(rawString);
			RoboCustosInteractionFromElasticSearch interaction =
				new RoboCustosInteractionFromElasticSearch("Interaction", (JObject)interactionDoc["Interaction"]);

			// top level, non-interaction information
			kustoInteraction.RobotName = interactionDoc["RobotName"]?.Value<string>();
			kustoInteraction.Environment =
				interactionDoc["Information"]?["Product"]?["Environment"]?.Value<string>();
			// there is no role in the documents so set to hard coded value
			kustoInteraction.MachineRole = "robocustos";
			kustoInteraction.ApplicationName = "robocustos";
			// no machine name either so use instance id
			kustoInteraction.MachineName = interactionDoc["Tester"]?["InstanceId"]?.Value<string>();

			// interaction info
			kustoInteraction.DurationInMs = (int)interaction.TimeTaken.TotalMilliseconds;
			kustoInteraction.Happiness = interaction.HappinessGrade.ToString();
			kustoInteraction.HappinessExplanation = interaction.HappinessExplanation;

			// find the interaction responsible for unhappiness (if unhappy)
			if (interaction.HappinessGrade == InteractionHappinessGrade.Unacceptable ||
			    interaction.HappinessGrade == InteractionHappinessGrade.ReallyAnnoyed)
			{
				var likelyCause = interaction.LikelyRootCauseInteraction;
				if (likelyCause != null)
				{
					// handle different cases of "ID"
					if (likelyCause.HasDetailProperty("OperationID"))
					{
						kustoInteraction.CorrelationId = likelyCause.GetDetailProperty<string>("OperationID");
					}
					else if (likelyCause.HasDetailProperty("OperationId"))
					{
						kustoInteraction.CorrelationId = likelyCause.GetDetailProperty<string>("OperationId");
					}
				}
			}
			kustoInteraction.Blob = rawString;
			return kustoInteraction;
		}

		public void FlushAllBuffers()
		{
			foreach (var sink in _blobSinksBySchema.Values)
			{
				sink.FlushWriteBufferToBlob();
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
	}
}
