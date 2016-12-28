using System;
using System.Globalization;
using System.Text;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace EventHubLogProcessor
{
	public sealed class BulkItem
	{
		/// <summary>
		/// The maximum number of characters to include when displaying the partial start of the DocumentBody (e.g. on abandoned documents).
		/// </summary>
		private const int DocumentBodyStartMaxLength = 1024;

		/// <summary>
		/// Create a valid BulkItem with the given properties.
		/// </summary>
		public BulkItem(string indexBaseName, DateTime timestamp, DateTime enqueueTime, string documentType, string documentId, string documentBody)
			: this(indexBaseName: indexBaseName,
					indexName: GetTimeBasedIndex(indexBaseName, timestamp),
					timestamp: timestamp,
					enqueueTime: enqueueTime,
					documentType: documentType,
					documentId: documentId,
					documentBody: documentBody,
					isValid: true,
					invalidReason: string.Empty)
		{
		}

		/// <summary>
		/// Create a BulkItem with the given properties.
		/// </summary>
		private BulkItem(string indexBaseName, string indexName, DateTime timestamp, DateTime enqueueTime, string documentType, string documentId, string documentBody, bool isValid = true, string invalidReason = null)
		{
			IndexBaseName = indexBaseName;
			IndexName = indexName;
			Timestamp = timestamp;
			EnqueueTime = enqueueTime;
			DocumentType = documentType;
			DocumentId = documentId;
			DocumentBody = documentBody;
			IsValid = isValid;
			InvalidReason = invalidReason ?? string.Empty;

			if (isValid)
			{
				// Simple (fast) validation to ensure the document body can be sent to ElasticSearch.
				// Newlines in the document body will cause the entire bulk upload to fail.
				if (documentBody.Contains("\n"))
				{
					IsValid = false;
					InvalidReason = "Document body contains newlines.";
				}
			}
		}

		/// <summary>
		/// Create an invalid BulkItem that will be immediately abandoned (due to missing properties, e.g. unknown document type).
		/// </summary>
		private BulkItem(DateTime timestamp, DateTime enqueueTime, string documentId, string documentBody, string invalidReason)
			: this(indexBaseName: string.Empty,
					indexName: string.Empty,
					timestamp: timestamp,
					enqueueTime: enqueueTime,
					documentType: string.Empty,
					documentId: documentId,
					documentBody: documentBody,
					isValid: false,
					invalidReason: invalidReason)
		{
		}

		public bool IsValid { get; }
		public string InvalidReason { get; }

		public string IndexBaseName { get; }
		public string IndexName { get; }
		public DateTime EnqueueTime { get; }
		public DateTime Timestamp { get; }
		public string DocumentType { get; }
		public string DocumentId { get; }
		public string DocumentBody { get; }

		public int BodyLength => DocumentBody.Length;
		public string DocumentBodyStart => DocumentBody.Substring(0, Math.Min(DocumentBodyStartMaxLength, DocumentBody.Length));

		public string ToBulkString()
		{
			return string.Format("{{\"index\":{{\"_index\":\"{0}\",\"_type\":\"{1}\",\"_id\":\"{2}\"}}}}\n{3}",
				IndexName, DocumentType, DocumentId, DocumentBody);
		}

		/// <summary>
		/// Creates a BulkItem from EventHub <see cref="EventData"/>.
		/// </summary>
		public static BulkItem FromEventData(EventData eventData)
		{
			DateTime timestamp = default(DateTime);
			string messageId = string.Empty;
			string documentBody = string.Empty;

			try
			{
				var eventType = eventData.GetMessageType();
				messageId = eventData.GetMessageId();
				timestamp = eventData.GetTimestamp();
				documentBody = Encoding.UTF8.GetString(eventData.GetBytes());
				string indexBaseName = null, indexName = null, documentType = null;

				switch (eventType)
				{
					case EventProcessorConstants.SerilogEventType:
						indexBaseName = "logstash";
						indexName = GetTimeBasedIndex(indexBaseName, timestamp);
						documentType = "logevent";
						break;

					case EventProcessorConstants.RoboCustosInteractionEventType:
						indexBaseName = "robointeractions";
						indexName = GetTimeBasedIndex(indexBaseName, timestamp);
						documentType = "interaction";
						break;

					case EventProcessorConstants.ExternalTelemetryEventType:
						indexBaseName = "externaltelemetry";
						indexName = GetTimeBasedIndex(indexBaseName, timestamp);
						documentType = eventData.GetSource() ?? "telemetryevent";
						break;

					case EventProcessorConstants.AzureResourcesEventType:
						indexBaseName = "azure-resources";
						indexName = "azure-resources";
						documentType = eventData.GetSource() ?? "metadata";
						break;

					default:
						if (!TryInferBulkItemType(documentBody, ref timestamp, ref indexBaseName, ref indexName, ref documentType))
						{
							// Invalid bulk item.
							return new BulkItem(timestamp, eventData.EnqueuedTimeUtc, messageId, documentBody, invalidReason: $"Missing or invalid Type: '{eventType}'.");
						}
						break;
				}

				// Valid bulk item.
				return new BulkItem(
					indexBaseName: indexBaseName,
					indexName: indexName,
					timestamp: timestamp,
					enqueueTime: eventData.EnqueuedTimeUtc,
					documentType: documentType,
					documentId: messageId,
					documentBody: documentBody);
			}
			catch (InvalidEventDataException e)
			{
				// Invalid bulk item.
				return new BulkItem(timestamp, eventData.EnqueuedTimeUtc, messageId, documentBody, invalidReason: e.Message);
			}
			catch (Exception e)
			{
				// Invalid bulk item.
				return new BulkItem(timestamp, eventData.EnqueuedTimeUtc, messageId, documentBody, invalidReason: $"Unknown exception parsing EventData: {e.Message}");
			}
		}

		private static bool TryInferBulkItemType(string documentBody, ref DateTime timestamp, ref string indexBaseName, ref string indexName, ref string documentType)
		{
			// Infer logstash message types if it contains known properties.
			var options = new JsonSerializerSettings { DateParseHandling = DateParseHandling.None };
			var content = JsonConvert.DeserializeObject<JObject>(documentBody, options);
			var message = content.Property("message")?.Value.Value<string>();
			var messageTemplate = content.Property("messageTemplate")?.Value.Value<string>();
			var timestampString = content.Property("@timestamp")?.Value.Value<string>();

			if (message != null && messageTemplate != null && timestampString != null)
			{
				if (!DateTime.TryParse(timestampString, CultureInfo.InvariantCulture, DateTimeStyles.None, out timestamp))
					throw new InvalidEventDataException($"EventData field '@timestamp' is not formatted as a DateTime: {timestampString}");

				indexBaseName = "logstash";
				indexName = GetTimeBasedIndex(indexBaseName, timestamp);
				documentType = "logevent";

				return true;
			}

			return false;
		}

		private static string GetTimeBasedIndex(string indexBaseName, DateTime timestamp)
		{
			return $"{indexBaseName}-{timestamp:yyyy.MM.dd}";
		}

		
	}
}
