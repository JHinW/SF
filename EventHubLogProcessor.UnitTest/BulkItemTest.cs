using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using Xunit;

namespace EventHubLogProcessor.UnitTest
{
	public class BulkItemTest
	{
		[Fact]
		public void PropertiesTest()
		{
			var indexBaseName = "logstash";
			var timestamp = DateTime.UtcNow;
			var enqueueTime = timestamp.Subtract(TimeSpan.FromSeconds(1));
			var documentType = "logevent";
			var documentId = Guid.NewGuid().ToString();
			var documentBody = "{ \"message\": \"log message\", \"data\": 17 }";

			var item = new BulkItem(indexBaseName, timestamp, enqueueTime, documentType, documentId, documentBody);

			Assert.True(item.IsValid);
			Assert.Equal(indexBaseName, item.IndexBaseName);
			Assert.NotEqual(indexBaseName, item.IndexName);
			Assert.Equal(timestamp, item.Timestamp);
			Assert.Equal(enqueueTime, item.EnqueueTime);
			Assert.Equal(documentType, item.DocumentType);
			Assert.Equal(documentType, item.DocumentType);
			Assert.Equal(documentId, item.DocumentId);
			Assert.Equal(documentBody, item.DocumentBody);
			Assert.Equal(documentBody.Length, item.BodyLength);
			Assert.Equal(documentBody, item.DocumentBodyStart); // Since document body is short, these should match.
		}

		[Fact]
		public void FromEventData_NoType_InvalidBulkItem()
		{
			var data = new EventData();

			// Default constructed EventData will not have the Type property set, so should fail.
			var item = BulkItem.FromEventData(data);
			Assert.False(item.IsValid, "BulkItem with no 'Type' should be invalid.");
			Assert.False(string.IsNullOrEmpty(item.InvalidReason), "BulkItem with no 'Type' should have an invalid reason.");
		}

		[Fact]
		public void FromEventData_UnknownType_InvalidBulkItem()
		{
			var data = new EventData();
			data.Properties["Type"] = "invalid";

			var item = BulkItem.FromEventData(data);
			Assert.False(item.IsValid, "BulkItem with unknown 'Type' should be invalid.");
			Assert.False(string.IsNullOrEmpty(item.InvalidReason), "BulkItem with unknown 'Type' should have an invalid reason.");
		}

		[Fact]
		public void FromEventData_InvalidType_InvalidBulkItem()
		{
			var data = new EventData();
			data.Properties["Type"] = 17;

			var item = BulkItem.FromEventData(data);
			Assert.False(item.IsValid, "BulkItem with invalid 'Type' should be invalid.");
			Assert.False(string.IsNullOrEmpty(item.InvalidReason), "BulkItem with invalid 'Type' should have an invalid reason.");
		}

		[Fact]
		public void FromEventData_NoType_InferSerilogEvent()
		{
			DateTime timestamp = DateTime.Parse("2016-11-02T20:15:21.504Z");
			var body = "{ \"@timestamp\": \"2016-11-02T20:15:21.504Z\", \"message\": \"Elapsed time: 100 ms\", \"messageTemplate\": \"Elapsed time: {ElapsedTime} ms\", \"fields.ElapsedTime\": 100 }";
			var data = new EventData(Encoding.UTF8.GetBytes(body));

			// Default constructed EventData will not have the Type property set, but should infer logstash event from the properties.
			var item = BulkItem.FromEventData(data);
			Assert.True(item.IsValid, "BulkItem with known logstash properties should be inferred.");
			Assert.Equal("logstash", item.IndexBaseName);
			Assert.Equal("logevent", item.DocumentType);
			Assert.False(string.IsNullOrEmpty(item.DocumentId));
			Assert.True(ApproximatelyEqual(timestamp, item.Timestamp)); // Timestamp should be parsed from the message body.
		}

		[Fact]
		public void FromEventData_SerilogEvent_NoMessageId_NoTimestamp()
		{
			var data = new EventData();
			data.Properties["Type"] = "SerilogEvent";

			var item = BulkItem.FromEventData(data);

			Assert.True(item.IsValid);
			Assert.Equal("logstash", item.IndexBaseName);
			Assert.Equal("logevent", item.DocumentType);
			Assert.False(string.IsNullOrEmpty(item.DocumentId));
			Assert.True(ApproximatelyEqual(DateTime.UtcNow, item.Timestamp)); // Timestamp should get generated based on current time.
		}

		[Fact]
		public void FromEventData_RoboCustosInteraction_NoMessageId_NoTimestamp()
		{
			var data = new EventData();
			data.Properties["Type"] = "RoboCustosInteraction";

			var item = BulkItem.FromEventData(data);

			Assert.True(item.IsValid);
			Assert.Equal("robointeractions", item.IndexBaseName);
			Assert.Equal("interaction", item.DocumentType);
			Assert.False(string.IsNullOrEmpty(item.DocumentId));
			Assert.True(ApproximatelyEqual(DateTime.UtcNow, item.Timestamp)); // Timestamp should get generated based on current time.
		}

		[Fact]
		public void FromEventData_ExternalTelemetryEvent_NoSource_NoMessageId_NoTimestamp()
		{
			var data = new EventData();
			data.Properties["Type"] = "ExternalTelemetry";

			var item = BulkItem.FromEventData(data);

			Assert.True(item.IsValid);
			Assert.Equal("externaltelemetry", item.IndexBaseName);
			Assert.Equal("telemetryevent", item.DocumentType); // No 'Source' should provide a default document type.
			Assert.False(string.IsNullOrEmpty(item.DocumentId));
			Assert.True(ApproximatelyEqual(DateTime.UtcNow, item.Timestamp)); // Timestamp should get generated based on current time.
		}

		[Fact]
		public void FromEventData_AzureResources_NoSource_NoMessageId_NoTimestamp()
		{
			var data = new EventData();
			data.Properties["Type"] = "azure-resources";

			var item = BulkItem.FromEventData(data);

			Assert.True(item.IsValid);
			Assert.Equal("azure-resources", item.IndexBaseName);
			Assert.Equal("azure-resources", item.IndexName);
			Assert.Equal("metadata", item.DocumentType); // No 'Source' should provide a default document type.
			Assert.False(string.IsNullOrEmpty(item.DocumentId));
			Assert.True(ApproximatelyEqual(DateTime.UtcNow, item.Timestamp)); // Timestamp should get generated based on current time.
		}

		[Fact]
		public void FromEventData_SerilogEvent_WithProperties()
		{
			var messageId = Guid.NewGuid().ToString();
			var timestamp = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);

			var data = new EventData();
			data.Properties["Type"] = "SerilogEvent";
			data.Properties["MessageId"] = messageId;
			data.Properties["Timestamp"] = timestamp;

			var item = BulkItem.FromEventData(data);

			Assert.True(item.IsValid);
			Assert.Equal("logstash", item.IndexBaseName);
			Assert.Equal("logevent", item.DocumentType);
			Assert.Equal(messageId, item.DocumentId);
			Assert.Equal(timestamp, item.Timestamp.ToString(CultureInfo.InvariantCulture));
		}

		[Fact]
		public void FromEventData_RoboCustosInteraction_WithProperties()
		{
			var messageId = Guid.NewGuid().ToString();
			var timestamp = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);

			var data = new EventData();
			data.Properties["Type"] = "RoboCustosInteraction";
			data.Properties["MessageId"] = messageId;
			data.Properties["Timestamp"] = timestamp;

			var item = BulkItem.FromEventData(data);

			Assert.True(item.IsValid);
			Assert.Equal("robointeractions", item.IndexBaseName);
			Assert.Equal("interaction", item.DocumentType);
			Assert.Equal(messageId, item.DocumentId);
			Assert.Equal(timestamp, item.Timestamp.ToString(CultureInfo.InvariantCulture));
		}

		[Fact]
		public void FromEventData_ExternalTelemetryEvent_WithProperties()
		{
			var source = "vmrestarts";
			var messageId = Guid.NewGuid().ToString();
			var timestamp = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);

			var data = new EventData();
			data.Properties["Type"] = "ExternalTelemetry";
			data.Properties["Source"] = source;
			data.Properties["MessageId"] = messageId;
			data.Properties["Timestamp"] = timestamp;

			var item = BulkItem.FromEventData(data);

			Assert.True(item.IsValid);
			Assert.Equal("externaltelemetry", item.IndexBaseName);
			Assert.Equal(source, item.DocumentType);
			Assert.Equal(messageId, item.DocumentId);
			Assert.Equal(timestamp, item.Timestamp.ToString(CultureInfo.InvariantCulture));
		}

		[Fact]
		public void FromEventData_ExternalTelemetryEvent_WithProperties_InvalidSource()
		{
			var data = new EventData();
			data.Properties["Type"] = "ExternalTelemetry";
			data.Properties["Source"] = 17; // Incorrect type - should be string.
			data.Properties["MessageId"] = Guid.NewGuid().ToString();
			data.Properties["Timestamp"] = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);

			var item = BulkItem.FromEventData(data);
			Assert.False(item.IsValid, "BulkItem with invalid 'Source' should be invalid.");
			Assert.False(string.IsNullOrEmpty(item.InvalidReason), "BulkItem with invalid 'Source' should have an invalid reason.");
		}

		[Fact]
		public void FromEventData_ExternalTelemetryEvent_InvalidMessageId()
		{
			var data = new EventData();
			data.Properties["Type"] = "ExternalTelemetry";
			data.Properties["Source"] = "vmrestarts";
			data.Properties["MessageId"] = Guid.NewGuid(); // Incorrect type - should be string.
			data.Properties["Timestamp"] = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);

			var item = BulkItem.FromEventData(data);
			Assert.False(item.IsValid, "BulkItem with invalid 'MessageId' should be invalid.");
			Assert.False(string.IsNullOrEmpty(item.InvalidReason), "BulkItem with invalid 'MessageId' should have an invalid reason.");
		}

		[Fact]
		public void FromEventData_ExternalTelemetryEvent_InvalidTimestamp()
		{
			var data = new EventData();
			data.Properties["Type"] = "ExternalTelemetry";
			data.Properties["Source"] = "vmrestarts";
			data.Properties["MessageId"] = Guid.NewGuid().ToString();
			data.Properties["Timestamp"] = DateTime.UtcNow; // Incorrect type - should be string.

			var item = BulkItem.FromEventData(data);
			Assert.False(item.IsValid, "BulkItem with invalid 'Timestamp' should be invalid.");
			Assert.False(string.IsNullOrEmpty(item.InvalidReason), "BulkItem with invalid 'Timestamp' should have an invalid reason.");
		}

		[Fact]
		public void FromEventData_AzureResources_WithProperties()
		{
			var source = "Microsoft.Compute/availabilitySets";
			var messageId = Guid.NewGuid().ToString();
			var timestamp = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);

			var data = new EventData();
			data.Properties["Type"] = "azure-resources";
			data.Properties["Source"] = source;
			data.Properties["MessageId"] = messageId;
			data.Properties["Timestamp"] = timestamp;

			var item = BulkItem.FromEventData(data);

			Assert.True(item.IsValid);
			Assert.Equal("azure-resources", item.IndexBaseName);
			Assert.Equal("azure-resources", item.IndexName);
			Assert.Equal(source, item.DocumentType);
			Assert.Equal(messageId, item.DocumentId);
			Assert.Equal(timestamp, item.Timestamp.ToString(CultureInfo.InvariantCulture));
		}

		[Fact]
		public void ToBulkString_EmptyContent_OneNewline()
		{
			var data = MockEventData.CreateSerilogEventData(body: "");
			var item = BulkItem.FromEventData(data);

			var bulkBody = item.ToBulkString();

			// BulkString should have exactly one newline when formatted correctly.
			Assert.Equal(1, bulkBody.Count(c => c == '\n'));
		}

		[Fact]
		public void ToBulkString_SimpleContent_OneNewline()
		{
			var data = MockEventData.CreateSerilogEventData(body: "{ \"message\": \"log message\" }");
			var item = BulkItem.FromEventData(data);

			var bulkBody = item.ToBulkString();

			// BulkString should have exactly one newline when formatted correctly.
			Assert.Equal(1, bulkBody.Count(c => c == '\n'));
		}

		[Fact]
		public void ToBulkString_ComplexContent_OneNewline()
		{
			var data = MockEventData.CreateSerilogEventData(body: JsonConvert.SerializeObject(new { message = "log message\n with newline" }));
			var item = BulkItem.FromEventData(data);

			var bulkBody = item.ToBulkString();

			// BulkString should have exactly one newline when formatted correctly.
			Assert.True(item.IsValid, "BulkItem with json encoded newlines should be valid.");
			Assert.Equal(1, bulkBody.Count(c => c == '\n'));
		}

		[Fact]
		public void ToBulkString_MultilineContent_MultipleNewlines()
		{
			var data = MockEventData.CreateSerilogEventData(body: "{ \"message\": \"log message\n with newline\" }");
			var item = BulkItem.FromEventData(data);

			var bulkBody = item.ToBulkString();

			Assert.False(item.IsValid, "BulkItem with document body containing newlines should be invalid.");
			Assert.Equal(2, bulkBody.Count(c => c == '\n'));
		}

		private static bool ApproximatelyEqual(DateTime expected, DateTime actual)
		{
			var skew = TimeSpan.FromMinutes(1);
			return expected.Subtract(skew) < actual && actual < expected.Add(skew);
		}
	}
}
