using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace EventHubLogProcessor
{
	public static class EventDataExtensions
	{
		public static string GetMessageType(this EventData eventData)
		{
			if (eventData == null)
			{
				throw new ArgumentNullException(nameof(eventData));
			}
			object messageTypeString;
			if (eventData.Properties.TryGetValue("Type", out messageTypeString))
			{
				if (!(messageTypeString is string))
					throw new InvalidEventDataException($"EventData property 'Type' is not a string: {messageTypeString?.GetType().FullName}");

				return (string)messageTypeString;
			}

			return string.Empty;
		}

		public static string GetMessageId(this EventData eventData)
		{
			if (eventData == null)
			{
				throw new ArgumentNullException(nameof(eventData));
			}
			object messageIdString;
			if (eventData.Properties.TryGetValue("MessageId", out messageIdString))
			{
				if (!(messageIdString is string))
					throw new InvalidEventDataException($"EventData property 'MessageId' is not a string: {messageIdString?.GetType().FullName}");

				return (string)messageIdString;
			}

			return Guid.NewGuid().ToString();
		}

		public static DateTime GetTimestamp(this EventData eventData)
		{
			if (eventData == null)
			{
				throw new ArgumentNullException(nameof(eventData));
			}
			object timestampString;
			if (eventData.Properties.TryGetValue("Timestamp", out timestampString))
			{
				if (!(timestampString is string))
					throw new InvalidEventDataException($"EventData property 'Timestamp' is not a string: {timestampString?.GetType().FullName}");

				DateTime timestamp;
				if (!DateTime.TryParse((string)timestampString, CultureInfo.InvariantCulture, DateTimeStyles.None, out timestamp))
					throw new InvalidEventDataException($"EventData property 'Timestamp' is not formatted as a DateTime: {(string)timestampString}");

				return timestamp;
			}

			return DateTime.UtcNow;
		}

		public static string GetSource(this EventData eventData)
		{
			if (eventData == null)
			{
				throw new ArgumentNullException(nameof(eventData));
			}
			object sourceString;
			if (eventData.Properties.TryGetValue("Source", out sourceString))
			{
				if (!(sourceString is string))
					throw new InvalidEventDataException($"EventData property 'Source' is not a string: {sourceString?.GetType().FullName}");

				return (string)sourceString;
			}

			return null;
		}
	}
}
