using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AzureCAT.Extensions.Logging.AppInsights.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Serilog;

namespace EventHubLogProcessor
{
	/// <summary>
	/// Blob container sink for writing log events of a specific type to blob storage
	/// </summary>
	public class BlobContainerSink
	{
		public long BlobRawSize { get; set; }
		public long BlobSize { get; set; }
		public int WriteErrorCount { get; set; }
		public int CallbackErrorCount { get; set; }
		public DateTime OldestDoc { get; set; }

		private readonly Uri _telemetryServiceEndpoint;
		private readonly string _instrumentationKey;
		private readonly string _baseContainerName;
		private readonly bool _sendProcessingStats;

		private readonly int InitialBackoffDelayInMilliseconds = 100;
		private readonly int MaxBackoffDelayInMilliseconds = 5000;

		// If the Event won't fit in our configurable memory buffer, log something to local logger
		private const int MaxDumpSizeBytes = 1000;

		private readonly SemaphoreSlim _semaphoreMemoryBuffer;
		private readonly MemoryStream _memoryBuffer;

		public bool CompressBlobs { get; set; }

		private const int MaxBlobWriteAttempts = 10;

		// Log number of events processed
		private int _eventCount = 0;
		private long _eventCountTotal = 0;

		private const string NewLine = "\r\n";
		static readonly byte[] NewLineBytes = Encoding.UTF8.GetBytes(NewLine);

		private readonly List<CloudBlobClient> _blobClients;

		private readonly string _schemaName;
		private readonly Guid _schemaId;
		private readonly string _fileType;

		/// <summary>
		/// Class to handle writing blobs for a specific kusto schema.  Code is very similar to the code in the AppInsightsLogging project in deadpool but we have copied here for this poc
		/// ideally we should be able to factor the code so it can be used by both code bases.
		/// </summary>
		/// <param name="blobClients">The BLOB clients.</param>
		/// <param name="schemaName">Name of the schema.</param>
		/// <param name="schemaId">The schema identifier.</param>
		/// <param name="telemetryServiceEndpoint">The telemetry service endpoint.</param>
		/// <param name="instrumentationKey">The instrumentation key.</param>
		/// <param name="baseContainerName">Name of the base container.</param>
		/// <param name="memoryBufferSizeInBytes">The memory buffer size in bytes.</param>
		/// <param name="compressBlobs">if set to <c>true</c> [compress blobs].</param>
		/// <param name="initialBackoffDelayInMilliseconds">The initial backoff delay in milliseconds.</param>
		/// <param name="maxBackoffDelayInMilliseconds">The maximum backoff delay in milliseconds.</param>
		public BlobContainerSink(IEnumerable<CloudBlobClient> blobClients, string schemaName, Guid schemaId, Uri telemetryServiceEndpoint, string instrumentationKey, string baseContainerName, long memoryBufferSizeInBytes, bool compressBlobs = false, int? initialBackoffDelayInMilliseconds = null, int? maxBackoffDelayInMilliseconds = null)
		{
			_blobClients = new List<CloudBlobClient>(blobClients);
			_fileType = "json"; // we only deal with json
			CompressBlobs = compressBlobs;
			if (CompressBlobs)
			{
				_fileType += ".gz";
			}
			_schemaName = schemaName;
			_schemaId = schemaId;
			_telemetryServiceEndpoint = telemetryServiceEndpoint;
			_instrumentationKey = instrumentationKey;
			_baseContainerName = baseContainerName;

			if (initialBackoffDelayInMilliseconds.HasValue)
				InitialBackoffDelayInMilliseconds = initialBackoffDelayInMilliseconds.Value;
			if (maxBackoffDelayInMilliseconds.HasValue)
				MaxBackoffDelayInMilliseconds = maxBackoffDelayInMilliseconds.Value;
		
			// Semaphore for single memory buffer
			_semaphoreMemoryBuffer = new SemaphoreSlim(1, 1);

			var transmitBuffer = new byte[memoryBufferSizeInBytes];
			_memoryBuffer = new MemoryStream(transmitBuffer);

			OldestDoc = DateTime.MaxValue;
		}

		protected virtual void FlushBuffer(object state)
		{
			FlushWriteBufferToBlob();
		}

		/// <summary>
		/// writes the object to the blob, returning true if the buffer was flushed
		/// </summary>
		/// <param name="o"></param>
		/// <returns></returns>
		public async Task<bool> WriteToBlobBuffer(IBaseOpenSchema o)
		{
			// track oldest timestamp
			if (o.Timestamp < OldestDoc)
			{
				OldestDoc = o.Timestamp;
			}
			return await WriteToBlobBuffer(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(o)));
		}

		/// <summary>
		/// writes the object to the blob, returning true if the buffer was flushed
		/// </summary>
		/// <param name="byteEvent"></param>
		/// <returns></returns>
		public async Task<bool> WriteToBlobBuffer(byte[] byteEvent)
		{
			try
			{
				await _semaphoreMemoryBuffer.WaitAsync();
				try
				{
					// Is the Event bigger than the Memory Buffer?
					if (byteEvent.Length > _memoryBuffer.Capacity)
					{
						string snippet = Encoding.UTF8.GetString(byteEvent, 0,
							(byteEvent.Length > MaxDumpSizeBytes ? MaxDumpSizeBytes : byteEvent.Length));
						Log.Error("BlobContainerSink.WriteToBlobBuffer Event too large, dropping Event. Event={LargeEventSize} bytes, Memory Buffer={MemoryBufferSize} bytes {snippet}",
							byteEvent.Length,
							_memoryBuffer.Capacity,
							snippet);
						return false;
					}

					if ((_memoryBuffer.Capacity - _memoryBuffer.Position) > (byteEvent.Length + NewLineBytes.Length))
					{
						// NewLine for JSON or CSV format
						if (_memoryBuffer.Position != 0)
							_memoryBuffer.Write(NewLineBytes, 0, NewLineBytes.Length);

						_memoryBuffer.Write(byteEvent, 0, byteEvent.Length);
						_eventCount++;
						_eventCountTotal++;
					}
					else
					{
						// Flush the buffer and clear it (default) for re-use
						// TODO - more advanced version and iterate through a pool of buffers
						await WriteMemoryBuffer(_memoryBuffer);
						_memoryBuffer.Write(byteEvent, 0, byteEvent.Length);
						_eventCount++;
						_eventCountTotal++;
						return true; // we flushed the buffer
					}
				}
				finally
				{
					_semaphoreMemoryBuffer.Release();
				}
			}
			catch (Exception ex)
			{
				Log.Error(ex, "BlobContainerSink.WriteToBlobBuffer exception publishing to blob storage");
				throw ex;
			}
			return false;
		}

		public void ResetStats()
		{
			// set to time in the future
			OldestDoc = DateTime.MaxValue;
			// reset couneters etc stats
			BlobRawSize = 0;
			BlobSize = 0;
			WriteErrorCount = 0;
			CallbackErrorCount = 0;
		}

		public void FlushWriteBufferToBlob()
		{
			try
			{
				_semaphoreMemoryBuffer.Wait();
				try
				{
					// If we didn't write buffer to the file but have something in the memory buffer that is not written yet, write it
					if (_memoryBuffer.Position > 0)
					{
						Log.Information(
							"BlobContainerSink.FlushWriteBufferToBlobposition {Position} bytes, containing {EventCount} events, total {EventCountTotal}",
							_memoryBuffer.Position, _eventCount, _eventCountTotal);

						// Flush the buffer and clear it (default) for re-use
						// TODO - more advanced version and iterate through a pool of buffers
						WriteMemoryBuffer(_memoryBuffer).Wait();
					}
				}
				finally
				{
					_semaphoreMemoryBuffer.Release();
				}
			}
			catch (Exception ex)
			{
				Log.Error(ex, "BlobContainerSink.FlushWriteBufferToBlob exception flushing to blob storage");
				throw ex;
			}
		}

		protected virtual async Task WriteMemoryBuffer(MemoryStream memoryStream, bool resetBuffer = true)
		{
			try
			{
				// Prepare memoryStream for Compression
				memoryStream.SetLength(memoryStream.Position);
				memoryStream.Position = 0;
				BlobRawSize = memoryStream.Length;
				BlobSize = memoryStream.Length;

				// Compress?
				if (CompressBlobs)
				{
					// Compress memory buffer
					using (var compressed = new MemoryStream())
					{
						using (GZipStream gz = new GZipStream(compressed, CompressionLevel.Fastest, true))
						{
							await memoryStream.CopyToAsync(gz);
						}
						compressed.Position = 0;
						BlobSize = compressed.Length;
						await WriteBuffer(compressed, 0, compressed.Length).ConfigureAwait(false);
					}
				}
				else
				{
					// No Compression
					await WriteBuffer(memoryStream, 0, memoryStream.Length).ConfigureAwait(false);
				}

				// Reset Memory Buffer
				if (resetBuffer)
				{
					memoryStream.SetLength(0);
					memoryStream.Position = 0;
				}
			}
			catch (Exception ex)
			{
				Log.Error(ex, "BlobContainerSink.WriteMemoryBuffer Exception writing memory buffer");
				throw;
			}
		}

		protected virtual async Task WriteBuffer(Stream buffer, int offset, long length)
		{
			try
			{
				var blobPath = GetBlobName(_schemaName, _fileType);
				var container = GetContainerReference();
				var blobReference = container.GetBlockBlobReference(blobPath);

				// None of the stuff above might exist, rather than calling CreateIfNotExists every time, we will optimize for
				// perf and not call it unless we get an error
				int attempts = 0;
				while (true)
				{
					try
					{
						// Set buffer position before writing
						buffer.Position = offset;
						// Upload to blob storage
						await blobReference.UploadFromStreamAsync(buffer, length).ConfigureAwait(false);

						// call the kusto notifcation function (do some retries on errors)
						int callbackAttempts = 0;
						int backoffDelayInMilliseconds = 100;
						while (true)
						{
							try
							{
								CallbackErrorCount++;
								callbackAttempts++;
								await
									OpenSchemaCallback.PostCallback(blob: blobReference, endpoint: _telemetryServiceEndpoint,
										schemaName: _schemaId.ToString(), iKey: _instrumentationKey);
								break;
							}
							catch (Exception e)
							{
								Log.Error(e, "Exception encountered when calling kusto endpoint, attempt {CallbackAttempt}", callbackAttempts);
								if (callbackAttempts > MaxBlobWriteAttempts)
								{
									throw;
								}
								backoffDelayInMilliseconds = Math.Min(backoffDelayInMilliseconds*2, MaxBackoffDelayInMilliseconds);
								await Task.Delay(backoffDelayInMilliseconds);
							}
						}

						Log.Information(
							"BlobContainerSink.WriteBuffer blob wrote {BlobSizeInBytes} bytes {GZip}, containing {EventCount} events, total {EventCountTotal}",
							length, CompressBlobs ? "UseGZip" : "NoGZip", _eventCount, _eventCountTotal);
						break;
					}
					catch (Exception e)
					{
						WriteErrorCount++;

						if (attempts++ > MaxBlobWriteAttempts)
						{
							Log.Error(e, "BlobContainerSink.WriteBuffer Exception writing blob retries exhausted");
							// we are done retrying
							throw;
						}

						Log.Error(e, "BlobContainerSink.WriteBuffer Exception writing blob, retrying");
						var tryAnotherAccount = true;
						var storageException = e as StorageException;
						if (storageException != null)
						{
							if (storageException.RequestInformation.HttpStatusCode == 404)
							{
								// not found so we need to create it
								try
								{
									var created = await container.CreateIfNotExistsAsync();
									Log.Information(
										"Container not found, attempt to create resulted {ContainerCreationResult}", created);
									// we succeeeded to don't try a new account
									tryAnotherAccount = false;
								}
								catch (Exception ex)
								{
									Log.Error(ex, "Exception creating container");
									tryAnotherAccount = true;
								}
							}
						}
						if (tryAnotherAccount)
						{
							// some other kind of error not related to not found so try another random container
							container = GetContainerReference();
							blobReference = container.GetBlockBlobReference(blobPath);
						}
					}
				}
			}
			catch (Exception ex)
			{
				Log.Error(ex, "BlobContainerSink.WriteBuffer Exception writing blob");
				throw;
			}
			finally
			{
				_eventCount = 0;
			}
		}

		/// <summary>
		/// Gets the container reference (this may or may not exist at the time of calling)
		/// </summary>
		/// <returns></returns>
		private CloudBlobContainer GetContainerReference()
		{
			// Choose a random container from the list
			Random r = new Random();
			var account = _blobClients[r.Next(_blobClients.Count)];
			var containerName = GetContainerName();
			return account.GetContainerReference(containerName);
		}

		/// <summary>
		/// Gets a deterministic container name based on the current hour of the day (UTC).
		/// </summary>
		/// <returns></returns>
		private string GetContainerName()
		{
			var date = DateTime.UtcNow.ToString("yyyy-MM-dd-HH");
			return $"{GetHashPrefix(date, 5)}-{_baseContainerName}-{date}";
		}

		private static string GetHashPrefix(string input, int prefixLength)
		{
			var md5 = System.Security.Cryptography.MD5.Create();
			byte[] hash = md5.ComputeHash(Encoding.UTF8.GetBytes(input));
			StringBuilder sb = new StringBuilder(prefixLength);
			if (prefixLength > hash.Length)
			{
				throw new ArgumentException("prefix length too long", nameof(prefixLength));
			}
			for (int i = 0; i < prefixLength; i++)
			{
				sb.Append(hash[i].ToString("x2"));
			}
			return sb.ToString();

		}

		protected virtual string GetBlobName(string dataSourceName, string fileTypeName)
		{
			// Use a Guid up front to improve load balancing
			return $"{Guid.NewGuid()}_{DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss")}_{dataSourceName}.{fileTypeName}";
		}

		public void Dispose()
		{
			// Anything in the buffer, write it!
			FlushWriteBufferToBlob();

			_semaphoreMemoryBuffer?.Wait();
			try
			{
				_memoryBuffer?.Dispose();
			}
			finally
			{
				_semaphoreMemoryBuffer?.Release();
			}

			_semaphoreMemoryBuffer?.Dispose();
		}
	}
}
