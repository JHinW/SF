using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Moq;
using Nest;
using Newtonsoft.Json;

namespace EventHubLogProcessor.UnitTest
{
	/// <summary>
	/// Mock IElasticClient that supports being able to simulate ElasticSearch for unit tests.
	/// </summary>
	public class MockElasticClient : Mock<IElasticClient>
	{
		public MockElasticLowLevelClient MockLowLevelClient { get; }

		public MockElasticClient(MockBehavior behavior) : base(behavior)
		{
			MockLowLevelClient = new MockElasticLowLevelClient(behavior);
			this.SetupGet(c => c.LowLevel).Returns(MockLowLevelClient.Object);
		}

		public void SimulateElasticSearch()
		{
			MockLowLevelClient.SimulateElasticSearch();
		}

		public int GetTotalItems()
		{
			return MockLowLevelClient.GetTotalItems();
		}

		public int GetTotalItemsInIndex(string indexName)
		{
			return MockLowLevelClient.GetTotalItemsInIndex(indexName);
		}
	}

	/// <summary>
	/// Mock IElasticLowLevelClient that supports being able to simulate ElasticSearch for unit tests.
	/// </summary>
	public class MockElasticLowLevelClient : Mock<IElasticLowLevelClient>
	{
		/// <summary>
		/// Simulated ElasticSearch.  (index -> (type -> (id -> data)))
		/// </summary>
		private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<string, string>>> _data =
			new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<string, string>>>();

		private readonly ConnectionSettings _settings = new ConnectionSettings();

		public MockElasticLowLevelClient(MockBehavior behavior) : base(behavior)
		{
		}

		public void SimulateElasticSearch()
		{
			this.Setup(c => c.BulkAsync<BulkResponse>(It.IsAny<PostData<object>>(), It.IsAny<Func<BulkRequestParameters, BulkRequestParameters>>()))
				.Returns((PostData<object> data, Func<BulkRequestParameters, BulkRequestParameters> param) => OnBulkAsync(data, param));
		}

		/// <summary>
		/// Get the total number items indexed across all indexes.
		/// </summary>
		public int GetTotalItems()
		{
			return CountItems(indexFilter: index => true, typeFilter: type => true, idFilter: id => true);
		}

		/// <summary>
		/// Get the total number of items indexed in the given index.
		/// </summary>
		public int GetTotalItemsInIndex(string indexName)
		{
			return CountItems(indexFilter: index => index.StartsWith(indexName), typeFilter: type => true, idFilter: id => true);
		}

		private Task<ElasticsearchResponse<BulkResponse>> OnBulkAsync(PostData<object> data, Func<BulkRequestParameters, BulkRequestParameters> param)
		{
			using (var stream = new MemoryStream())
			{
				data.Write(stream, _settings);
				string content = Encoding.UTF8.GetString(stream.GetBuffer(), 0, checked((int)stream.Length));
				return Task.FromResult(HandleBulkRequest(content));
			}
		}

		private ElasticsearchResponse<BulkResponse> HandleBulkRequest(string content)
		{
			if (string.IsNullOrWhiteSpace(content))
				return ElasticsearchResponseExtensions.ParseException();

			var lines = content.Split('\n');
			IndexCommand current = null;

			dynamic responseBuilder = ElasticsearchResponseExtensions.GetBulkResponseBuilder();
			var itemsToIndex = new Dictionary<string, Dictionary<string, Dictionary<string, string>>>();
			foreach (var line in lines)
			{
				if (current == null)
				{
					if (string.IsNullOrWhiteSpace(line))
						continue;

					// Validate the action metadata line.
					current = IndexCommand.Parse(line);
					if (current == null)
						return ElasticsearchResponseExtensions.JsonParseException();
					if (current.index == null)
						return ElasticsearchResponseExtensions.MalformedActionException();
					if (string.IsNullOrWhiteSpace(current.index._index))
						return ElasticsearchResponseExtensions.ActionIndexMissingException();
					if (string.IsNullOrWhiteSpace(current.index._type))
						return ElasticsearchResponseExtensions.ActionTypeMissingException();
				}
				else
				{
					if (string.IsNullOrWhiteSpace(line))
					{
						ElasticsearchResponseExtensions.AddEmptyItem(responseBuilder, current.index._index, current.index._type, current.index._id);
					}
					else if (!IsValidJson(line))
					{
						ElasticsearchResponseExtensions.AddInvalidJsonItem(responseBuilder, current.index._index, current.index._type, current.index._id);
					}
					else
					{
						ElasticsearchResponseExtensions.AddBulkItem(responseBuilder, current.index._index, current.index._type, current.index._id);
						TrackItemToIndex(itemsToIndex, current.index._index, current.index._type, current.index._id, line);
					}

					current = null;
				}
			}

			if (responseBuilder.items.Count == 0)
				return ElasticsearchResponseExtensions.ParseException();

			IndexItems(itemsToIndex);
			return ElasticsearchResponseExtensions.GetBulkResponse(HttpStatusCode.OK, responseBuilder);
		}

		private void IndexItems(Dictionary<string, Dictionary<string, Dictionary<string, string>>> newData)
		{
			foreach (var newIndexData in newData)
			{
				string index = newIndexData.Key;
				foreach (var newTypeData in newIndexData.Value)
				{
					string type = newTypeData.Key;
					foreach (var newIdData in newTypeData.Value)
					{
						string id = newIdData.Key;
						string content = newIdData.Value;

						// Permanently index the data (in memory).
						var indexData = _data.GetOrAdd(index, k => new ConcurrentDictionary<string, ConcurrentDictionary<string, string>>());
						var typeData = indexData.GetOrAdd(type, k => new ConcurrentDictionary<string, string>());
						typeData[id] = content;
					}
				}
			}
		}

		/// <summary>
		/// Count the number of items indexed, optionally filtering by index, type, and/or id.
		/// </summary>
		private int CountItems(Func<string, bool> indexFilter, Func<string, bool> typeFilter, Func<string, bool> idFilter)
		{
			int count = 0;
			foreach (var indexData in _data.Where(kvp => indexFilter(kvp.Key)))
			{
				foreach (var typeData in indexData.Value.Where(kvp => typeFilter(kvp.Key)))
				{
					foreach (var idData in typeData.Value.Where(kvp => idFilter(kvp.Key)))
					{
						count++;
					}
				}
			}

			return count;
		}

		private static bool IsValidJson(string content)
		{
			try
			{
				JsonConvert.DeserializeObject(content);
				return true;
			}
			catch
			{
				return false;
			}
		}

		private static void TrackItemToIndex(Dictionary<string, Dictionary<string, Dictionary<string, string>>> data, string index, string type, string id, string content)
		{
			Dictionary<string, Dictionary<string, string>> indexData;
			if (!data.TryGetValue(index, out indexData))
				indexData = data[index] = new Dictionary<string, Dictionary<string, string>>();

			Dictionary<string, string> typeData;
			if (!indexData.TryGetValue(type, out typeData))
				typeData = indexData[type] = new Dictionary<string, string>();

			typeData[id] = content;
		}

		public class IndexCommand
		{
			public IndexData index { get; set; }

			public static IndexCommand Parse(string content)
			{
				try
				{
					return JsonConvert.DeserializeObject<IndexCommand>(content);
				}
				catch
				{
					return null;
				}
			}

			public class IndexData
			{
				public string _index { get; set; }
				public string _type { get; set; }
				public string _id { get; set; }
			}
		}
	}
}
