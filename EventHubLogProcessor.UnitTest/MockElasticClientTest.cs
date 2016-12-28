using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Moq;
using Nest;
using Newtonsoft.Json;
using Xunit;

namespace EventHubLogProcessor.UnitTest
{
	/// <summary>
	/// The MockElasticClient is complicated enough to warrant some unit tests, to validate
	/// it is returning the expected responses for a variety of valid/invalid requests :)
	/// </summary>
	public class MockElasticClientTest
	{
		private MockElasticClient _client = new MockElasticClient(MockBehavior.Strict);
		private IElasticLowLevelClient Client => _client.Object.LowLevel;

		public MockElasticClientTest()
		{
			_client.SimulateElasticSearch();
		}

		[Fact]
		public async Task EmptyContent_BadRequest()
		{
			var response = await Client.BulkAsync<BulkResponse>("", p => p.Consistency(Consistency.One).Refresh(false));

			Assert.NotNull(response);
			Assert.False(response.Success);
			Assert.NotNull(response.ServerError);
		}

		[Fact]
		public async Task InvalidContent_BadRequest()
		{
			var response = await Client.BulkAsync<BulkResponse>("invalid content", p => p.Consistency(Consistency.One).Refresh(false));

			Assert.NotNull(response);
			Assert.False(response.Success);
			Assert.NotNull(response.ServerError);
		}

		[Fact]
		public async Task InvalidJson_BadRequest()
		{
			var response = await Client.BulkAsync<BulkResponse>("{}", p => p.Consistency(Consistency.One).Refresh(false));

			Assert.NotNull(response);
			Assert.False(response.Success);
			Assert.NotNull(response.ServerError);
		}

		[Fact]
		public async Task ValidSingleRequest_OK()
		{
			var request = @"
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""1"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 1 }
			";
			var response = await Client.BulkAsync<BulkResponse>(request, p => p.Consistency(Consistency.One).Refresh(false));

			// ElasticsearchResponse should be successful.
			Assert.NotNull(response);
			Assert.True(response.Success);
			Assert.Null(response.ServerError);
			
			// BulkResponse body should be successful.
			Assert.NotNull(response.Body);
			Assert.True(response.Body.IsValid);
			Assert.False(response.Body.Errors);
			Assert.Equal(1, response.Body.Items.Count());
			Assert.Equal(0, response.Body.ItemsWithErrors.Count());
		}

		[Fact]
		public async Task ValidMultipleRequest_OK()
		{
			var request = @"
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""1"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 1 }
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""2"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 2 }
			";
			var response = await Client.BulkAsync<BulkResponse>(request, p => p.Consistency(Consistency.One).Refresh(false));

			// ElasticsearchResponse should be successful.
			Assert.NotNull(response);
			Assert.True(response.Success);
			Assert.Null(response.ServerError);

			// BulkResponse body should be successful.
			Assert.NotNull(response.Body);
			Assert.True(response.Body.IsValid);
			Assert.False(response.Body.Errors);
			Assert.Equal(2, response.Body.Items.Count());
			Assert.Equal(0, response.Body.ItemsWithErrors.Count());
		}

		[Fact]
		public async Task ValidRequest_OneEmptyItem_OK()
		{
			// First item content is empty.
			var request = @"
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""1"" } }
				
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""2"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 2 }
			";
			var response = await Client.BulkAsync<BulkResponse>(request, p => p.Consistency(Consistency.One).Refresh(false));

			// ElasticsearchResponse should be successful.
			Assert.NotNull(response);
			Assert.True(response.Success);
			Assert.Null(response.ServerError);

			// BulkResponse body should have one failed item.
			Assert.NotNull(response.Body);
			Assert.False(response.Body.IsValid);
			Assert.True(response.Body.Errors);
			Assert.Equal(2, response.Body.Items.Count());
			Assert.Equal(1, response.Body.ItemsWithErrors.Count());

			var items = response.Body.Items.ToList();
			Assert.False(items[0].IsValid);
			Assert.True(items[1].IsValid);
		}

		[Fact]
		public async Task ValidRequest_OneNonJsonItem_OK()
		{
			// First item content is invalid json.
			var request = @"
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""1"" } }
				invalid item content
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""2"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 2 }
			";
			var response = await Client.BulkAsync<BulkResponse>(request, p => p.Consistency(Consistency.One).Refresh(false));

			// ElasticsearchResponse should be successful.
			Assert.NotNull(response);
			Assert.True(response.Success);
			Assert.Null(response.ServerError);

			// BulkResponse body should have one failed item.
			Assert.NotNull(response.Body);
			Assert.False(response.Body.IsValid);
			Assert.True(response.Body.Errors);
			Assert.Equal(2, response.Body.Items.Count());
			Assert.Equal(1, response.Body.ItemsWithErrors.Count());

			var items = response.Body.Items.ToList();
			Assert.False(items[0].IsValid);
			Assert.True(items[1].IsValid);
		}

		[Fact]
		public async Task InvalidRequest_ItemWithNewlines_BadRequest()
		{
			// First item content has newlines.
			var request = @"
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""1"" } }
				{
					""message"": ""log message"",
					""@timestamp"": ""2016-06-29T14:12:12"",
					""data"": 1
				}
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""2"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 2 }
			";
			var response = await Client.BulkAsync<BulkResponse>(request, p => p.Consistency(Consistency.One).Refresh(false));

			// ElasticsearchResponse should fail.
			Assert.NotNull(response);
			Assert.False(response.Success);
			Assert.NotNull(response.ServerError);
		}

		[Fact]
		public async Task InvalidRequest_MissingIndex_BadRequest()
		{
			// Missing '_index' on first "index" request.
			var request = @"
				{ ""index"": { ""_type"": ""logevent"", ""_id"": ""1"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 1 }
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""2"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 2 }
			";
			var response = await Client.BulkAsync<BulkResponse>(request, p => p.Consistency(Consistency.One).Refresh(false));
			
			Assert.NotNull(response);
			Assert.False(response.Success);
			Assert.NotNull(response.ServerError);
		}

		[Fact]
		public async Task InvalidRequest_MissingType_BadRequest()
		{
			// Missing '_type' on first "index" request.
			var request = @"
				{ ""index"": { ""_index"": ""logstash"", ""_id"": ""1"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 1 }
				{ ""index"": { ""_index"": ""logstash"", ""_type"": ""logevent"", ""_id"": ""2"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 2 }
			";
			var response = await Client.BulkAsync<BulkResponse>(request, p => p.Consistency(Consistency.One).Refresh(false));

			Assert.NotNull(response);
			Assert.False(response.Success);
			Assert.NotNull(response.ServerError);
		}

		[Fact]
		public async Task InvalidRequest_InvalidAction_BadRequest()
		{
			// Invalid 'action' line.
			var request = @"
				{ ""index"": { ""_index"": ""logstash"", ""_id"": ""1"" } }
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 1 }
				invalid action metadata
				{ ""message"": ""log message"", ""@timestamp"": ""2016-06-29T14:12:12"", ""data"": 2 }
			";
			var response = await Client.BulkAsync<BulkResponse>(request, p => p.Consistency(Consistency.One).Refresh(false));

			Assert.NotNull(response);
			Assert.False(response.Success);
			Assert.NotNull(response.ServerError);
		}
	}
}
