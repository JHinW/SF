using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Moq;
using Moq.Language.Flow;
using Nest;

namespace EventHubLogProcessor.UnitTest
{
	public static class MockExtensions
	{
		public static ISetup<IElasticLowLevelClient, Task<ElasticsearchResponse<BulkResponse>>> OnBulkAsync(this MockElasticClient mock)
		{
			return mock.MockLowLevelClient.Setup(c => c.BulkAsync<BulkResponse>(It.IsAny<PostData<object>>(), It.IsAny<Func<BulkRequestParameters, BulkRequestParameters>>()));
		}

		public static IReturnsResult<IElasticLowLevelClient> Returns(this ISetup<IElasticLowLevelClient, Task<ElasticsearchResponse<BulkResponse>>> setup, HttpStatusCode statusCode)
		{
			var response = ElasticsearchResponseExtensions.GetBulkResponse(statusCode);
			return setup.Returns(response);
		}

		public static void VerifyBulkAsyncCalled(this MockElasticClient mock, Times timesCalled)
		{
			mock.MockLowLevelClient.Verify(c => c.BulkAsync<BulkResponse>(It.IsAny<PostData<object>>(), It.IsAny<Func<BulkRequestParameters, BulkRequestParameters>>()), timesCalled);
		}
	}
}
