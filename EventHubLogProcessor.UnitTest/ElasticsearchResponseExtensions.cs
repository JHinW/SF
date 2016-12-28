using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Nest;
using Newtonsoft.Json;

namespace EventHubLogProcessor.UnitTest
{
	public static class ElasticsearchResponseExtensions
	{
		public static Task<ElasticsearchResponse<BulkResponse>> GetBulkResponse(HttpStatusCode statusCode)
		{
			return Task.FromResult(GetElasticsearchResponse<BulkResponse>(statusCode));
		}

		public static ElasticsearchResponse<BulkResponse> GetBulkResponse(HttpStatusCode statusCode, dynamic content)
		{
			return GetElasticsearchResponse<BulkResponse>(statusCode, content);
		}

		public static ElasticsearchResponse<BulkResponse> ParseException()
		{
			return ElasticsearchResponseWithServerError(HttpStatusCode.BadRequest, "parse_exception", "Failed to derive xcontent");
		}

		public static ElasticsearchResponse<BulkResponse> JsonParseException()
		{
			return ElasticsearchResponseWithServerError(HttpStatusCode.InternalServerError, "json_parse_exception", "Unrecognized token 'foo': was expecting ('true', 'false' or 'null')\n at [Source: [B@7fc9f31f; line: 1, column: 9]");
		}

		public static ElasticsearchResponse<BulkResponse> MalformedActionException()
		{
			return ElasticsearchResponseWithServerError(HttpStatusCode.BadRequest, "illegal_argument_exception", "Malformed action/metadata line [1], expected START_OBJECT or END_OBJECT but found [VALUE_NUMBER]");
		}

		public static ElasticsearchResponse<BulkResponse> ActionIndexMissingException()
		{
			return ActionValidationException("index is missing;");
		}

		public static ElasticsearchResponse<BulkResponse> ActionTypeMissingException()
		{
			return ActionValidationException("type is missing;");
		}

		private static ElasticsearchResponse<BulkResponse> ActionValidationException(string reason)
		{
			return ElasticsearchResponseWithServerError(HttpStatusCode.BadRequest, "action_request_validation_exception", $"Validation Failed: 1: {reason}");
		}

		public static dynamic GetBulkResponseBuilder()
		{
			dynamic responseBuilder = new ExpandoObject();
			responseBuilder.took = 100;
			responseBuilder.errors = false;
			responseBuilder.items = new List<dynamic>();
			return responseBuilder;
		}

		public static void AddBulkItem(dynamic responseBuilder, string index, string type, string id)
		{
			responseBuilder.items.Add(CreateBulkItem(HttpStatusCode.OK, index, type, id));
		}

		public static void AddInvalidJsonItem(dynamic responseBuilder, string index, string type, string id)
		{
			AddBulkItem(responseBuilder, HttpStatusCode.BadRequest, index, type, id, "mapper_parsing_exception", "failed to parse, document is empty");
		}

		public static void AddEmptyItem(dynamic responseBuilder, string index, string type, string id)
		{
			AddBulkItem(responseBuilder, HttpStatusCode.BadRequest, index, type, id, "mapper_parsing_exception", "failed to parse");
		}

		private static void AddBulkItem(dynamic responseBuilder, HttpStatusCode statusCode, string index, string type, string id, string errorType, string errorReason)
		{
			responseBuilder.items.Add(CreateBulkItem(statusCode, index, type, id, errorType, errorReason));
			responseBuilder.errors = true;
		}

		public static ElasticsearchResponse<T> SetBody<T>(this ElasticsearchResponse<T> response, T body) where T : ResponseBase
		{
			((IResponse)body).CallDetails = response;
			return response.SetProperty("Body", body);
		}

		public static ElasticsearchResponse<T> SetServerError<T>(this ElasticsearchResponse<T> response, ServerError error) where T : ResponseBase
		{
			return response.SetProperty("ServerError", error);
		}

		public static ElasticsearchResponse<T> SetUri<T>(this ElasticsearchResponse<T> response, Uri uri) where T : ResponseBase
		{
			return response.SetProperty("Uri", uri);
		}

		private static ElasticsearchResponse<T> SetProperty<T, TValue>(this ElasticsearchResponse<T> response, string propertyName, TValue value) where T : ResponseBase
		{
			var property = typeof(ElasticsearchResponse<T>).GetProperty(propertyName);
			if (property == null)
				throw new InvalidOperationException($"Failed to find '{propertyName}' property on ElasticsearchResponse<T>");

			property.GetSetMethod(true).Invoke(response, new object[] { value });
			return response;
		}

		private static ElasticsearchResponse<T> GetElasticsearchResponse<T>(HttpStatusCode statusCode) where T : ResponseBase, new()
		{
			var response = new ElasticsearchResponse<T>((int)statusCode, new int[] { });
			response.SetUri(new Uri("http://localhost:9200"));
			return response.SetBody<T>(new T());
		}

		private static ElasticsearchResponse<T> GetElasticsearchResponse<T>(HttpStatusCode statusCode, dynamic content) where T : ResponseBase, new()
		{
			string serialized = JsonConvert.SerializeObject(content);
			var body = JsonConvert.DeserializeObject<T>(serialized);
			var response = GetElasticsearchResponse<T>(statusCode);
			return response.SetBody(body);
		}

		private static ElasticsearchResponse<BulkResponse> ElasticsearchResponseWithServerError(HttpStatusCode statusCode, string type, string reason)
		{
			var serverError = new ServerError()
			{
				Error = new Error()
				{
					Type = type,
					Reason = reason,
					RootCause = new List<RootCause>
					{
						new RootCause()
						{
							Type = type,
							Reason = reason,
						},
					},
				},
				Status = (int)statusCode,
			};
			return GetElasticsearchResponse<BulkResponse>(statusCode)
				.SetServerError(serverError);
		}

		private static dynamic CreateBulkItem(HttpStatusCode statusCode, string index, string type, string id)
		{
			return new
			{
				index = new
				{
					_index = index,
					_type = type,
					_id = id,
					_version = 1,
					_shards = new
					{
						total = 2,
						successful = 1,
						failed = 0,
					},
					status = (int)statusCode,
				},
			};
		}

		private static dynamic CreateBulkItem(HttpStatusCode statusCode, string index, string type, string id, string errorType, string errorReason)
		{
			return new
			{
				index = new
				{
					_index = index,
					_type = type,
					_id = id,
					status = (int)statusCode,
					error = new
					{
						type = errorType,
						reason = errorReason,
					},
				},
			};
		}
	}
}
