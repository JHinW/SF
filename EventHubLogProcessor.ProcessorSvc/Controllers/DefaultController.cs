using System.Collections.Generic;
using System.Web.Http;

namespace EventHubLogProcessor.ProcessorSvc.Controllers
{
	[RoutePrefix("api")]
	public class DefaultController : ApiController
	{
		[HttpGet]
		[Route("values")]
		public IEnumerable<string> Get()
		{
			return new string[] { "value1", "value2" };
		}

		[HttpGet]
		[Route("values")]
		public string Get(int id)
		{
			return "value";
		}

		[HttpPost]
		[Route("values")]
		public void Post([FromBody]string value)
		{
		}

		[HttpPut]
		[Route("values")]
		public void Put(int id, [FromBody]string value)
		{
		}

		[HttpDelete]
		[Route("values")]
		public void Delete(int id)
		{
		}
	}
}
