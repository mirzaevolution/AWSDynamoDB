using System;
using System.Collections.Generic;
using Amazon;
using Amazon.DynamoDBv2;

namespace Rewind.DocumentModelAPI
{
    class Program
    {
        private readonly static IAmazonDynamoDB _dynamoDBClient;
        static Program()
        {
            _dynamoDBClient = new AmazonDynamoDBClient(RegionEndpoint.APSoutheast1);
        }
        static void InitiateDocumentLevelInstance()
        {
            DocumentLevelOperations op = new DocumentLevelOperations(_dynamoDBClient);
            //op.ScanAll().GetAwaiter().GetResult();
            //op.ScanByConditions().GetAwaiter().GetResult();
            //op.GetItem("ID", "Mirza Ghulam Rasyid").GetAwaiter().GetResult();
            //op.QueryByCountryId("ID").GetAwaiter().GetResult();
            //op.QueryByConditions().GetAwaiter().GetResult();
            //op.InsertNewItem(new Profile
            //{
            //    CountryId = "US",
            //    Name = "James Gordon",
            //    Email = "jgordon21@hotmail.com",
            //    Phones =
            //    {
            //        "+11233355",
            //        "+18922112"
            //    }
            //}).GetAwaiter().GetResult();
            //op.UpdateItem().GetAwaiter().GetResult();
            op.InsertBatchItems(new List<Profile>
            {
                new Profile("Ririn Putri Ayu","ririnayu22@hotmail.com","ID", new List<string>{ "+62858112233" }),
                new Profile("Rey Shaun","rsh9090@gmail.com","US",new List<string> { "+12244903" })
            }).GetAwaiter().GetResult();
        }
        static void Main(string[] args)
        {
            InitiateDocumentLevelInstance();
            Console.ReadLine();
        }
    }
}
