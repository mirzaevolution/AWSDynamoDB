using System;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon;
using System.Collections.Generic;

namespace Rewind.LowLevelAPI
{
    class Program
    {
        private readonly static IAmazonDynamoDB _dynamoDBClient;
        static Program()
        {
            _dynamoDBClient = new AmazonDynamoDBClient(RegionEndpoint.APSoutheast1);
        }
        static void CallLowLevelOperations()
        {
            LowLevelOperations lowLevelOperations = new LowLevelOperations(_dynamoDBClient);
            //lowLevelOperations.Scan().GetAwaiter().GetResult();
            //lowLevelOperations.QueryByCountryId().GetAwaiter().GetResult();
            //lowLevelOperations.QueryByCountryIdAndId("a88b2fa7-816c-47c9-8a63-e08499ce3760").GetAwaiter().GetResult(); 
            //lowLevelOperations.GetItem("ID", "Mirza Ghulam Rasyid").GetAwaiter().GetResult();
            lowLevelOperations.QueryByEmails("ID", new List<string> { "ghulamcyber@hotmail.com","raraanjani@gmail.com" }).GetAwaiter().GetResult();
            //lowLevelOperations.QueryNameStartsWith("M").GetAwaiter().GetResult();
            //lowLevelOperations.InsertNewItem(new Profile
            //{
            //    Name = "Anggita Anjasari",
            //    Email = "anggitaanjasari12@hotmail.com",
            //    CountryId = "ID",
            //    Phones = new List<string>
            //    {
            //        "+62896123123"
            //    }
            //}).GetAwaiter().GetResult();
            //lowLevelOperations.UpdateEmail("ID", "Ahmad P. Muzakir", "a88b2fa7-816c-47c9-8a63-e08499ce3760", "+62858112233", "ahmadmuzakir1991@hotmail.com").GetAwaiter().GetResult();
        }
        static void Main(string[] args)
        {
            CallLowLevelOperations();
            Console.ReadLine();
        }
    }
}
