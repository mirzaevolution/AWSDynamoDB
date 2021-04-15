using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;


namespace StarterIntro
{
    public class LowLevelModel
    {
        private readonly IAmazonDynamoDB _dynamoDbClient;
        private string _tableName;
        public LowLevelModel(IAmazonDynamoDB dynamoDBClient, string tableName)
        {
            _dynamoDbClient = dynamoDBClient;
            _tableName = tableName;
        }
        private Profile ConvertToProfile(Dictionary<string, AttributeValue> rawItem)
        {
            return new Profile(
                    id: rawItem[nameof(Profile.Id)].S,
                    name: rawItem[nameof(Profile.Name)].S,
                    email: rawItem[nameof(Profile.Email)].S,
                    countryId: rawItem[nameof(Profile.CountryId)].S,
                    phones: rawItem[nameof(Profile.Phones)].SS
                );
        }
        private Dictionary<string, AttributeValue> ConvertToRaw(Profile profile)
        {
            return new Dictionary<string, AttributeValue>
            {
                { nameof(Profile.Id), new AttributeValue(profile.Id) },
                { nameof(Profile.Name), new AttributeValue(profile.Name) },
                { nameof(Profile.Email), new AttributeValue(profile.Email) },
                { nameof(Profile.CountryId), new AttributeValue(profile.CountryId) },
                { nameof(Profile.Phones), new AttributeValue(profile.Phones) }
            };
        }
        private List<string> AttributesToGet() => new List<string>
            {
                nameof(Profile.CountryId),
                nameof(Profile.Name),
                nameof(Profile.Email),
                nameof(Profile.Id),
                nameof(Profile.Phones)
            };
        public async Task ScanItems()
        {
            #region All items

            ScanResponse scanResponse = await _dynamoDbClient.ScanAsync(_tableName, AttributesToGet());
            if(scanResponse.HttpStatusCode == System.Net.HttpStatusCode.OK && scanResponse.Count > 0)
            {
               
                var result = scanResponse.Items.Select(ConvertToProfile);
                Console.WriteLine("#All items:");
                foreach (var item in result)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            #endregion


            #region People which the names start with 'Mi'
            Console.WriteLine("\n#People which the names start with 'Mi': ");
            scanResponse = await _dynamoDbClient.ScanAsync(new ScanRequest
            {
                TableName = _tableName,
                AttributesToGet = AttributesToGet(),
                ScanFilter = new Dictionary<string, Condition>
                {
                    {
                        nameof(Profile.CountryId), new Condition
                        {
                            ComparisonOperator = ComparisonOperator.EQ,
                            AttributeValueList = new List<AttributeValue>
                            {
                                new AttributeValue("ID")
                            }
                        }
                    },
                    {
                        nameof(Profile.Name), new Condition
                        {
                            ComparisonOperator = ComparisonOperator.BEGINS_WITH,
                            AttributeValueList = new List<AttributeValue>
                            {
                                new AttributeValue("Mi")
                            }
                        }
                    }
                }
            });
            if(scanResponse.HttpStatusCode == System.Net.HttpStatusCode.OK && scanResponse.Count > 0)
            {
                var result = scanResponse.Items.Select(ConvertToProfile);
                foreach (var item in result)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            
            #endregion
        }

        public async Task QueryItems()
        {
            #region Query based on partition key: 'ID' in asc order
            //var queryResponse = await _dynamoDbClient.QueryAsync(new QueryRequest
            //{
            //    TableName = _tableName,
            //    AttributesToGet = AttributesToGet(),
            //    KeyConditions = new Dictionary<string, Condition>
            //    {
            //         {
            //            nameof(Profile.CountryId), new Condition
            //            {
            //                ComparisonOperator = ComparisonOperator.EQ,
            //                AttributeValueList = new List<AttributeValue>
            //                {
            //                    new AttributeValue("ID")
            //                }
            //            }
            //        }
            //    },
            //    ScanIndexForward = true
            //});
            var queryResponse = await _dynamoDbClient.QueryAsync(new QueryRequest
            {
               TableName = _tableName,
               KeyConditionExpression = $"CountryId = :ci",
               ExpressionAttributeValues = new Dictionary<string, AttributeValue>
               {
                   {
                       ":ci", new AttributeValue{ S = "ID" }
                   }
               },
                ScanIndexForward = true
            });
            if (queryResponse.HttpStatusCode == System.Net.HttpStatusCode.OK && queryResponse.Count > 0)
            {

                var profiles = queryResponse.Items.Select(ConvertToProfile);
                Console.WriteLine("\nQuery based on partition key: 'ID' in asc order");
                foreach (var item in profiles)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            #endregion


            #region Query based on partition key: 'ID' in desc order
            queryResponse = await _dynamoDbClient.QueryAsync(new QueryRequest
            {
                TableName = _tableName,
                KeyConditionExpression = $"CountryId = :ci",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
               {
                   {
                       ":ci", new AttributeValue{ S = "ID" }
                   }
               },
                ScanIndexForward = false
            });
            if (queryResponse.HttpStatusCode == System.Net.HttpStatusCode.OK && queryResponse.Count > 0)
            {

                var profiles = queryResponse.Items.Select(ConvertToProfile);
                Console.WriteLine("\nQuery based on partition key: 'ID' in desc order");

                foreach (var item in profiles)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            #endregion


            #region Query based on partition key: 'ID' and name starts with 'M' in desc order
            queryResponse = await _dynamoDbClient.QueryAsync(new QueryRequest
            {
                TableName = _tableName,
                KeyConditionExpression = $"{nameof(Profile.CountryId)} = :ci AND begins_with(#{nameof(Profile.Name)}, :personName)",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { $"#{nameof(Profile.Name)}", nameof(Profile.Name) }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
               {
                    {
                       ":ci", new AttributeValue{ S = "ID" }
                    },
                    {
                        ":personName", new AttributeValue { S = "M" }
                    }
               },
                ScanIndexForward = false
            });
            if (queryResponse.HttpStatusCode == System.Net.HttpStatusCode.OK && queryResponse.Count > 0)
            {

                var profiles = queryResponse.Items.Select(ConvertToProfile);
                Console.WriteLine("\nQuery based on partition key: 'ID' and name starts with 'M' in desc order");

                foreach (var item in profiles)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            #endregion

        }
        public async Task GetItem(string countryId, string name)
        {
            try
            {
                GetItemResponse getItemResponse = await _dynamoDbClient.GetItemAsync(new GetItemRequest
                {
                    AttributesToGet = AttributesToGet(),
                    ConsistentRead = true,
                    TableName = _tableName,
                    Key = new Dictionary<string, AttributeValue>
                    {
                        {
                            nameof(Profile.CountryId), new AttributeValue
                            {
                                S = countryId
                            }
                        },
                        {
                            nameof(Profile.Name), new AttributeValue
                            {
                                S = name
                            }
                        }
                    }
                });
                if(getItemResponse.HttpStatusCode == System.Net.HttpStatusCode.OK &&
                   getItemResponse.Item.Count > 0)
                {
                    var result = ConvertToProfile(getItemResponse.Item);
                    Console.WriteLine(result.ToString());
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        public async Task InsertItem(Profile profile)
        {
            try
            {
                var rawItem = ConvertToRaw(profile);
                
                var putItemResponse = await _dynamoDbClient.PutItemAsync(new PutItemRequest
                {
                    Item = rawItem,
                    TableName = _tableName
                });
                if(putItemResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    Console.WriteLine("Data has been inserted successfully");
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        public async Task UpdateItem(Profile profile)
        {
            try
            {
                

                var updateItemResponse = await _dynamoDbClient.UpdateItemAsync(new UpdateItemRequest
                {
                    AttributeUpdates = new Dictionary<string, AttributeValueUpdate>
                    {
                        {
                            nameof(Profile.Email), new AttributeValueUpdate(new AttributeValue(profile.Email),AttributeAction.PUT)
                        },
                        {
                            nameof(Profile.Phones), new AttributeValueUpdate(new AttributeValue(profile.Phones), AttributeAction.PUT)
                        }

                    },
                    Key = new Dictionary<string, AttributeValue>
                    {
                        {
                            nameof(Profile.CountryId), new AttributeValue
                            {
                                S = profile.CountryId
                            }
                        },
                        {
                            nameof(Profile.Name), new AttributeValue
                            {
                                S = profile.Name
                            }
                        }
                    },
                    TableName = _tableName
                });
                if (updateItemResponse.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    Console.WriteLine("Data has been updated successfully");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}
