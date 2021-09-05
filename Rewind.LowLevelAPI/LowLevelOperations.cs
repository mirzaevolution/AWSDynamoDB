using System;
using System.Collections.Generic;
using System.Text;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon;
using System.Threading.Tasks;
using System.Net;
using System.Linq;

namespace Rewind.LowLevelAPI
{
    public class LowLevelOperations

    {
        private readonly IAmazonDynamoDB _dynamoDBClient;
        private readonly string _tableName = "Profile";
        public LowLevelOperations(IAmazonDynamoDB dynamoDBClient)
        {
            _dynamoDBClient = dynamoDBClient;
        }

        protected virtual Profile ConvertToProfile(Dictionary<string, AttributeValue> item)
        {
            return new Profile(
                    id: item[nameof(Profile.Id)].S,
                    name: item[nameof(Profile.Name)].S,
                    email: item[nameof(Profile.Email)].S,
                    countryId: item[nameof(Profile.CountryId)].S,
                    phones: item[nameof(Profile.Phones)].SS
                );
        }
        protected virtual Dictionary<string, AttributeValue> ConvertFromProfile(Profile profile)
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

        private void IterateProfileList(IEnumerable<Profile> profiles)
        {
            foreach(var profile in profiles)
            {
                Console.WriteLine(profile.ToString());
            }
        }

        public async Task Scan()
        {
            ScanResponse response = await _dynamoDBClient.ScanAsync(new ScanRequest
            {
                TableName = _tableName
            });
            if(response.HttpStatusCode == HttpStatusCode.OK && response.Items.Count > 0)
            {
                IterateProfileList(response.Items.Select(ConvertToProfile));
            }
        }

        public async Task GetItem(string countryId, string name)
        {
            GetItemResponse getItemResponse =  await _dynamoDBClient.GetItemAsync(new GetItemRequest
            {
                TableName = _tableName,
                Key =
                {
                    { nameof(Profile.CountryId), new AttributeValue(countryId) },
                    { nameof(Profile.Name), new AttributeValue(name)  }
                }
            });
            if(getItemResponse.HttpStatusCode == HttpStatusCode.OK &&
               getItemResponse.Item !=null)
            {
                Console.WriteLine(ConvertToProfile(getItemResponse.Item));
            }
        }

        public async Task QueryByCountryId(string countryId = "ID")
        {
            QueryResponse queryResponse = await _dynamoDBClient.QueryAsync(new QueryRequest
            {
                TableName = _tableName,
                KeyConditionExpression = $"{nameof(Profile.CountryId)} = :countryId",
                ExpressionAttributeValues =
                {
                    {
                        ":countryId", new AttributeValue("ID")
                    }
                }
            });
            if(queryResponse.HttpStatusCode == HttpStatusCode.OK && queryResponse.Items.Count > 0)
            {
                IterateProfileList(queryResponse.Items.Select(ConvertToProfile));
            }
        }
        public async Task QueryByCountryIdAndId(string id, string countryId = "ID")
        {
            QueryResponse queryResponse = await _dynamoDBClient.QueryAsync(new QueryRequest
            {
                TableName = _tableName,
                KeyConditionExpression = $"{nameof(Profile.CountryId)} = :countryId",
                FilterExpression = "Id = :id",
                ExpressionAttributeValues =
                {
                    {
                        ":countryId", new AttributeValue("ID")
                    },
                    {
                        ":id", new AttributeValue(id)
                    }
                }
            });
            if (queryResponse.HttpStatusCode == HttpStatusCode.OK && queryResponse.Items.Count > 0)
            {
                IterateProfileList(queryResponse.Items.Select(ConvertToProfile));
            }
        }
        public async Task QueryNameStartsWith(string name, string countryId = "ID")
        {
            QueryRequest queryRequest = new QueryRequest(_tableName)
            {
                KeyConditionExpression = $"{nameof(Profile.CountryId)} = :countryId AND begins_with(#name, :name)",
                //because 'Name' is reserved name in DynamoDB Engine
                ExpressionAttributeNames =
                {
                    { "#name", nameof(Profile.Name) }
                },
                ExpressionAttributeValues =
                {
                    { ":countryId", new AttributeValue(countryId) },
                    { ":name", new AttributeValue(name) }
                }
            };
            QueryResponse queryResponse = await _dynamoDBClient.QueryAsync(queryRequest);
            if(queryResponse.HttpStatusCode == HttpStatusCode.OK &&
               queryResponse.Items.Count > 0)
            {
                IterateProfileList(queryResponse.Items.Select(ConvertToProfile));
            }
        }

        public async Task QueryByEmails(string countryId, List<string> emails)
        {
            var request = new QueryRequest(_tableName)
            {
                KeyConditionExpression = "CountryId = :countryId",
                ExpressionAttributeValues =
                {
                    {
                        ":countryId", new AttributeValue(countryId)
                    }
                }
            };
            string filterExpressions = "Email IN (";
            int index = 1;
            foreach(var email in emails)
            {
                filterExpressions += $":email{index}";
                if (index != emails.Count)
                {
                    filterExpressions += ", ";
                }
                request.ExpressionAttributeValues.Add($":email{index++}", new AttributeValue(email));
            }
            filterExpressions += ")";
            request.FilterExpression = filterExpressions;

            var response = await _dynamoDBClient.QueryAsync(request);
            if(response.HttpStatusCode == HttpStatusCode.OK && response.Items.Count > 0)
            {
                IterateProfileList(response.Items.Select(ConvertToProfile));
            }

        }

        public async Task InsertNewItem(Profile profile)
        {
            Dictionary<string, AttributeValue> item =
                ConvertFromProfile(profile);
            PutItemRequest putItemRequest = new PutItemRequest
            {
                TableName = _tableName,
                Item = item
            };
            PutItemResponse putItemResponse = await _dynamoDBClient.PutItemAsync(putItemRequest);
            if(putItemResponse.HttpStatusCode == HttpStatusCode.OK || 
               putItemResponse.HttpStatusCode == HttpStatusCode.Created)
            {
                Console.WriteLine("New item successfully added");
            }
            else
            {
                Console.WriteLine(putItemResponse.HttpStatusCode);
            }
        }

        public async Task UpdateEmail(string countryId, string name, string id, string newPhoneToInsert, string emailToUpdate)
        {
            UpdateItemResponse updateItemResponse = await _dynamoDBClient.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = _tableName,
                Key =
                {
                    { "CountryId", new AttributeValue(countryId) },
                    { "Name", new AttributeValue(name) }
                },
                UpdateExpression = "ADD Phones :newPhone SET Email = :email",
                ConditionExpression = "Id = :id",
                ExpressionAttributeValues =
                {
                    {
                        ":id", new AttributeValue(id)
                    },
                    {
                        ":newPhone", new AttributeValue{ SS = new List<string> { newPhoneToInsert } }
                    },
                    {
                        ":email", new AttributeValue(emailToUpdate)
                    }
                }
            });


            if (updateItemResponse.HttpStatusCode == HttpStatusCode.OK ||
               updateItemResponse.HttpStatusCode == HttpStatusCode.Accepted)
            {
                Console.WriteLine("Fields updated successfully");
            }
            else
            {
                Console.WriteLine(updateItemResponse.HttpStatusCode);
            }

        }

    }
}
