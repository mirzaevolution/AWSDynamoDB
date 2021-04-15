using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.EnvironmentVariables;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

using Amazon.Extensions.NETCore.Setup;
using System.Collections.Generic;

namespace StarterIntro
{
    
    
    class Program
    {
        private static IConfiguration _config;
        private static IServiceProvider _serviceProvider;
        private static DynamoDBContext _context;
        static Program()
        {
            _config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", true, true)
                .Build();
            var awsOptions = _config.GetAWSOptions();
            _serviceProvider = new ServiceCollection()
                .AddAWSService<IAmazonDynamoDB>(awsOptions)
                .BuildServiceProvider();
        }
        static IAmazonDynamoDB GetDynamoDBInstance()
        {
             return _serviceProvider.GetRequiredService<IAmazonDynamoDB>();
        }
        static DynamoDBContext GetContext()
        {
            if(_context == null)
            {
                _context = new DynamoDBContext(GetDynamoDBInstance());
            }
            return _context;
        }
        static async Task ListTables()
        {
            var client = GetDynamoDBInstance();
            var response = await client.ListTablesAsync();
            foreach(string table in response.TableNames)
            {
                Console.WriteLine(table);
            }
        }
        
        static async Task UsingObjectPersistenceModel()
        {
            ObjectPersistentModel obj = new ObjectPersistentModel(GetContext());
            //await obj.InsertNewItems(new List<Profile>
            //{
            //    new Profile("Mirza Ghulam Rasyid","ghulamcyber@hotmail.com","ID",new[]
            //    {
            //        "+62-896-1648-2113",
            //        "+62-858-0637-7218"
            //    }),
            //    new Profile("Rara Anjani", "raraanjani@gmail.com", "ID", new[]
            //    {
            //        "+62-896-2221-5511"
            //    }),
            //    new Profile("Beggi Mammad","beggi.mammad@gmail.com","ID",new []
            //    {
            //        "+62-857-5412-2131"
            //    }),
            //    new Profile("Michael Hawk","hawk@gmail.com","US",new[]
            //    {
            //        "+1-221-5511"
            //    }),
            //    new Profile("Darren Johnson","drjohnson22@gmail.com","US", new[]
            //    {
            //        "+1-222-5212"
            //    })

            //});

            //await obj.ScanItems();

            //await obj.QueryItem();

            //await obj.LoadItems();
            //await obj.InsertNewItem();

            await obj.UpdateItem();

        }

        static async Task UsingDocumentModel()
        {
            DocumentModel documentModel = new DocumentModel(GetDynamoDBInstance(), nameof(Profile));
            //await documentModel.ScanItems();
            //await documentModel.QueryItems();
            //await documentModel.InsertNewItems(new List<Profile>
            //{
            //    new Profile("Ahmad Muzakir","ah.mz23@gmail.com","ID", new[]
            //    {
            //        "+62-896-2221-2224"
            //    }),
            //    new Profile("Debby Ayu","debyy.ayuu@gmail.com","ID", new[]
            //    {
            //        "+62-857-9901-4312"
            //    })
            //});
            var profile =  await documentModel.LoadItems("ID", "Rara Anjani");
            if (profile != null)
            {
                profile.Phones.Add("+62-857-2128-778");
                await documentModel.InsertOrUpdateItem(profile, true);
            }

        }
        static async Task UsingLowLevelModel()
        {
            LowLevelModel lowLevelModel = new LowLevelModel(GetDynamoDBInstance(), nameof(Profile));
            //await lowLevelModel.ScanItems();
            //await lowLevelModel.QueryItems();
            //await lowLevelModel.GetItem("ID", "Mirza Ghulam Rasyid");
            //await lowLevelModel.InsertItem(new Profile
            //{
            //    Name = "Muhammad Hasan",
            //    Email = "m.hasan30@gmail.com",
            //    CountryId = "ID",
            //    Phones =
            //    {
            //        "+62-896-4412-4421"
            //    }
            //});
            await lowLevelModel.UpdateItem(new Profile
            {
                Name = "Muhammad Hasan",
                Email = "muh.hasan30@gmail.com",
                CountryId = "ID",
                Phones =
                {
                    "+62-896-4412-4421"
                }
            });
        }

        static void Main(string[] args)
        {
            //ListTables().GetAwaiter().GetResult();

            //UsingObjectPersistenceModel().Wait();

            //UsingDocumentModel().Wait();

            UsingLowLevelModel().Wait();

            Console.ReadLine();
        }
    }
}
