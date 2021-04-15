using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Amazon.DynamoDBv2.DataModel;
namespace StarterIntro
{
    [DynamoDBTable("Profile")]
    public class Profile
    {

        public Profile() { }
        public Profile(string id, string name, string email, string countryId, IEnumerable<string> phones)
        {
            Id = id;
            Name = name;
            Email = email;
            CountryId = countryId;
            if (phones != null && phones.Count() > 0)
            {
                Phones.AddRange(phones);
            }
        }
        public Profile(string name, string email, string countryId, IEnumerable<string> phones)
        {
            Name = name;
            Email = email;
            CountryId = countryId;
            if(phones!=null && phones.Count() > 0)
            {
                Phones.AddRange(phones);
            }
        }
        public string Id { get; set; } = Guid.NewGuid().ToString();
        [DynamoDBRangeKey]
        public string Name { get; set; }
        public string Email { get; set; }
        [DynamoDBHashKey]
        public string CountryId { get; set; }
        public List<string> Phones { get; set; } = new List<string>();
        public override string ToString()
        {
            return $"Id: {Id}, Name: {Name}, Email: {Email}, CountryId: {CountryId}, Phones: [{Phones.Aggregate((current, next) => $"{current}, {next}")}]";
        }
    }
    
}
