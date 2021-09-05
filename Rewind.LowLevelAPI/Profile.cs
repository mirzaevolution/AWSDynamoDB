using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Rewind.LowLevelAPI
{
    public class Profile
    {
        public Profile() { }
        public Profile(string id, string name, string email, string countryId, List<string> phones)
        {
            Id = id;
            Name = name;
            Email = email;
            CountryId = countryId;
            Phones = phones;
            
        }
        public Profile(string name, string email, string countryId, List<string> phones)
        {
            Name = name;
            Email = email;
            CountryId = countryId;
            Phones = phones;
        }
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string Name { get; set; }
        public string Email { get; set; }
        public string CountryId { get; set; }
        public List<string> Phones { get; set; } = new List<string>();
        public override string ToString()
        {
            return $"Id: {Id}, " +
                $"Name: {Name}, " +
                $"Email: {Email}, " +
                $"CountryId: {CountryId}, " +
                $"Phones: [{Phones.Aggregate((current, next) => $"{current}, {next}")}]";

        }

    }
}
