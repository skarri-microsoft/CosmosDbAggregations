using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationDataModels
{
    using Newtonsoft.Json;

    public class Worker
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("lastUpdatedEventId")]
        public string LastUpdatedEventId { get; set; }

        [JsonProperty("lastUpdatedTs")]
        public Int32 LastUpdatedUnixTimestamp { get; set; }
    }
}
