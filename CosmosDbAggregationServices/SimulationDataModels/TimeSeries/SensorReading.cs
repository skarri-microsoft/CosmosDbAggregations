using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationDataModels.TimeSeries
{
    using Newtonsoft.Json;

    public class SensorReading
    {
        private static int countries = 1;
        private static int states = 1;
        private static int cities = 1;

        // Approximate sites per city
        private static int sites = 100000;

        // Approximate units, homes, business units
        private static int units = 1000;

        // Approximate units per unit
        private static int noOfSensors = 50;

        [JsonProperty("id")]
        public String Id { get; set; }

        [JsonProperty("sensorId")]
        public String SensorId { get; set; }

        [JsonProperty("siteId")]
        public String SiteId { get; set; }

        [JsonProperty("ts")]
        public Int32 UnixTimestamp { get; set; }

        [JsonProperty("temp")]
        public Double Temperature { get; set; }

        [JsonProperty("pressure")]
        public Double Pressure { get; set; }

        public static SensorReading GetSampleReading()
        {

            return new SensorReading()
            {
                Id = System.Guid.NewGuid().ToString(),
                SensorId = "SensorId_" + new Random().Next(0, noOfSensors * units * sites * cities * states * countries),
                SiteId = "SiteId_" + new Random().Next(0, sites * cities * states * countries),
                Pressure = new Random().NextDouble(),
                UnixTimestamp = DateTimeUtil.GetUnixTimeStamp(),
                Temperature = new Random().Next(50, 100)
            };
        }
        

    }
}
