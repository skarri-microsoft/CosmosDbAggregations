using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationDataModels.TimeSeries
{
    using Newtonsoft.Json;

    public class SiteIdAggModel
    {
        [JsonProperty("id")]
        public String Id { get; set; }

        [JsonProperty("sensorId")]
        public String SensorId { get; set; }

        [JsonProperty("siteId")]
        public String SiteId { get; set; }

        [JsonProperty("eventCount")]
        public Int32 EventCount { get; set; }

        [JsonProperty("ts")]
        public Int32 UnixTimestamp { get; set; }

        [JsonProperty("updatedWorkers")]
        public List<Worker> UpdatedWorkers { get; set; }

        public static bool IsDuplicate(SiteIdAggModel model, SensorReading reading)
        {
            foreach (var worker in model.UpdatedWorkers)
            {
                if (worker.LastUpdatedEventId == reading.Id)
                {
                    return true;
                }
            }
            return false;
        }
        public static SiteIdAggModel GetNewDoc(SensorReading reading)
        {
            SiteIdAggModel model = new SiteIdAggModel();
            model.Id = reading.SiteId;
            model.SiteId = reading.SiteId;
            model.SensorId = reading.SensorId;
            model.UnixTimestamp = DateTimeUtil.GetUnixTimeStamp();
            model.EventCount = model.EventCount + 1;
            model.UpdatedWorkers = new List<Worker>();
            string machineName = Environment.MachineName;
            model.UpdatedWorkers.Add(
                new Worker()
                {
                    LastUpdatedEventId = reading.Id,
                    LastUpdatedUnixTimestamp = DateTimeUtil.GetUnixTimeStamp(),
                    Name = machineName
                });
            return model;
        }

        public static SiteIdAggModel GetIncrementedCountDoc(SiteIdAggModel model, SensorReading reading)
        {
            model.EventCount = model.EventCount + 1;
            bool isNew = true;
            string machineName = Environment.MachineName;
            foreach (var worker in model.UpdatedWorkers)
            {
                if (worker.Name.ToLower() == machineName.ToLower())
                {
                    worker.LastUpdatedEventId = reading.Id;
                    worker.LastUpdatedUnixTimestamp = DateTimeUtil.GetUnixTimeStamp();
                    isNew = false;
                    break;
                }
            }
            if (isNew)
            {
                model.UpdatedWorkers.Add(
                    new Worker()
                    {
                        LastUpdatedEventId = reading.Id,
                        LastUpdatedUnixTimestamp = DateTimeUtil.GetUnixTimeStamp(),
                        Name = machineName
                    });
            }

            return model;
        }
    }
}
