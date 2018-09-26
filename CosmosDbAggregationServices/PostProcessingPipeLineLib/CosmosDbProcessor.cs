using Common.SdkExtensions;
using Common.SinkContracts;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace PostProcessingWorkerRole
{
    using System.Diagnostics;
    using System.Net;
    using System.Threading.Tasks;
    using Common;
    using Newtonsoft.Json;
    using SimulationDataModels.TimeSeries;

    public class CosmosDbProcessor : ICosmosDbSink
    {
        private int retries = 10;

        private  async Task<List<object>> GetSiteIdAggDocs(SqlClientExtension client, SensorReading sensorReading)
        {
            return  await client.queryDocs("select * from c where c.id='" + sensorReading.SiteId + "'", sensorReading.SensorId);
            
        }

        private async Task UpdateSiteIdAgg(
            SqlClientExtension client, 
            object doc,
            SensorReading sensorReading)
        {
            // take the first document
            SiteIdAggModel siteIdAggModel =
                JsonConvert.DeserializeObject<SiteIdAggModel>(doc.ToString());

            if (!SiteIdAggModel.IsDuplicate(siteIdAggModel, sensorReading))
            {
                SiteIdAggModel aggModel =
                    SiteIdAggModel.GetIncrementedCountDoc(siteIdAggModel, sensorReading);

                for (int i = 0; i < retries; i++)
                {
                    try
                    {
                        await client.UpdateItem((Document)doc, aggModel);
                        break;
                    }
                    catch (DocumentClientException docEx)
                    {
                        if (docEx.StatusCode == HttpStatusCode.Conflict)
                        {
                            // Eat this execption, it can happen more workers are trying
                            // to update the same document
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message, false);
                    }

                }

            }
        }
        public async void IngestDocs(
            SqlClientExtension client, 
            IChangeFeedObserverContext context, 
            IReadOnlyList<Document> docs, 
            CancellationToken cancellationToken, 
            Uri destinationCollectionUri)
        {
            // Model is received
            foreach (var doc in docs)
            {
                SensorReading sensorReading = 
                    JsonConvert.DeserializeObject<SensorReading>(doc.ToString());
                // Load the aggregated model from the destination

                List<object> aggDocs =
                    await GetSiteIdAggDocs(client, sensorReading);

                if (aggDocs != null && aggDocs.Any())
                {
                    await UpdateSiteIdAgg(client, aggDocs[0], sensorReading);
                }
                else
                {
                    SiteIdAggModel siteIdAggModel = SiteIdAggModel.GetNewDoc(sensorReading);
                    bool isExits = false;
                    bool isCreateCompleted = false;

                    try
                    {
                        for (int i = 0; i < retries; i++)
                        {
                            if (!isExits)
                            {

                                await client.CreateDocument(siteIdAggModel, false);
                                isCreateCompleted = true;
                            }

                            if(isCreateCompleted)
                            {
                                break;
                            }

                        }
                    }
                    catch (DocumentClientException docEx)
                    {
                        if (docEx.StatusCode == HttpStatusCode.Conflict)
                        {
                            // Eat this may be other process created the document already.
                            isExits = true;
                            isCreateCompleted = true;
                            //wait for second to retry
                            System.Threading.Thread.Sleep(new TimeSpan(0,0,0,1));
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(ex.Message,false);
                        //wait for second to retry
                        System.Threading.Thread.Sleep(new TimeSpan(0, 0, 0, 1));
                    }

                    if (isExits)
                    {

                        aggDocs =
                            await GetSiteIdAggDocs(client, sensorReading);
                        await UpdateSiteIdAgg(client, aggDocs[0], sensorReading);

                    }
                }

            }
        }


        public void IngestDocsInBulk(SqlClientExtension client, IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken, Uri destinationCollectionUri)
        {
            throw new NotImplementedException();
        }
    }
}
