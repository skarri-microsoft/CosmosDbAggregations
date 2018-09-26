using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostProcessingPipeLineLib
{
    using Common;
    using PostProcessingWorkerRole;
    using SqlDataMovementLib.Source;

    public class PostProcessingController
    {
        public async Task Start()
        {
            CosmosDbProcessor cosmosDbProcessor=new CosmosDbProcessor();
            ChangeFeed changeFeed=new ChangeFeed(cosmosDbProcessor);
            await changeFeed.StartAsync();
        }
    }
}
