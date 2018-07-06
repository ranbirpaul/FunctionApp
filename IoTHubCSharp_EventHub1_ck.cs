using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace FunctionAppRanbir
{
    public static class IoTHubCSharp_EventHub1_ck
    {
        [FunctionName("IoTHubCSharp_EventHub1_ck")]
        public static void Run(string myIoTHubMessage, TraceWriter log)
        {
            log.Info($"1First Function App Method Start");
            log.Info($"C# IoT Hub trigger function processed a message: {myIoTHubMessage}");
            // myIoTHubMessage ="New message arrived";

            string json1 = "{\"ObjectId\":\"0d15dd33-3b76-445b-8300-82a1fc585dd1\",\"GlobalId\":\"4d9f7d84-58d7-4eb8-8f5d-05785e428ca34\",\"Value\":\"3.91597819\",\"VariableId\":\"{0d15dd33-3b76-445b-8300-82a1fc585dd1}{}:pf\",\"StatusCode\":\"1083114496\",\"NamespaceId\":\"f6888d0f-b18d-4c87-8f58-94c95d77ba34\",\"SourceTimeStamp\":\"06/04/2018 06:29:02\" }";

            string str1 = string.Format("INSERT INTO [dbo].[TelemetryDataNew] SELECT * FROM OPENJSON('");
            //string str2 = string.Format(json1);return;
            string str3 = string.Format("') WITH ( GlobalId varchar(max) N'$.GlobalId', VariableId varchar(max) N'$.VariableId',Value decimal(8,0) N'$.Value', StatusCode varchar(max) N'$.StatusCode', SourceTimeStamp datetime2 N'$.SourceTimeStamp')");
            //string querystr = "INSERT INTO [dbo].[TelemetryDataNew] SELECT * FROM OPENJSON('{"ObjectId":"0d15dd33-3b76-445b-8300-82a1fc585dd1","GlobalId":"4d9f7d84-58d7-4eb8-8f5d-05785e428ca34","Value":"3.91597819","VariableId":"{0d15dd33-3b76-445b-8300-82a1fc585dd1}{}:pf","StatusCode":"1083114496","NamespaceId":"f6888d0f-b18d-4c87-8f58-94c95d77ba34","SourceTimeStamp":"06/04/2018 06:29:02" }') WITH ( GlobalId varchar(max) N'$.GlobalId', VariableId varchar(max) N'$.VariableId',Value decimal(8,0) N'$.Value', StatusCode varchar(max) N'$.StatusCode', SourceTimeStamp datetime2 N'$.SourceTimeStamp')";
            string querystr = str1 + json1 + str3;
            log.Info(querystr);

            if (String.IsNullOrWhiteSpace(myIoTHubMessage))
                return;
            try
            {
                string ConnString = ConfigurationManager.ConnectionStrings["azure-db-connection"].ConnectionString;
                //var cmd = new Command(ConnString);
                var sqlCmd = new SqlCommand(querystr);

                //   var sqlCmd = new SqlCommand("INSERT INTO dbo.TestIoT(Name) VALUES('"+ myIoTHubMessage +"');");
                //  var sqlCmd = new SqlCommand("Insert_TempTimeSeriesData");
                //    sqlCmd.CommandType = System.Data.CommandType.StoredProcedure;
                //   sqlCmd.Parameters.AddWithValue("TempVariable", myIoTHubMessage);
                sqlCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                log.Error($"C# Event Hub trigger function exception: {ex.Message}");
            }
            log.Info($"First Function App Method End");
        }


    }
}
