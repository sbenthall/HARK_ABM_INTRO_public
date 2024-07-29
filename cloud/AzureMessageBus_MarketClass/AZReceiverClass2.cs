using System.Threading.Tasks;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using System;

namespace AZRPCReceiver
{
    public class Program
    {
    //const string CONNECTION_STR = "Endpoint=sb://sharkfinmq.servicebus.windows.net/;SharedAccessKeyName=brokerDev;SharedAccessKey=ethbk8ZhqarR89v455j9psqsBPPa/CcjdtxZY7+3vVw=";
    static public string CONNECTION_STR = Get_RPC_ConnectionString("sharkfinkv");
    //const string SIMULATION_ID = "chum4000";
    public ServiceBusClient? client;
    public ServiceBusProcessor? processor;
    
    //int RequestCounter = 0;
        public static async Task Main(string[] args)
        {
            //string finalFlag = "";
            var options = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1              
            };
            string SIMULATION_ID = args[0];
            string clientQueueName = ($"{SIMULATION_ID}_requests");
            string responseQueueName = ($"{SIMULATION_ID}_responses");
            Console.WriteLine("The current time is " + DateTime.Now);
            Console.WriteLine("Configuring AMMPS RPC Listenter...");
            Console.WriteLine($"The current simulation_ID is: {SIMULATION_ID}");
            Console.WriteLine($"The Request queue is: {clientQueueName} and the response queue name is {responseQueueName}"); 
            Console.WriteLine($"Attempting to conect AMMPS to the Azure Service Bus Endpoint:{CONNECTION_STR}");
            await using ServiceBusClient? client = new ServiceBusClient(CONNECTION_STR);
            await using ServiceBusProcessor processor = client.CreateProcessor(clientQueueName, options);   
            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;  
            await processor.StartProcessingAsync();
            //listener will process until it receives the final message
            //await processor.StopProcessingAsync();
            Console.ReadLine();
        }

        public static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            string mesgID = args.Message.MessageId.ToString();
            string replyTo = args.Message.ReplyTo;
            var finalMessage = args.Message.ApplicationProperties["finalMessage"].ToString();
            string coorelationID = mesgID;
            Console.WriteLine($"Connected to Azure Service Bus....Listening for requests....received message ID: {mesgID}");
            Console.WriteLine($"Message body contents: {body}");
            Console.WriteLine($"Has simulation completed? {finalMessage}");
            // there is where we add logic to process request and send response.
            Console.WriteLine($"Processing response using coorolationID: {coorelationID}");
            Console.WriteLine($"Attempting to send response to the following replyTo address: {replyTo}");
            //need to add and 'if' statement to determin when Simulations is complete and exit the program
            await ReplyRPCMessage(body,replyTo, mesgID);
            //code to complete message.
            await args.CompleteMessageAsync(args.Message);
            //else exit program
            
            if (finalMessage == "yes") 
            {
                Console.WriteLine("Processed final Message. Closing Listener");
            }

        }

        public static Task ErrorHandler(ProcessErrorEventArgs args)   
        {
            Console.WriteLine("-----------------------------");
            // the fully qualified namespace is available
            Console.WriteLine(args.FullyQualifiedNamespace);
            // the error source tells me at what point in the processing an error occurred
            Console.WriteLine(args.ErrorSource);
            //entity path
            Console.WriteLine(args.EntityPath);
            //Console.WriteLine(args.Exception.ToString());
            Console.WriteLine("-----------------------------");
            Console.WriteLine("-----------------------------");
            //processor.StopProcessingAsync();
            return Task.CompletedTask;
        }
        public static async Task ReplyRPCMessage(string mesgBody, string responseQueueName, string mesgID)
        {
            await using ServiceBusClient? client = new ServiceBusClient(CONNECTION_STR);
            ServiceBusSender sender = client.CreateSender(responseQueueName);
            ServiceBusMessage message = new ServiceBusMessage(mesgBody);
            message.CorrelationId=mesgID;
            string resposeMessageID= message.MessageId;
            Console.WriteLine($"Connected to Azure Service Bus....Sending AMMPS response message with ID {resposeMessageID} with coorolationID {message.CorrelationId}");
            await sender.SendMessageAsync(message);
        }


        public static string Get_RPC_ConnectionString(string keyVaultName)
        {
            string keyVaultUri = ($"https://{keyVaultName}.vault.azure.net");
            Console.WriteLine($"Retrieving Service Bus Endpoint from Azure KeyVault: {keyVaultUri}");
            //var credential = new InteractiveBrowserCredential();
            DefaultAzureCredential? credential = new DefaultAzureCredential();
            var client = new SecretClient(new Uri(keyVaultUri), credential);
            var retreived_ConnectionString_Secret = client.GetSecret(name:"sharkfinMQconnectionstring");
            var sharkfinMQconnectionstringValue = retreived_ConnectionString_Secret.Value;
            string cstring = "";
            cstring = sharkfinMQconnectionstringValue.Value;
                return cstring;
        }

    }

}
