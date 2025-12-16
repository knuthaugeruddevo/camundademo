using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Zeebe.Client;
using Zeebe.Client.Api.Responses;
using Zeebe.Client.Api.Worker;
using Zeebe.Client.Impl.Builder;

namespace CamundaCloudWorker
{
    class MenuItem
    {
        public MenuItem() { }
        public int Index { get; set; }
        public string Name { get; set; }  
    }
    class Program
    {
        private static IZeebeClient _zeebeClient;

        // Constants for worker configuration
        private const int MaxJobsActive = 5;
        private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
        
        // Kendte processer (hardkodet indtil Operate API bruges)
        private static readonly string[] KnownProcesses =
        {
            "Process_SignalMessageDemo"
        };

        // Dictionary to map job types to their handlers
        private static readonly Dictionary<string, AsyncJobHandler> JobHandlers = new()
        {
            { "register.petition", RegisterPetition },
            { "register.lawyer", RegisterPetitionLawyer },
            { "register.rekvirent", RegisterPetitionRekvirent },
            { "register.skyldner", RegisterPetitionSkyldner },
            { "case.create.invalid.notification", CaseCreateInvalid },
            { "case.create.persist.orgunit", CaseCreatePersistOrgunit }
        };

        static async Task Main(string[] args)
        {
            _zeebeClient = ZeebeClient.Builder()
                .UseGatewayAddress("127.0.0.1:26500")
                .UsePlainText()
                .Build();

            using (var signal = new EventWaitHandle(false, EventResetMode.AutoReset))
            {
                // Dynamically create workers from the JobHandlers dictionary
                foreach (var entry in JobHandlers)
                {
                    var workerBuilder = _zeebeClient.NewWorker()
                        .JobType(entry.Key)
                        .Handler(entry.Value) 
                        .MaxJobsActive(MaxJobsActive)
                        .Name(Environment.MachineName)
                        .AutoCompletion()
                        .PollInterval(PollInterval)
                        .Timeout(Timeout);
                    
                    workerBuilder.Open();
                }

                Console.WriteLine("Workers attached. Starting interactive menu...\n");

                await RunMenuAsync();

                signal.WaitOne();
            }


            // Dispose client if possible on exit
            try
            {
                if (_zeebeClient is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            catch
            {
                // ignore disposal errors on shutdown
            }
        }

        private static async Task RegisterPetition(IJobClient client, IJob activatedJob)
        {

        }
        private static async Task RegisterPetitionLawyer(IJobClient client, IJob activatedJob)
        {

        }

        private static async Task SystemTask(IJobClient client, IJob activatedJob)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"[WORKER] Received job for '{activatedJob.Type}'.");
            Console.WriteLine($"  - Process Instance Key: {activatedJob.ProcessInstanceKey}");
            Console.WriteLine($"  - Job Key: {activatedJob.Key}");
            Console.WriteLine($"  - Variables: {activatedJob.Variables}");
            Console.ResetColor();
        }

        private static Task HandleMessage1(IJobClient client, IJob activatedJob)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"[WORKER] Received job for '{activatedJob.Type}'.");
            Console.WriteLine($"  - Process Instance Key: {activatedJob.ProcessInstanceKey}");
            Console.WriteLine($"  - Job Key: {activatedJob.Key}");
            Console.WriteLine($"  - Variables: {activatedJob.Variables}");
            Console.ResetColor();
            return Task.CompletedTask;
        }

        private static Task RegisterPetitionSkyldner(IJobClient client, IJob activatedJob)
        {
            // This handler is for jobs of type 'start_event'. You can add logging here as well if needed.
            return Task.CompletedTask;
        }

        private static Task CaseCreateInvalid(IJobClient client, IJob activatedJob)
        {
            // This handler is for jobs of type 'start_event'. You can add logging here as well if needed.
            return Task.CompletedTask;
        }
        private static Task CaseCreatePersistOrgunit(IJobClient client, IJob activatedJob)
        {
            // This handler is for jobs of type 'start_event'. You can add logging here as well if needed.
            return Task.CompletedTask;
        }
        private static async Task RegisterPetitionRekvirent(IJobClient client, IJob activatedJob)
        {
            
        }

        // External menu loop method (prints menu again after each selection)
        private static async Task RunMenuAsync()
        {
            bool exit = false;
            Console.ResetColor();

            while (!exit)
            {
                PrintMenu();

                var choice = Console.ReadLine();

                Console.ForegroundColor = ConsoleColor.Yellow;

                switch (choice)
                {
                    case "1":
                        await ListProcesses();
                        break;
                    case "2":
                        await ShowProcessNodes();
                        break;
                    case "3":
                        await StartProcess();
                        break;
                    case "4":
                        await SendSignalAsMessage(); // bemærk signals som messages
                        break;
                    case "5":
                        await SendMessage();
                        break;
                    case "0":
                        exit = true;
                        break;
                    default:
                        Console.WriteLine("Ukendt valg.");
                        break;
                }

                Console.ResetColor();

                // After each action (unless exiting) loop continues and PrintMenu() will run again.
                if (!exit)
                {
                    Console.WriteLine(); // small spacing between iterations
                }
            }
        }

        private static void PrintMenu()
        {
            Console.WriteLine("=== Camunda 8 .NET Prototype ===");
            Console.WriteLine("1) Vis liste over processer");
            Console.WriteLine("2) Vis noder i proces");
            Console.WriteLine("3) Start proces");
            Console.WriteLine("4) Send signal (RabbitSignal)");
            Console.WriteLine("5) Send message (RabbitMessage, correlationKey='id')");
            Console.WriteLine("0) Afslut");
            Console.Write("\nVælg et punkt: ");
        }


        private static Dictionary<string,ProcessDefinitionDto> _processes = new();
        private static async Task ListProcesses()
        {
            Console.WriteLine("=== Liste over processer via Operate Search API ===\n");

            using var http = new HttpClient();
            http.BaseAddress = new Uri("http://localhost:8088/");  // dit Operate base

            var req = new ProcessDefinitionSearchRequest
            {
                filter = null,   // tom filter → alle
                size = 50,
                sort = new[]
                {
                    new SortDefinition { field = "version", order = "DESC" }
                }
            };

            try
            {
                var response = await http.PostAsJsonAsync("v1/process-definitions/search", req);
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Fejl: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
                }
                else
                {
                    var res_txt = await response.Content.ReadAsStringAsync();
                     var result = await response.Content.ReadFromJsonAsync<SearchResult<ProcessDefinitionDto>>();
                    if (result?.items != null && result.items.Length > 0)
                    {
                        foreach (var p in result.items)
                        {
                            if( !_processes.ContainsKey(p.key.ToString()) )
                            {
                                _processes.Add(p.key.ToString(), p);
                            }

                            Console.WriteLine($"Name: {p.name} (v: {p.version})");
                            Console.WriteLine($"  KEY: {p.key}"); ;
                            Console.WriteLine($"  BPMN ID: {p.bpmnProcessId}");
                            Console.WriteLine();
                        }
                    }
                    else
                    {
                        Console.WriteLine("Ingen processer fundet via search.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception ved kald til Operate API: " + ex.Message);
            }

        }

        private static async Task ShowProcessNodes()
        {
            Console.Write("Indtast processDefinitionKey (numeric/internal key): ");
            var defKey = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(defKey))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Ugyldig input.");
                Console.ResetColor();
                return;
            }

            // Hent BPMN XML fra Operate
            using var http = new HttpClient();
            http.BaseAddress = new Uri("http://localhost:8088/");  // dit Operate-base

            try
            {
                if (!_processes.Values.Any(v => v.bpmnProcessId == defKey))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Ugyldig process name: '{defKey}'.");
                    Console.ResetColor();
                    return;
                }
                ProcessDefinitionDto proces = _processes.OrderByDescending(p => p.Value.version).FirstOrDefault(k => k.Value.bpmnProcessId == defKey).Value;
                var resp = await http.GetAsync($"v1/process-definitions/{proces.key}/xml");
                if (!resp.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Fejl ved hentning af XML: {resp.StatusCode}");
                    var body = await resp.Content.ReadAsStringAsync();
                    Console.WriteLine(body);
                }
                else
                {
                    var xmlString = await resp.Content.ReadAsStringAsync();
                    Console.WriteLine("BPMN XML modtaget. Parser noder ...\n");

                    // Parse XML
                    var xdoc = XDocument.Parse(xmlString);
                    XNamespace bpm = "http://www.omg.org/spec/BPMN/20100524/MODEL";

                    // Saml elementer, fx tasks, events, gateways osv.
                    var nodeElements = xdoc
                        .Descendants()
                        .Where(e =>
                            e.Name == bpm + "userTask" ||
                            e.Name == bpm + "serviceTask" ||
                            e.Name == bpm + "intermediateCatchEvent" ||
                            e.Name == bpm + "intermediateThrowEvent" ||
                            e.Name == bpm + "startEvent" ||
                            e.Name == bpm + "endEvent" ||
                            e.Name == bpm + "exclusiveGateway" ||
                            e.Name == bpm + "parallelGateway"
                        );

                    Console.WriteLine($"Fundne {nodeElements.Count()} node(s):\n");
                    foreach (var node in nodeElements)
                    {
                        string id = node.Attribute("id")?.Value ?? "(no id)";
                        string name = node.Attribute("name")?.Value ?? "";
                        Console.WriteLine($"- {node.Name.LocalName}  |  id = {id}  |  name = {name}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: " + ex.Message);
            }

        }

        private static ProcessDefinitionDto GetProcessFromName(string name)
        {
            return _processes.OrderByDescending(p => p.Value.version).FirstOrDefault(k => k.Value.name == name).Value;
        }

        private static async Task StartProcess()
        {
            Console.Write("Indtast BPMN process id: ");
            var bpmnid = Console.ReadLine();

            ProcessDefinitionDto proces = _processes.OrderByDescending(p => p.Value.version).FirstOrDefault(k => k.Value.bpmnProcessId == bpmnid).Value;

            if( proces != null) 
            {
                var instance = await _zeebeClient.NewCreateProcessInstanceCommand()
                .BpmnProcessId(proces.bpmnProcessId)
                .LatestVersion()
                .Send();

                Console.WriteLine($"Startet procesinstans med key: {instance.ProcessInstanceKey}");

            }

            Console.WriteLine();
            Console.WriteLine();
        }

        private static async Task SendSignal()
        {
            //Console.Clear();
            //Console.WriteLine("Sender broadcast signal: RabbitSignal ...");

            //await _zeebeClient.NewBroadcastSignalCommand()
            //    .SignalName("RabbitSignal")
            //    .Send();

            //Console.WriteLine("Signal sendt!");
            //Console.WriteLine("\nTryk en tast for at fortsætte...");
            //Console.ReadKey();
            await SendSignalAsMessage();
        }

        private static async Task SendSignalAsMessage()
        {
            Console.WriteLine("Sender RabbitSignal (via PublishMessageCommand) ...");

            await _zeebeClient.NewPublishMessageCommand()
                .MessageName("wait_msg")
                .CorrelationKey("message_wait") // alle ventende instanser kan bruge samme key
                .TimeToLive(TimeSpan.FromMinutes(5))
                .Send();

            Console.WriteLine("RabbitSignal sendt (som message).");
        }

        private static async Task SendMessage()
        {
            Console.Clear();
            Console.WriteLine("Sender message: RabbitMessage (correlationKey='id') ...");

            await _zeebeClient.NewPublishMessageCommand()
                .MessageName("RabbitMessage")
                .CorrelationKey("id") // Hardkodet correlation key
                .TimeToLive(TimeSpan.FromMinutes(5))
                .Send();

            Console.WriteLine("Message sendt!");
            Console.WriteLine("\nTryk en tast for at fortsætte...");
            Console.ReadKey();
        }

        private static void NotifyPersonToQuarantine(IJobClient jobClient, IJob job)
        {
            JObject jsonObject = JObject.Parse(job.Variables);
            string person_uuid = (string)jsonObject["person_uuid"];

            Console.WriteLine("Retrieving contact details for person " + person_uuid + " from external database...");
            Console.WriteLine("Sending notification to person " + person_uuid + " to quarantine...");
        }

        private static void GenerateCertificateOfRecovery(IJobClient jobClient, IJob job)
        {
            Guid uuid = Guid.NewGuid();
            string recovery_certificate_uuid = uuid.ToString();
            JObject jsonObject = JObject.Parse(job.Variables);
            string person_uuid = (string)jsonObject["person_uuid"];

            Console.WriteLine("Generating certificate of recovery for person " + person_uuid + "...");
            Console.WriteLine("Generated certificate ID: " + recovery_certificate_uuid);
            Console.WriteLine("Storing Recovery Certificate in external database...");

            jobClient.NewCompleteJobCommand(job.Key)
                    .Variables("{\"recovery_certificate_uuid\": \"" + recovery_certificate_uuid + "\"}")
                    .Send()
                    .GetAwaiter()
                    .GetResult();
        }

        private static void SendCertificateOfRecovery(IJobClient jobClient, IJob job)
        {
            JObject jsonObject = JObject.Parse(job.Variables);
            string person_uuid = (string)jsonObject["person_uuid"];
            string recovery_certificate_uuid = (string)jsonObject["recovery_certificate_uuid"];

            Console.WriteLine("Retrieving Recovery Certificate " + recovery_certificate_uuid + " from external database...");
            Console.WriteLine("Retrieving contact details for person " + person_uuid + "from external database...");
            Console.WriteLine("Sending Recovery Certificate to person " + person_uuid + ". Enjoy that ice-cream!");
        }
    }
}
