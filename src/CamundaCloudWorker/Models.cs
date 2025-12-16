using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text;

namespace CamundaCloudWorker;

// DTO’er
public class ProcessDefinitionSearchRequest
{
    public object filter { get; set; } = new { };
    public int size { get; set; } = 50;
    public SortDefinition[] sort { get; set; }
}

public class SortDefinition
{
    public string field { get; set; }
    public string order { get; set; }
}

public class ProcessDefinitionDto
{
    public long key { get; set; }
    public string bpmnProcessId { get; set; }
    public int version { get; set; }
    public string name { get; set; }
    public string resourceName { get; set; }
    // andre felter som Operate returnerer ...
}

public class SearchResult<T>
{
    public T[] items { get; set; }
    public long? nextKey { get; set; }
    public long totalCount { get; set; }
}

