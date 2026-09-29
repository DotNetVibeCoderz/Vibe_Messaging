namespace BigPipe.Client.Tests;

[Collection("bigpipe")]
public class SchemaRegistryTests(BigPipeFixture fx)
{
    /// <summary>Needs a running registry (BIGPIPE_TEST_REGISTRY, default http://localhost:8081); skipped otherwise.</summary>
    [Fact]
    public async Task Serializer_registers_schema_and_roundtrips()
    {
        var url = Environment.GetEnvironmentVariable("BIGPIPE_TEST_REGISTRY") ?? "http://localhost:8081";
        using var probe = new HttpClient();
        try { await probe.GetAsync(url + "/subjects"); } catch { return; }

        var topic = BigPipeFixture.Unique("sr-orders");
        var ser = new SchemaRegistrySerializer<Order>(url);
        var bytes = ser.Serialize(new Order(7, 1500m, "IDR"), new SerializationContext(topic, false))!;
        Assert.Equal(0, bytes[0]);
        var des = new SchemaRegistryDeserializer<Order>(url);
        var back = des.Deserialize(bytes, new SerializationContext(topic, false));
        Assert.Equal(new Order(7, 1500m, "IDR"), back);
        Assert.True(des.LastSchemaId > 0);
        using var client = new SchemaRegistryClient(url);
        Assert.Contains($"{topic}-value", await client.SubjectsAsync());
        _ = fx;
    }
}
