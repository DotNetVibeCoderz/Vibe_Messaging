namespace BigPipe.Gallery.Cases;

/// <summary>All Gallery cases in display order.</summary>
public static class CaseCatalog
{
    public static IReadOnlyList<GalleryCase> All { get; } =
    [
        new HelloCase(),
        new StorageModesCase(),
        new MigrationCase(),
        new GroupsCase(),
        new ShareGroupCase(),
        new FilterCase(),
        new FlowCase(),
        new StreamsWindowCase(),
        new SchemaCase(),
        new MLAnomalyCase(),
        new TorchCase(),
        new ScriptingCase(),
        new SentimentCase(),
        new ClusteringCase(),
    ];
}
