using BigPipe.Client.Admin;

namespace BigPipe.Console.Services;

/// <summary>English / Bahasa Indonesia strings for the console chrome and headings.</summary>
public sealed class L10n
{
    public string Lang { get; set; } = "en";

    public bool IsIndonesian => Lang == "id";

    public string T(string key) =>
        (IsIndonesian ? Id : En).TryGetValue(key, out var v) ? v : En.TryGetValue(key, out var e) ? e : key;

    private static readonly Dictionary<string, string> En = new()
    {
        ["nav.overview"] = "Overview",
        ["nav.topics"] = "Topics",
        ["nav.groups"] = "Groups",
        ["nav.flows"] = "Flows",
        ["nav.schemas"] = "Schemas",
        ["nav.about"] = "About",
        ["plate.cluster"] = "Cluster",
        ["plate.node"] = "Node",
        ["plate.version"] = "Version",
        ["plate.uptime"] = "Uptime",
        ["plate.objects"] = "Object storage",
        ["plate.kafka"] = "Kafka endpoint",
        ["overview.title"] = "Plant overview",
        ["overview.lede"] = "Every topic is a pipe. Its material shows where the data lives, and the flow speed follows what is being written right now.",
        ["overview.in"] = "Records in",
        ["overview.bytesIn"] = "Ingress",
        ["overview.bytesOut"] = "Egress",
        ["overview.p99"] = "Produce p99",
        ["overview.lag"] = "Consumer lag",
        ["overview.schematic"] = "Pipeline schematic",
        ["overview.schematicHint"] = "Producers on the left, consumer groups on the right",
        ["overview.noTopics"] = "No topics yet. Create one on the Topics page or produce to any name — topics are created on first use.",
        ["mode.local"] = "Local disk",
        ["mode.tiered"] = "Tiered to object storage",
        ["mode.diskless"] = "Diskless (object storage)",
        ["topics.title"] = "Topics",
        ["topics.lede"] = "Pick the storage per topic. Change it any time: offsets never move, new data flows into the new material.",
        ["topics.create"] = "Create topic",
        ["topics.name"] = "Name",
        ["topics.partitions"] = "Partitions",
        ["topics.mode"] = "Storage",
        ["topics.records"] = "Records",
        ["topics.stored"] = "Stored",
        ["topics.rate"] = "Rate",
        ["topics.retention"] = "Retention (hours, -1 = forever)",
        ["topics.showInternal"] = "Show internal topics",
        ["topic.partitions"] = "Partitions",
        ["topic.messages"] = "Messages",
        ["topic.live"] = "Live tail",
        ["topic.produce"] = "Produce",
        ["topic.config"] = "Configuration",
        ["topic.migrate"] = "Move new data to",
        ["topic.migrateHint"] = "Migration is online and keeps every offset. Existing data stays where it is.",
        ["topic.filter"] = "Filter (bpql)",
        ["topic.filterHint"] = "e.g. this.amount > 1000 && header(\"region\") == \"ID\"",
        ["topic.search"] = "Search",
        ["topic.delete"] = "Delete topic",
        ["topic.send"] = "Send",
        ["topic.key"] = "Key",
        ["topic.value"] = "Value",
        ["topic.headers"] = "Headers (k=v per line)",
        ["topic.startTail"] = "Start tail",
        ["topic.stopTail"] = "Stop",
        ["groups.title"] = "Consumer groups",
        ["groups.lede"] = "Kafka-protocol groups, HTTP groups and flow cursors in one place. Lag is how far each group is behind the end of the log.",
        ["groups.members"] = "Members",
        ["groups.state"] = "State",
        ["groups.lag"] = "Lag",
        ["groups.reset"] = "Reset offsets",
        ["flows.title"] = "Flows",
        ["flows.lede"] = "Managed pipelines inside the broker: filter, map and route records from one topic to another without running extra services.",
        ["flows.deploy"] = "Deploy flow",
        ["flows.pause"] = "Pause",
        ["flows.resume"] = "Resume",
        ["flows.delete"] = "Delete",
        ["schemas.title"] = "Schemas",
        ["schemas.lede"] = "Subjects in the BigPipe Schema Registry (Avro, JSON Schema, Protobuf) with compatibility checks on every new version.",
        ["schemas.unreachable"] = "The Schema Registry is not reachable at",
        ["about.title"] = "About BigPipe",
        ["common.offline"] = "The BigPipe admin API is not reachable. Start bigpiped (bigpiped --mode dev) or check ConsoleOptions:AdminUrl.",
        ["common.refresh"] = "Refresh",
        ["common.cancel"] = "Cancel",
        ["common.save"] = "Save",
        ["common.none"] = "Nothing here yet.",
        ["sch.producers"] = "Producers",
        ["sch.noConsumers"] = "no consumers",
        ["sch.more"] = "more on the Topics page",
    };

    private static readonly Dictionary<string, string> Id = new()
    {
        ["nav.overview"] = "Ikhtisar",
        ["nav.topics"] = "Topic",
        ["nav.groups"] = "Grup",
        ["nav.flows"] = "Flow",
        ["nav.schemas"] = "Skema",
        ["nav.about"] = "Tentang",
        ["plate.cluster"] = "Klaster",
        ["plate.node"] = "Node",
        ["plate.version"] = "Versi",
        ["plate.uptime"] = "Waktu aktif",
        ["plate.objects"] = "Object storage",
        ["plate.kafka"] = "Endpoint Kafka",
        ["overview.title"] = "Ikhtisar instalasi",
        ["overview.lede"] = "Setiap topic adalah pipa. Materialnya menunjukkan tempat data disimpan, dan kecepatan aliran mengikuti data yang sedang ditulis.",
        ["overview.in"] = "Record masuk",
        ["overview.bytesIn"] = "Data masuk",
        ["overview.bytesOut"] = "Data keluar",
        ["overview.p99"] = "Produce p99",
        ["overview.lag"] = "Lag consumer",
        ["overview.schematic"] = "Skema pipa",
        ["overview.schematicHint"] = "Producer di kiri, consumer group di kanan",
        ["overview.noTopics"] = "Belum ada topic. Buat di halaman Topic atau langsung produce ke nama apa pun — topic dibuat saat pertama dipakai.",
        ["mode.local"] = "Disk lokal",
        ["mode.tiered"] = "Tiered ke object storage",
        ["mode.diskless"] = "Diskless (object storage)",
        ["topics.title"] = "Topic",
        ["topics.lede"] = "Pilih penyimpanan per topic. Bisa diubah kapan saja: offset tidak berubah, data baru mengalir ke material baru.",
        ["topics.create"] = "Buat topic",
        ["topics.name"] = "Nama",
        ["topics.partitions"] = "Partisi",
        ["topics.mode"] = "Penyimpanan",
        ["topics.records"] = "Record",
        ["topics.stored"] = "Tersimpan",
        ["topics.rate"] = "Laju",
        ["topics.retention"] = "Retensi (jam, -1 = selamanya)",
        ["topics.showInternal"] = "Tampilkan topic internal",
        ["topic.partitions"] = "Partisi",
        ["topic.messages"] = "Pesan",
        ["topic.live"] = "Pantau langsung",
        ["topic.produce"] = "Kirim",
        ["topic.config"] = "Konfigurasi",
        ["topic.migrate"] = "Pindahkan data baru ke",
        ["topic.migrateHint"] = "Migrasi berjalan online dan semua offset tetap. Data lama tetap di tempatnya.",
        ["topic.filter"] = "Filter (bpql)",
        ["topic.filterHint"] = "mis. this.amount > 1000 && header(\"region\") == \"ID\"",
        ["topic.search"] = "Cari",
        ["topic.delete"] = "Hapus topic",
        ["topic.send"] = "Kirim",
        ["topic.key"] = "Key",
        ["topic.value"] = "Value",
        ["topic.headers"] = "Header (k=v per baris)",
        ["topic.startTail"] = "Mulai pantau",
        ["topic.stopTail"] = "Berhenti",
        ["groups.title"] = "Consumer group",
        ["groups.lede"] = "Grup protokol Kafka, grup HTTP, dan kursor flow di satu tempat. Lag adalah seberapa jauh grup tertinggal dari ujung log.",
        ["groups.members"] = "Anggota",
        ["groups.state"] = "Status",
        ["groups.lag"] = "Lag",
        ["groups.reset"] = "Reset offset",
        ["flows.title"] = "Flow",
        ["flows.lede"] = "Pipeline terkelola di dalam broker: filter, mapping, dan routing record antar topic tanpa layanan tambahan.",
        ["flows.deploy"] = "Deploy flow",
        ["flows.pause"] = "Jeda",
        ["flows.resume"] = "Lanjutkan",
        ["flows.delete"] = "Hapus",
        ["schemas.title"] = "Skema",
        ["schemas.lede"] = "Subject di BigPipe Schema Registry (Avro, JSON Schema, Protobuf) dengan pemeriksaan kompatibilitas di setiap versi baru.",
        ["schemas.unreachable"] = "Schema Registry tidak dapat dihubungi di",
        ["about.title"] = "Tentang BigPipe",
        ["common.offline"] = "Admin API BigPipe tidak dapat dihubungi. Jalankan bigpiped (bigpiped --mode dev) atau periksa ConsoleOptions:AdminUrl.",
        ["common.refresh"] = "Muat ulang",
        ["common.cancel"] = "Batal",
        ["common.save"] = "Simpan",
        ["common.none"] = "Belum ada data.",
        ["sch.producers"] = "Producer",
        ["sch.noConsumers"] = "tanpa consumer",
        ["sch.more"] = "lainnya di halaman Topic",
    };
}

public static class Format
{
    public static string Bytes(double n)
    {
        string[] units = ["B", "KiB", "MiB", "GiB", "TiB"];
        var i = 0;
        while (n >= 1024 && i < units.Length - 1)
        {
            n /= 1024;
            i++;
        }
        return i == 0 ? $"{n:0} {units[i]}" : $"{n:0.#} {units[i]}";
    }

    public static string Count(double n) => n switch
    {
        >= 1_000_000_000 => $"{n / 1_000_000_000:0.##}B",
        >= 1_000_000 => $"{n / 1_000_000:0.##}M",
        >= 10_000 => $"{n / 1_000:0.#}K",
        _ => $"{n:#,0}",
    };

    public static string Duration(long seconds)
    {
        var t = TimeSpan.FromSeconds(seconds);
        return t.TotalDays >= 1 ? $"{(int)t.TotalDays}d {t.Hours}h" : t.TotalHours >= 1 ? $"{(int)t.TotalHours}h {t.Minutes}m" : $"{t.Minutes}m {t.Seconds}s";
    }

    public static string ModeClass(StorageMode m) => m switch
    {
        StorageMode.Tiered => "mat-tiered",
        StorageMode.Diskless => "mat-diskless",
        _ => "mat-local",
    };
}
