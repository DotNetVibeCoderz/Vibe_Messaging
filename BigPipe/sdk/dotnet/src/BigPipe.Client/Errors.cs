namespace BigPipe.Client;

/// <summary>Kafka protocol error codes surfaced by BigPipe and Apache Kafka.</summary>
public enum ErrorCode : short
{
    UnknownServerError = -1,
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    LeaderNotAvailable = 5,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    MessageTooLarge = 10,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopic = 17,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidConfig = 40,
    InvalidRequest = 42,
    OutOfOrderSequenceNumber = 45,
    DuplicateSequenceNumber = 46,
    InvalidProducerEpoch = 47,
    UnknownProducerId = 59,
    NonEmptyGroup = 68,
    GroupIdNotFound = 69,
    MemberIdRequired = 79,
    InvalidRecord = 87,

    // Client-side conditions (not sent by brokers).
    ConnectionFailed = -100,
    Timeout = -101,
    HttpError = -102,
}

/// <summary>Error raised by BigPipe clients. <see cref="Code"/> carries the Kafka error code.</summary>
public class BigPipeException : Exception
{
    public BigPipeException(ErrorCode code, string message, Exception? inner = null) : base(message, inner)
    {
        Code = code;
    }

    public ErrorCode Code { get; }

    /// <summary>True for conditions that usually succeed when retried.</summary>
    public bool IsRetriable => Code is ErrorCode.LeaderNotAvailable or ErrorCode.NotLeaderOrFollower
        or ErrorCode.RequestTimedOut or ErrorCode.CoordinatorLoadInProgress or ErrorCode.CoordinatorNotAvailable
        or ErrorCode.NotCoordinator or ErrorCode.ConnectionFailed or ErrorCode.Timeout
        or ErrorCode.UnknownTopicOrPartition;

    internal static void ThrowIfError(short code, string context)
    {
        if (code != 0)
            throw new BigPipeException((ErrorCode)code, $"{context}: {(ErrorCode)code}");
    }
}
